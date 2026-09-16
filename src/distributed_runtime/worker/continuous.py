"""Continuous supervisor: run owned partitions with real lease fencing.

One supervisor per worker instance. Each tick renews owned leases, starts
tasks for newly assigned partitions, and cancels contexts when ownership
is lost. Emissions go through fenced sink writes into the outbox — a
stale owner's writes are rejected by the fencing check inside emit().
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from distributed_runtime.continuous import (
    EventSink,
    LeaseHandle,
    Partition,
    PartitionContext,
    SinkGuarantee,
)
from distributed_runtime.core.enums import PartitionStatus, WorkloadMode
from distributed_runtime.core.envelope import VersionedEnvelope
from distributed_runtime.core.identifiers import (
    DeploymentId,
    PartitionId,
    RuntimeInstanceId,
)
from distributed_runtime.core.lifecycle import CancellationSource
from distributed_runtime.registry import ContinuousRegistration, RuntimeRegistry
from distributed_runtime.state.continuous import ContinuousStateStore
from distributed_runtime.state.engine import StateEngine


class _StoreSink:
    """EventSink that writes fenced emissions to the outbox."""

    guarantee = SinkGuarantee.RUNTIME_FENCED

    def __init__(self, store: ContinuousStateStore, name: str) -> None:
        self._store = store
        self._name = name

    async def emit(
        self,
        event: VersionedEnvelope,
        *,
        stable_id: str,
        lease: LeaseHandle,
    ) -> None:
        await self._store.emit(lease, destination=self._name, stable_id=stable_id, envelope=event)


class _StoreSinks:
    """SinkRegistry resolving sink names to fenced outbox writers."""

    def __init__(self, store: ContinuousStateStore, registry: RuntimeRegistry) -> None:
        self._store = store
        self._registry = registry

    def get(self, name: str) -> EventSink:
        self._registry.sink(name)  # validates the sink is registered
        return _StoreSink(self._store, name)


@dataclass(slots=True)
class _RunningPartition:
    task: asyncio.Task[None]
    lease: LeaseHandle
    cancellation: CancellationSource


class ContinuousSupervisor:
    """Drive this instance's assigned partitions for one deployment."""

    def __init__(
        self,
        *,
        registry: RuntimeRegistry,
        engine: StateEngine,
        instance_id: RuntimeInstanceId,
        lease_seconds: int = 60,
    ) -> None:
        self._registry = registry
        self._store = ContinuousStateStore(engine)
        self._instance_id = instance_id
        self._lease_seconds = lease_seconds
        self._running: dict[PartitionId, _RunningPartition] = {}

    async def tick(self, deployment_id: DeploymentId) -> int:
        """One supervisor pass: start new partitions, renew leases, reap.

        Returns the number of partitions currently running.
        """
        deployment = await self._store.get_deployment(deployment_id)
        if deployment is None:
            raise LookupError(f"unknown deployment: {deployment_id}")
        registration = self._registry.workload(
            str(deployment.workload_id),
            deployment.workload_version,
            WorkloadMode.CONTINUOUS,
        )
        if not isinstance(registration, ContinuousRegistration):
            raise TypeError("deployment workload is not continuous")

        partitions = await self._store.partitions(deployment_id)
        mine = {
            p.partition_id: p
            for p in partitions
            if p.owner_id == self._instance_id and p.fencing_token > 0
        }

        # reap: partition gone, ownership moved, or task finished
        for partition_id, running in list(self._running.items()):
            current = mine.get(partition_id)
            token_moved = (
                current is not None and current.fencing_token != running.lease.fencing_token
            )
            if current is None or token_moved or running.task.done():
                if not running.task.done():
                    running.cancellation.cancel("ownership lost")
                    running.task.cancel()
                del self._running[partition_id]

        # start newly assigned partitions
        for partition_id, stored in mine.items():
            if partition_id in self._running:
                continue
            lease = LeaseHandle(
                deployment_id=deployment_id,
                partition_id=partition_id,
                owner_id=self._instance_id,
                fencing_token=stored.fencing_token,
            )
            cancellation = CancellationSource()
            partition = Partition(
                partition_id=partition_id,
                payload=stored.payload,
                weight=stored.weight,
            )
            context = PartitionContext(
                workload_id=deployment.workload_id,
                partition=partition,
                lease=lease,
                sinks=_StoreSinks(self._store, self._registry),
                cancellation=cancellation.token,
            )
            task = asyncio.create_task(registration.workload.run_partition(context, partition))
            self._running[partition_id] = _RunningPartition(
                task=task, lease=lease, cancellation=cancellation
            )
            await self._store.set_partition_status(lease, PartitionStatus.RUNNING)

        # renew live leases
        for running in self._running.values():
            ok = await self._store.renew_lease(running.lease, lease_seconds=self._lease_seconds)
            if not ok:
                running.cancellation.cancel("lease renewal rejected")

        return len(self._running)

    async def stop_all(self) -> None:
        """Cancel every running partition and wait for tasks."""
        for running in self._running.values():
            running.cancellation.cancel("supervisor stopping")
        tasks = [r.task for r in self._running.values()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._running.clear()
