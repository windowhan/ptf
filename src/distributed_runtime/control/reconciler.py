"""Continuous reconciler: desired vs actual partition assignment.

One reconcile pass = discover → sync partition rows → reclaim expired
leases → mark stale workers dead → assign unassigned partitions to the
least-loaded live worker. Idempotent; safe to run on every control tick.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from distributed_runtime.continuous import Partition
from distributed_runtime.core.enums import DeploymentStatus, PartitionStatus
from distributed_runtime.core.identifiers import DeploymentId, RuntimeInstanceId
from distributed_runtime.state.continuous import ContinuousStateStore, StoredPartition
from distributed_runtime.state.engine import StateEngine
from distributed_runtime.state.workers import WorkerInstance, WorkerRegistry


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    assigned: int
    reclaimed: int
    deactivated: int
    dead_workers: int


class Reconciler:
    """Keep continuous partition ownership converged with discovery."""

    def __init__(
        self,
        engine: StateEngine,
        *,
        lease_seconds: int = 60,
        stale_worker_seconds: int = 120,
    ) -> None:
        self._store = ContinuousStateStore(engine)
        self._workers = WorkerRegistry(engine)
        self._lease_seconds = lease_seconds
        self._stale_worker_seconds = stale_worker_seconds

    async def reconcile(
        self,
        deployment_id: DeploymentId,
        partitions_desired: Sequence[Partition],
    ) -> ReconcileResult:
        """One convergence pass for a deployment.

        ``partitions_desired`` is the result of ``discover_partitions`` —
        the reconciler never calls product code itself.
        """
        deployment = await self._store.get_deployment(deployment_id)
        if deployment is None:
            raise LookupError(f"unknown deployment: {deployment_id}")

        dead = await self._workers.mark_stale_dead(stale_after_seconds=self._stale_worker_seconds)

        stored = await self._store.sync_partitions(deployment_id, partitions_desired)
        deactivated = sum(1 for p in stored if p.status is PartitionStatus.INACTIVE)

        active_workers = await self._workers.list_active()
        live_ids = {w.instance_id for w in active_workers}
        now = datetime.now(UTC)
        assigned = 0
        reclaimed = 0
        if deployment.status is DeploymentStatus.ACTIVE and active_workers:
            load = self._load_map(stored, active_workers)
            for partition in stored:
                if partition.status is PartitionStatus.INACTIVE:
                    continue
                lease_expired = (
                    partition.lease_expires_at is not None and partition.lease_expires_at < now
                )
                owner_dead = partition.owner_id is not None and partition.owner_id not in live_ids
                needs_assign = (
                    partition.status is PartitionStatus.UNASSIGNED or lease_expired or owner_dead
                )
                if not needs_assign:
                    continue
                owner = min(load, key=lambda i: (load[i], str(i)))
                lease = await self._store.acquire_lease(
                    deployment_id,
                    partition.partition_id,
                    owner,
                    lease_seconds=self._lease_seconds,
                )
                if lease is not None:
                    # reflect the assignment so the next partition sees it —
                    # the stored snapshot is stale within this loop
                    load[owner] += partition.weight
                    if partition.owner_id is not None:
                        reclaimed += 1
                    else:
                        assigned += 1
        return ReconcileResult(
            assigned=assigned,
            reclaimed=reclaimed,
            deactivated=deactivated,
            dead_workers=dead,
        )

    def _load_map(
        self,
        partitions: tuple[StoredPartition, ...],
        workers: tuple[WorkerInstance, ...],
    ) -> dict[RuntimeInstanceId, int]:
        load = {w.instance_id: 0 for w in workers}
        for partition in partitions:
            if partition.owner_id in load and partition.status in (
                PartitionStatus.ASSIGNED,
                PartitionStatus.RUNNING,
            ):
                load[partition.owner_id] += partition.weight
        return load
