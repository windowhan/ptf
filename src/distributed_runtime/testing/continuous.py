"""Deterministic in-memory test kit for continuous workloads."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field

from distributed_runtime.continuous import (
    ContinuousWorkload,
    EventSink,
    LeaseHandle,
    Partition,
    PartitionContext,
    SinkGuarantee,
    discover_partitions,
)
from distributed_runtime.core.config import ContinuousPolicy
from distributed_runtime.core.enums import DeploymentStatus, PartitionStatus, WorkloadMode
from distributed_runtime.core.envelope import VersionedEnvelope
from distributed_runtime.core.errors import InvariantViolationError
from distributed_runtime.core.identifiers import (
    DeploymentId,
    PartitionId,
    RuntimeInstanceId,
    WorkloadId,
)
from distributed_runtime.core.lifecycle import CancellationSource, FakeClock
from distributed_runtime.registry import ContinuousRegistration, RegistrationError, RuntimeRegistry

_ACTIVE = (PartitionStatus.ASSIGNED, PartitionStatus.RUNNING)


@dataclass(frozen=True, slots=True)
class EmissionRecord:
    """One event emitted through a fenced sink in the local kit."""

    deployment_id: DeploymentId
    partition_id: PartitionId
    stable_id: str
    event: VersionedEnvelope
    fencing_token: int


@dataclass(frozen=True, slots=True)
class PartitionRecord:
    """Observed assignment state of one continuous partition."""

    partition_id: PartitionId
    status: PartitionStatus
    owner_id: RuntimeInstanceId | None
    fencing_token: int


@dataclass(slots=True)
class _LeaseState:
    owner_id: RuntimeInstanceId
    fencing_token: int
    expires_at: float


class FakeLeaseManager:
    """Issue, renew, and revoke partition leases with fencing tokens.

    Expiry is measured on the kit's :class:`FakeClock`; nothing expires while
    the clock stands still.
    """

    def __init__(
        self,
        *,
        clock: FakeClock | None = None,
        policy: ContinuousPolicy | None = None,
    ) -> None:
        self.clock = FakeClock() if clock is None else clock
        self._policy = ContinuousPolicy() if policy is None else policy
        self._token = 0
        self._leases: dict[tuple[DeploymentId, PartitionId], _LeaseState] = {}

    def acquire(
        self,
        *,
        deployment_id: DeploymentId,
        partition_id: PartitionId,
        owner_id: RuntimeInstanceId,
    ) -> LeaseHandle:
        """Grant a fresh lease with a strictly increasing fencing token."""
        key = (deployment_id, partition_id)
        current = self._leases.get(key)
        if current is not None and not self._expired(current):
            raise InvariantViolationError(
                "partition already leased",
                details={
                    "partition_id": str(partition_id),
                    "owner_id": str(current.owner_id),
                },
            )
        self._token += 1
        self._leases[key] = _LeaseState(
            owner_id=owner_id,
            fencing_token=self._token,
            expires_at=self.clock.monotonic() + self._policy.lease_seconds,
        )
        return LeaseHandle(
            deployment_id=deployment_id,
            partition_id=partition_id,
            owner_id=owner_id,
            fencing_token=self._token,
        )

    def renew(self, lease: LeaseHandle) -> bool:
        """Extend an unexpired lease held by the authorizing owner."""
        state = self._state(lease)
        if state is None or self._expired(state):
            return False
        state.expires_at = self.clock.monotonic() + self._policy.lease_seconds
        return True

    def release(self, lease: LeaseHandle) -> bool:
        """Release a lease held by the authorizing owner."""
        state = self._state(lease)
        if state is None:
            return False
        del self._leases[(lease.deployment_id, lease.partition_id)]
        return True

    def force_expire(self, deployment_id: DeploymentId, partition_id: PartitionId) -> None:
        """Simulate heartbeat loss: the lease is immediately expired."""
        state = self._leases.get((deployment_id, partition_id))
        if state is not None:
            state.expires_at = self.clock.monotonic()

    def is_current(self, lease: LeaseHandle) -> bool:
        """Return whether the lease names the live owner, token, and is unexpired."""
        state = self._state(lease)
        return state is not None and not self._expired(state)

    def owner_of(
        self,
        deployment_id: DeploymentId,
        partition_id: PartitionId,
    ) -> RuntimeInstanceId | None:
        """Return the current unexpired owner, if any."""
        state = self._leases.get((deployment_id, partition_id))
        if state is None or self._expired(state):
            return None
        return state.owner_id

    def _state(self, lease: LeaseHandle) -> _LeaseState | None:
        state = self._leases.get((lease.deployment_id, lease.partition_id))
        if state is None:
            return None
        authorized = lease.authorizes(
            deployment_id=lease.deployment_id,
            partition_id=lease.partition_id,
            owner_id=state.owner_id,
            fencing_token=state.fencing_token,
        )
        return state if authorized else None

    def _expired(self, state: _LeaseState) -> bool:
        return self.clock.monotonic() >= state.expires_at


class RecordingSink:
    """EventSink that records emissions and rejects stale fencing tokens."""

    guarantee = SinkGuarantee.RUNTIME_FENCED

    def __init__(
        self,
        *,
        lease_manager: FakeLeaseManager,
        delegate: EventSink | None = None,
        records: list[EmissionRecord] | None = None,
    ) -> None:
        self._leases = lease_manager
        self._delegate = delegate
        self.records: list[EmissionRecord] = [] if records is None else records
        self._seen: set[tuple[DeploymentId, PartitionId, str]] = set()

    async def emit(
        self,
        event: VersionedEnvelope,
        *,
        stable_id: str,
        lease: LeaseHandle,
    ) -> None:
        """Record one event, deduplicated by stable_id under current fencing."""
        if not self._leases.is_current(lease):
            raise InvariantViolationError(
                "emission rejected: stale or expired fencing token",
                details={
                    "partition_id": str(lease.partition_id),
                    "owner_id": str(lease.owner_id),
                    "fencing_token": lease.fencing_token,
                },
            )
        key = (lease.deployment_id, lease.partition_id, stable_id)
        if key in self._seen:
            return
        self._seen.add(key)
        if self._delegate is not None:
            await self._delegate.emit(event, stable_id=stable_id, lease=lease)
        self.records.append(
            EmissionRecord(
                deployment_id=lease.deployment_id,
                partition_id=lease.partition_id,
                stable_id=stable_id,
                event=event,
                fencing_token=lease.fencing_token,
            )
        )


@dataclass(slots=True)
class _PartitionState:
    partition: Partition
    status: PartitionStatus = PartitionStatus.UNASSIGNED
    owner_id: RuntimeInstanceId | None = None
    lease: LeaseHandle | None = None
    cancellation: CancellationSource | None = None
    task: asyncio.Task[None] | None = None


@dataclass(slots=True)
class _DeploymentState:
    deployment_id: DeploymentId
    registration: ContinuousRegistration
    cancellation: CancellationSource
    status: DeploymentStatus = DeploymentStatus.ACTIVE
    partitions: dict[PartitionId, _PartitionState] = field(default_factory=dict)


class _KitSinks:
    """SinkRegistry that wraps every lookup in fencing checks and recording."""

    def __init__(
        self,
        *,
        registry: RuntimeRegistry,
        lease_manager: FakeLeaseManager,
        records: list[EmissionRecord],
        sinks: Mapping[str, EventSink],
    ) -> None:
        self._registry = registry
        self._leases = lease_manager
        self._records = records
        self._sinks = dict(sinks)
        self._cache: dict[str, RecordingSink] = {}

    def get(self, name: str) -> EventSink:
        try:
            return self._cache[name]
        except KeyError:
            pass
        delegate = self._sinks.get(name)
        if delegate is None:
            try:
                delegate = self._registry.sink(name)
            except RegistrationError as error:
                raise RegistrationError(f"unknown sink: {name}") from error
        if isinstance(delegate, RecordingSink):
            sink = delegate
        else:
            sink = RecordingSink(
                lease_manager=self._leases,
                delegate=delegate,
                records=self._records,
            )
        self._cache[name] = sink
        return sink


class ContinuousRuntimeTestKit:
    """Drive registered continuous workloads through explicit deterministic steps.

    The kit never advances time or reconciles on its own. Tests call
    :meth:`reconcile`, :meth:`run_for`, and :meth:`expire_lease` explicitly.
    See ``docs/design/local-testing-kits.md`` for the contract.
    """

    def __init__(
        self,
        *,
        registry: RuntimeRegistry,
        clock: FakeClock | None = None,
        policy: ContinuousPolicy | None = None,
        sinks: Mapping[str, EventSink] | None = None,
    ) -> None:
        self._registry = registry
        self.clock = FakeClock() if clock is None else clock
        self._policy = ContinuousPolicy() if policy is None else policy
        self.leases = FakeLeaseManager(clock=self.clock, policy=self._policy)
        self._emissions: list[EmissionRecord] = []
        self._sinks = _KitSinks(
            registry=registry,
            lease_manager=self.leases,
            records=self._emissions,
            sinks={} if sinks is None else sinks,
        )
        self._instances = [RuntimeInstanceId("instance:local-0")]
        self._deployments: dict[DeploymentId, _DeploymentState] = {}
        self._deployment_counter = 0

    def deploy(self, *, workload: str, version: str) -> DeploymentId:
        """Create a local deployment for a registered continuous workload."""
        registration = self._registry.workload(workload, version, WorkloadMode.CONTINUOUS)
        if not isinstance(registration, ContinuousRegistration):
            raise LookupError(f"workload is not continuous: {workload}@{version}")
        self._deployment_counter += 1
        deployment_id = DeploymentId(f"deployment:local-{self._deployment_counter}")
        self._deployments[deployment_id] = _DeploymentState(
            deployment_id=deployment_id,
            registration=registration,
            cancellation=CancellationSource(clock=self.clock),
        )
        return deployment_id

    def add_instance(self, instance_id: str | None = None) -> RuntimeInstanceId:
        """Register another fake worker instance for assignment."""
        instance = RuntimeInstanceId(instance_id or f"instance:local-{len(self._instances)}")
        self._instances.append(instance)
        return instance

    async def reconcile(self, deployment_id: DeploymentId) -> tuple[PartitionRecord, ...]:
        """Discover partitions, reclaim expired leases, and assign owners."""
        state = self._state(deployment_id)
        workload = state.registration.workload
        desired = {p.partition_id: p for p in await discover_partitions(workload)}
        for partition_id, existing in state.partitions.items():
            if partition_id not in desired and existing.status in _ACTIVE:
                self._release_partition(existing, PartitionStatus.INACTIVE)
        for discovered in sorted(desired.values(), key=lambda p: str(p.partition_id)):
            current = state.partitions.get(discovered.partition_id)
            if current is None:
                current = _PartitionState(partition=discovered)
                state.partitions[discovered.partition_id] = current
            else:
                current.partition = discovered
            if current.status is PartitionStatus.INACTIVE:
                current.status = PartitionStatus.UNASSIGNED
            lease_valid = current.lease is not None and self.leases.is_current(current.lease)
            if current.status in _ACTIVE and lease_valid:
                continue
            if state.status is DeploymentStatus.ACTIVE:
                self._assign(state, current)
        return self.partitions(deployment_id)

    async def run_for(self, seconds: float, *, tick_seconds: float = 1.0) -> None:
        """Advance the fake clock and step handler tasks cooperatively."""
        if seconds < 0 or tick_seconds <= 0:
            raise ValueError("seconds must be non-negative and tick_seconds positive")
        remaining = float(seconds)
        while remaining > 0:
            self.clock.advance(min(tick_seconds, remaining))
            for state in self._deployments.values():
                self._tick_deployment(state)
            await asyncio.sleep(0)
            remaining -= tick_seconds
        await asyncio.sleep(0)

    def expire_lease(
        self,
        partition_id: str,
        deployment_id: DeploymentId | None = None,
    ) -> None:
        """Simulate heartbeat loss for one partition across deployments."""
        target = PartitionId(partition_id)
        matched = False
        for state in self._select_deployments(deployment_id):
            partition = state.partitions.get(target)
            if partition is None:
                continue
            matched = True
            self.leases.force_expire(state.deployment_id, target)
            if partition.cancellation is not None:
                partition.cancellation.cancel("lease expired")
        if not matched:
            raise LookupError(f"unknown partition: {partition_id}")

    def drain(self, deployment_id: DeploymentId) -> None:
        """Cancel every partition context; handlers observe the token."""
        state = self._state(deployment_id)
        state.status = DeploymentStatus.DRAINING
        state.cancellation.cancel("deployment draining")
        for partition in state.partitions.values():
            if partition.status in _ACTIVE and partition.cancellation is not None:
                partition.cancellation.cancel("deployment draining")

    def partitions(self, deployment_id: DeploymentId) -> tuple[PartitionRecord, ...]:
        """Return observed partition assignment records."""
        state = self._state(deployment_id)
        return tuple(
            PartitionRecord(
                partition_id=partition.partition.partition_id,
                status=partition.status,
                owner_id=partition.owner_id,
                fencing_token=0 if partition.lease is None else partition.lease.fencing_token,
            )
            for partition in sorted(
                state.partitions.values(), key=lambda p: str(p.partition.partition_id)
            )
        )

    def emissions(
        self,
        partition_id: str | None = None,
    ) -> tuple[EmissionRecord, ...]:
        """Return recorded emissions, optionally limited to one partition."""
        if partition_id is None:
            return tuple(self._emissions)
        target = PartitionId(partition_id)
        return tuple(e for e in self._emissions if e.partition_id == target)

    def _assign(self, state: _DeploymentState, partition: _PartitionState) -> None:
        owner = self._least_loaded_instance(state)
        if partition.cancellation is not None:
            partition.cancellation.cancel("partition reassigned")
        partition.lease = self.leases.acquire(
            deployment_id=state.deployment_id,
            partition_id=partition.partition.partition_id,
            owner_id=owner,
        )
        partition.owner_id = owner
        partition.cancellation = state.cancellation.child()
        partition.task = None
        partition.status = PartitionStatus.ASSIGNED

    def _tick_deployment(self, state: _DeploymentState) -> None:
        for partition in state.partitions.values():
            if partition.task is not None and partition.task.done():
                self._harvest(partition)
            if state.status is not DeploymentStatus.ACTIVE:
                continue
            if partition.status is PartitionStatus.ASSIGNED and partition.task is None:
                self._start(state, partition)
            elif (
                partition.status is PartitionStatus.RUNNING
                and partition.task is not None
                and partition.lease is not None
                and self.leases.is_current(partition.lease)
            ):
                self.leases.renew(partition.lease)
            if partition.status is PartitionStatus.RUNNING and (
                partition.lease is None or not self.leases.is_current(partition.lease)
            ):
                if partition.cancellation is not None:
                    partition.cancellation.cancel("lease lost")
                partition.status = PartitionStatus.UNASSIGNED
                partition.owner_id = None
                partition.lease = None

    def _start(self, state: _DeploymentState, partition: _PartitionState) -> None:
        assert partition.lease is not None
        assert partition.cancellation is not None
        context = PartitionContext(
            workload_id=WorkloadId(str(state.registration.name)),
            partition=partition.partition,
            lease=partition.lease,
            sinks=self._sinks,
            cancellation=partition.cancellation.token,
        )
        workload: ContinuousWorkload = state.registration.workload
        partition.task = asyncio.create_task(workload.run_partition(context, partition.partition))
        partition.status = PartitionStatus.RUNNING

    def _harvest(self, partition: _PartitionState) -> None:
        assert partition.task is not None
        failed = False
        if not partition.task.cancelled() and partition.task.exception() is not None:
            failed = True
        if partition.lease is not None:
            self.leases.release(partition.lease)
        partition.task = None
        partition.owner_id = None
        partition.status = PartitionStatus.FAILED if failed else PartitionStatus.UNASSIGNED

    def _release_partition(self, partition: _PartitionState, status: PartitionStatus) -> None:
        if partition.cancellation is not None:
            partition.cancellation.cancel(f"partition {status.value}")
        if partition.task is not None:
            partition.task.cancel()
            partition.task = None
        if partition.lease is not None:
            self.leases.release(partition.lease)
            partition.lease = None
        partition.owner_id = None
        partition.status = status

    def _least_loaded_instance(self, state: _DeploymentState) -> RuntimeInstanceId:
        load = dict.fromkeys(self._instances, 0)
        for partition in state.partitions.values():
            if partition.status in _ACTIVE and partition.owner_id in load:
                assert partition.owner_id is not None
                load[partition.owner_id] += partition.partition.weight
        return min(self._instances, key=lambda i: (load[i], str(i)))

    def _select_deployments(self, deployment_id: DeploymentId | None) -> list[_DeploymentState]:
        if deployment_id is not None:
            return [self._state(deployment_id)]
        return list(self._deployments.values())

    def _state(self, deployment_id: DeploymentId) -> _DeploymentState:
        try:
            return self._deployments[deployment_id]
        except KeyError as error:
            raise LookupError(f"unknown deployment: {deployment_id}") from error
