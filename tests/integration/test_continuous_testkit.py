"""IT-LOCAL-CON: deterministic continuous local test kit behavior."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest

from distributed_runtime import RuntimeApplication
from distributed_runtime.continuous import (
    LeaseHandle,
    Partition,
    PartitionContext,
    SinkGuarantee,
)
from distributed_runtime.core import (
    ContinuousPolicy,
    DeploymentId,
    InvariantViolationError,
    PartitionId,
    PartitionStatus,
    RuntimeInstanceId,
    VersionedEnvelope,
    WorkloadMode,
)
from distributed_runtime.testing import (
    ContinuousRuntimeTestKit,
    PartitionRecord,
    RecordingSink,
)


def _envelope(name: str, seq: int) -> VersionedEnvelope:
    return VersionedEnvelope(
        schema_version=1,
        message_kind="continuous.emit",
        run_id="deployment:local-1",
        execution_id=f"partition:{name}",
        idempotency_key=f"{name}:{seq}",
        application="test-product",
        workload="event.stream",
        workload_version="1.0.0",
        handler="event.stream",
        attempt_generation=1,
        execution_class="stateful-stream",
        runtime_pool_revision="local",
        published_at="2026-01-01T00:00:00Z",
        payload={"partition": name, "seq": seq},
    )


class NullSink:
    """Product sink that accepts every fenced emission."""

    guarantee = SinkGuarantee.RUNTIME_FENCED

    def __init__(self) -> None:
        self.received: list[str] = []

    async def emit(
        self,
        event: VersionedEnvelope,
        *,
        stable_id: str,
        lease: LeaseHandle,
    ) -> None:
        del event, lease
        self.received.append(stable_id)


class EmittingStream:
    """Emit a fixed number of events per partition, then return."""

    name = "event.stream"
    version = "1.0.0"
    mode = WorkloadMode.CONTINUOUS

    def __init__(self, partition_count: int = 2, events: int = 2) -> None:
        self.partition_count = partition_count
        self.events = events

    async def discover_partitions(self) -> Sequence[Partition]:
        return [
            Partition(partition_id=PartitionId(f"partition-{index}"), payload={})
            for index in range(self.partition_count)
        ]

    async def run_partition(self, context: PartitionContext, partition: Partition) -> None:
        for seq in range(self.events):
            context.cancellation.raise_if_cancelled()
            event = _envelope(str(partition.partition_id), seq)
            await context.emit("events", event, stable_id=f"{partition.partition_id}:{seq}")
            await asyncio.sleep(0)


class PersistentStream(EmittingStream):
    """Run until cancelled; count emissions and note stale-write rejections."""

    def __init__(self, partition_count: int = 1) -> None:
        super().__init__(partition_count=partition_count, events=0)
        self.rejected: list[str] = []

    async def run_partition(self, context: PartitionContext, partition: Partition) -> None:
        seq = 0
        while not context.cancellation.is_cancelled:
            event = _envelope(str(partition.partition_id), seq)
            await context.emit("events", event, stable_id=f"{partition.partition_id}:{seq}")
            seq += 1
            await asyncio.sleep(0)


class ZombieStream(PersistentStream):
    """Keep emitting after lease loss to exercise fencing rejection."""

    async def run_partition(self, context: PartitionContext, partition: Partition) -> None:
        seq = 0
        while True:
            event = _envelope(str(partition.partition_id), seq)
            try:
                await context.emit("events", event, stable_id=f"{partition.partition_id}:z{seq}")
            except InvariantViolationError:
                self.rejected.append(str(partition.partition_id))
                return
            seq += 1
            await asyncio.sleep(0)


def _kit(
    workload: EmittingStream | None = None,
    *,
    policy: ContinuousPolicy | None = None,
) -> tuple[ContinuousRuntimeTestKit, EmittingStream]:
    app = RuntimeApplication("test-product")
    stream = EmittingStream() if workload is None else workload
    app.registry.register_continuous(
        name="event.stream",
        semantic_version="1.0.0",
        execution_class="stateful-stream",
        workload=stream,
    )
    app.registry.register_sink("events", NullSink())
    return ContinuousRuntimeTestKit(registry=app.registry, policy=policy), stream


def test_deploy_reconcile_assigns_and_runs_partitions() -> None:
    async def exercise() -> ContinuousRuntimeTestKit:
        kit, _ = _kit()
        deployment = kit.deploy(workload="event.stream", version="1.0.0")
        records = await kit.reconcile(deployment)
        assert [r.status for r in records] == [
            PartitionStatus.ASSIGNED,
            PartitionStatus.ASSIGNED,
        ]
        await kit.run_for(5)
        return kit

    kit = asyncio.run(exercise())
    emissions = kit.emissions()
    assert sorted(e.stable_id for e in emissions) == [
        "partition-0:0",
        "partition-0:1",
        "partition-1:0",
        "partition-1:1",
    ]


def test_lease_renewal_keeps_owner_across_expiry_window() -> None:
    stream = PersistentStream()

    async def exercise() -> tuple[ContinuousRuntimeTestKit, DeploymentId]:
        kit, _ = _kit(
            stream,
            policy=ContinuousPolicy(
                heartbeat_seconds=2, lease_seconds=10, renew_before_expiry_seconds=4
            ),
        )
        deployment = kit.deploy(workload="event.stream", version="1.0.0")
        await kit.reconcile(deployment)
        await kit.run_for(30)
        return kit, deployment

    kit, deployment = asyncio.run(exercise())
    records = kit.partitions(deployment)
    assert len(records) == 1
    assert records[0].status is PartitionStatus.RUNNING
    assert records[0].fencing_token == 1


def test_expire_lease_reassigns_with_larger_fencing_token() -> None:
    stream = PersistentStream()

    async def exercise() -> tuple[ContinuousRuntimeTestKit, DeploymentId]:
        kit, _ = _kit(stream)
        deployment = kit.deploy(workload="event.stream", version="1.0.0")
        await kit.reconcile(deployment)
        await kit.run_for(2)
        first = kit.partitions(deployment)[0]
        kit.expire_lease("partition-0")
        await kit.reconcile(deployment)
        await kit.run_for(2)
        second = kit.partitions(deployment)[0]
        assert second.fencing_token > first.fencing_token
        assert second.status is PartitionStatus.RUNNING
        return kit, deployment

    asyncio.run(exercise())


def test_stale_owner_writes_are_rejected() -> None:
    stream = ZombieStream()

    async def exercise() -> tuple[ContinuousRuntimeTestKit, DeploymentId]:
        kit, _ = _kit(stream)
        deployment = kit.deploy(workload="event.stream", version="1.0.0")
        await kit.reconcile(deployment)
        await kit.run_for(2)
        kit.expire_lease("partition-0")
        await kit.reconcile(deployment)
        await kit.run_for(5)
        return kit, deployment

    kit, deployment = asyncio.run(exercise())
    assert stream.rejected == ["partition-0"]
    records = kit.partitions(deployment)
    assert records[0].fencing_token == 2


def test_stable_id_dedup_survives_takeover() -> None:
    async def exercise() -> ContinuousRuntimeTestKit:
        kit, _ = _kit()
        deployment = kit.deploy(workload="event.stream", version="1.0.0")
        await kit.reconcile(deployment)
        await kit.run_for(2)
        kit.expire_lease("partition-0")
        await kit.reconcile(deployment)
        await kit.run_for(5)
        return kit

    kit = asyncio.run(exercise())
    stable_ids = [e.stable_id for e in kit.emissions("partition-0")]
    assert stable_ids == sorted(set(stable_ids))
    assert len(stable_ids) == len(set(stable_ids))


def test_drain_cancels_partition_handlers() -> None:
    stream = PersistentStream()

    async def exercise() -> tuple[ContinuousRuntimeTestKit, DeploymentId]:
        kit, _ = _kit(stream)
        deployment = kit.deploy(workload="event.stream", version="1.0.0")
        await kit.reconcile(deployment)
        await kit.run_for(2)
        kit.drain(deployment)
        await kit.run_for(2)
        await kit.reconcile(deployment)
        return kit, deployment

    kit, deployment = asyncio.run(exercise())
    records = kit.partitions(deployment)
    assert records[0].status is PartitionStatus.UNASSIGNED


def test_removed_partition_becomes_inactive() -> None:
    stream = EmittingStream(partition_count=2, events=0)

    async def exercise() -> tuple[PartitionRecord, ...]:
        kit, _ = _kit(stream)
        deployment = kit.deploy(workload="event.stream", version="1.0.0")
        await kit.reconcile(deployment)
        stream.partition_count = 1
        return await kit.reconcile(deployment)

    records = asyncio.run(exercise())
    by_id = {str(r.partition_id): r.status for r in records}
    assert by_id["partition-1"] is PartitionStatus.INACTIVE


def test_handler_failure_marks_partition_failed() -> None:
    class FailingStream(EmittingStream):
        async def run_partition(self, context: PartitionContext, partition: Partition) -> None:
            raise RuntimeError("boom")

    async def exercise() -> tuple[ContinuousRuntimeTestKit, DeploymentId]:
        kit, _ = _kit(FailingStream(partition_count=1))
        deployment = kit.deploy(workload="event.stream", version="1.0.0")
        await kit.reconcile(deployment)
        await kit.run_for(2)
        return kit, deployment

    kit, deployment = asyncio.run(exercise())
    assert kit.partitions(deployment)[0].status is PartitionStatus.FAILED


def test_emit_to_unknown_sink_fails_the_partition() -> None:
    class UnknownSinkStream(EmittingStream):
        async def run_partition(self, context: PartitionContext, partition: Partition) -> None:
            await context.emit("missing", _envelope("p", 0), stable_id="x")

    async def exercise() -> tuple[ContinuousRuntimeTestKit, DeploymentId]:
        kit, _ = _kit(UnknownSinkStream(partition_count=1))
        deployment = kit.deploy(workload="event.stream", version="1.0.0")
        await kit.reconcile(deployment)
        await kit.run_for(2)
        return kit, deployment

    kit, deployment = asyncio.run(exercise())
    assert kit.partitions(deployment)[0].status is PartitionStatus.FAILED


def test_assignment_prefers_least_loaded_instance() -> None:
    stream = EmittingStream(partition_count=4, events=0)

    async def exercise() -> tuple[ContinuousRuntimeTestKit, DeploymentId]:
        kit, _ = _kit(stream)
        kit.add_instance()
        deployment = kit.deploy(workload="event.stream", version="1.0.0")
        await kit.reconcile(deployment)
        return kit, deployment

    kit, deployment = asyncio.run(exercise())
    owners = [str(r.owner_id) for r in kit.partitions(deployment)]
    assert owners.count("instance:local-0") == 2
    assert owners.count("instance:local-1") == 2


def test_recording_sink_rejects_fabricated_lease() -> None:
    async def exercise() -> None:
        kit, _ = _kit()
        deployment = kit.deploy(workload="event.stream", version="1.0.0")
        await kit.reconcile(deployment)
        sink = RecordingSink(lease_manager=kit.leases)
        stale = LeaseHandle(
            deployment_id=deployment,
            partition_id=PartitionId("partition-0"),
            owner_id=RuntimeInstanceId("instance:local-0"),
            fencing_token=999,
        )
        with pytest.raises(InvariantViolationError, match="stale or expired"):
            await sink.emit(_envelope("p", 0), stable_id="x", lease=stale)

    asyncio.run(exercise())


def test_unknown_deployment_and_partition_lookups_fail() -> None:
    kit, _ = _kit()
    with pytest.raises(LookupError, match="unknown deployment"):
        kit.partitions(DeploymentId("deployment:x"))
    with pytest.raises(LookupError, match="unknown partition"):
        kit.expire_lease("partition-nope")
