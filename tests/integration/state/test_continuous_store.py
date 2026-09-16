"""IT-SQL-CON / IT-LEASE / IT-EMIT / IT-HEART: continuous + outbox + workers."""

from __future__ import annotations

import asyncio

import pytest

from distributed_runtime.continuous import Partition
from distributed_runtime.core import (
    DeploymentId,
    InvariantViolationError,
    PartitionId,
    PartitionStatus,
    RevisionId,
    RuntimeInstanceId,
    VersionedEnvelope,
    WorkloadId,
)
from distributed_runtime.state import StateEngine, migrate
from distributed_runtime.state.continuous import ContinuousStateStore
from distributed_runtime.state.outbox import OutboxMessage, OutboxStore
from distributed_runtime.state.workers import WorkerRegistry

from .conftest import requires_postgres

OWNER = RuntimeInstanceId("instance:c-0")
OTHER = RuntimeInstanceId("instance:c-1")
DEPLOY = DeploymentId("deployment:test-1")


def _envelope(seq: int = 0) -> VersionedEnvelope:
    return VersionedEnvelope(
        schema_version=1,
        message_kind="continuous.emit",
        run_id=str(DEPLOY),
        execution_id="partition:p-0",
        idempotency_key=f"p-0:{seq}",
        application="test",
        workload="w",
        workload_version="1.0.0",
        handler="w",
        attempt_generation=1,
        execution_class="stateful-stream",
        runtime_pool_revision="local",
        published_at="2026-01-01T00:00:00Z",
        payload={"seq": seq},
    )


async def _store(dsn: str) -> tuple[StateEngine, ContinuousStateStore]:
    engine = await StateEngine.connect(dsn)
    await migrate(engine)
    store = ContinuousStateStore(engine)
    await store.create_deployment(
        deployment_id=DEPLOY,
        workload_id=WorkloadId("workload:c"),
        workload_version="1.0.0",
    )
    return engine, store


@requires_postgres
def test_lease_acquire_renew_release_cycle(clean_state: str) -> None:
    async def exercise() -> None:
        engine, store = await _store(clean_state)
        try:
            await store.sync_partitions(
                DEPLOY, [Partition(partition_id=PartitionId("p-0"), payload={})]
            )
            lease = await store.acquire_lease(DEPLOY, PartitionId("p-0"), OWNER, lease_seconds=60)
            assert lease is not None and lease.fencing_token == 1
            # second owner cannot take a live lease
            denied = await store.acquire_lease(DEPLOY, PartitionId("p-0"), OTHER, lease_seconds=60)
            assert denied is None
            assert await store.renew_lease(lease, lease_seconds=60)
            assert await store.release_lease(lease)
            taken = await store.acquire_lease(DEPLOY, PartitionId("p-0"), OTHER, lease_seconds=60)
            assert taken is not None and taken.fencing_token == 2
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_expired_lease_can_be_taken_over(clean_state: str) -> None:
    async def exercise() -> None:
        engine, store = await _store(clean_state)
        try:
            await store.sync_partitions(
                DEPLOY, [Partition(partition_id=PartitionId("p-0"), payload={})]
            )
            lease = await store.acquire_lease(DEPLOY, PartitionId("p-0"), OWNER, lease_seconds=60)
            assert lease is not None
            async with engine.acquire() as connection:
                await connection.execute(
                    """
                    UPDATE runtime_state.continuous_partitions
                    SET lease_expires_at = now() - interval '1 second'
                    WHERE deployment_id = $1
                    """,
                    str(DEPLOY),
                )
            taken = await store.acquire_lease(DEPLOY, PartitionId("p-0"), OTHER, lease_seconds=60)
            assert taken is not None and taken.fencing_token == 2
            # stale owner can no longer renew or emit
            assert not await store.renew_lease(lease, lease_seconds=60)
            with pytest.raises(InvariantViolationError, match="stale"):
                await store.emit(
                    lease,
                    destination="events",
                    stable_id="x",
                    envelope=_envelope(1),
                )
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_emission_records_outbox_with_dedup(clean_state: str) -> None:
    async def exercise() -> None:
        engine, store = await _store(clean_state)
        try:
            await store.sync_partitions(
                DEPLOY, [Partition(partition_id=PartitionId("p-0"), payload={})]
            )
            lease = await store.acquire_lease(DEPLOY, PartitionId("p-0"), OWNER, lease_seconds=60)
            assert lease is not None
            first = await store.emit(
                lease, destination="events", stable_id="s-1", envelope=_envelope(0)
            )
            second = await store.emit(
                lease, destination="events", stable_id="s-1", envelope=_envelope(0)
            )
            assert first == second  # dedup converges on one outbox row
            outbox = OutboxStore(engine)
            pending = await outbox.pending()
            assert len(pending) == 1
            assert pending[0].kind == "emission"
            assert pending[0].fencing_token == lease.fencing_token
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_outbox_dispatch_marks_published(clean_state: str) -> None:
    sent: list[str] = []

    async def exercise() -> None:
        engine, store = await _store(clean_state)
        try:
            await store.sync_partitions(
                DEPLOY, [Partition(partition_id=PartitionId("p-0"), payload={})]
            )
            lease = await store.acquire_lease(DEPLOY, PartitionId("p-0"), OWNER, lease_seconds=60)
            assert lease is not None
            await store.emit(lease, destination="events", stable_id="s-1", envelope=_envelope(0))
            outbox = OutboxStore(engine)

            async def sender(message: OutboxMessage) -> None:
                sent.append(message.dedup_key)

            count = await outbox.dispatch(sender)
            assert count == 1
            assert await outbox.pending() == ()
        finally:
            await engine.close()

    asyncio.run(exercise())
    assert sent == [f"{DEPLOY}:p-0:s-1"]


@requires_postgres
def test_partition_sync_marks_missing_inactive(clean_state: str) -> None:
    async def exercise() -> None:
        engine, store = await _store(clean_state)
        try:
            await store.sync_partitions(
                DEPLOY,
                [
                    Partition(partition_id=PartitionId("p-0"), payload={}),
                    Partition(partition_id=PartitionId("p-1"), payload={}),
                ],
            )
            partitions = await store.sync_partitions(
                DEPLOY, [Partition(partition_id=PartitionId("p-0"), payload={})]
            )
            by_id = {str(p.partition_id): p.status for p in partitions}
            assert by_id["p-1"] is PartitionStatus.INACTIVE
            # rediscovery reactivates
            partitions = await store.sync_partitions(
                DEPLOY,
                [
                    Partition(partition_id=PartitionId("p-0"), payload={}),
                    Partition(partition_id=PartitionId("p-1"), payload={}),
                ],
            )
            by_id = {str(p.partition_id): p.status for p in partitions}
            assert by_id["p-1"] is PartitionStatus.UNASSIGNED
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_checkpoint_records_with_fencing(clean_state: str) -> None:
    async def exercise() -> None:
        engine, store = await _store(clean_state)
        try:
            await store.sync_partitions(
                DEPLOY, [Partition(partition_id=PartitionId("p-0"), payload={})]
            )
            lease = await store.acquire_lease(DEPLOY, PartitionId("p-0"), OWNER, lease_seconds=60)
            assert lease is not None
            outbox_id = await store.record_checkpoint(
                lease,
                destination="checkpoints",
                checkpoint_key="cursor:1",
                envelope=_envelope(9),
            )
            assert outbox_id > 0
            outbox = OutboxStore(engine)
            pending = await outbox.pending()
            assert pending[0].kind == "checkpoint"
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_worker_heartbeat_and_stale_detection(clean_state: str) -> None:
    async def exercise() -> None:
        engine = await StateEngine.connect(clean_state)
        try:
            await migrate(engine)
            workers = WorkerRegistry(engine)
            worker = await workers.register(
                RuntimeInstanceId("instance:w-0"),
                execution_classes=["lightweight"],
                revision=RevisionId("rev:1"),
            )
            assert worker.status == "active"
            assert await workers.heartbeat(worker.instance_id)
            async with engine.acquire() as connection:
                await connection.execute(
                    """
                    UPDATE runtime_state.worker_instances
                    SET last_heartbeat_at = now() - interval '1 hour'
                    WHERE instance_id = $1
                    """,
                    str(worker.instance_id),
                )
            dead = await workers.mark_stale_dead(stale_after_seconds=60)
            assert dead == 1
            assert (await workers.list_active()) == ()
            assert (await workers.get(worker.instance_id)).status == "dead"  # type: ignore[union-attr]

            # a resumed heartbeat revives a transiently-dead worker
            assert await workers.heartbeat(worker.instance_id)
            assert (await workers.get(worker.instance_id)).status == "active"  # type: ignore[union-attr]

            # but an explicit drain survives heartbeats
            await workers.mark_status(worker.instance_id, "draining")
            assert await workers.heartbeat(worker.instance_id)
            assert (await workers.get(worker.instance_id)).status == "draining"  # type: ignore[union-attr]
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_list_deployments_filters_by_status(clean_state: str) -> None:
    async def exercise() -> None:
        engine, store = await _store(clean_state)
        try:
            from distributed_runtime.core import DeploymentStatus

            other = DeploymentId("deployment:test-2")
            await store.create_deployment(
                deployment_id=other,
                workload_id=WorkloadId("workload:c"),
                workload_version="1.0.0",
            )
            everything = await store.list_deployments()
            assert {d.deployment_id for d in everything} == {DEPLOY, other}
            await store.transition_deployment(DEPLOY, DeploymentStatus.ACTIVE)
            active = await store.list_deployments([DeploymentStatus.ACTIVE])
            assert [d.deployment_id for d in active] == [DEPLOY]
            pending = await store.list_deployments([DeploymentStatus.PENDING])
            assert [d.deployment_id for d in pending] == [other]
        finally:
            await engine.close()

    asyncio.run(exercise())
