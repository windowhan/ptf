"""IT for continuous subsystem: reconciler + supervisor + admin (IT-LEASE/EMIT/HEART paths)."""

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
from distributed_runtime.control.admin import ContinuousAdmin
from distributed_runtime.registry import ContinuousRegistration
from distributed_runtime.control.reconciler import Reconciler
from distributed_runtime.core import (
    InvariantViolationError,
    PartitionId,
    PartitionStatus,
    RevisionId,
    RuntimeInstanceId,
    VersionedEnvelope,
    WorkloadMode,
)
from distributed_runtime.state import StateEngine, migrate
from distributed_runtime.state.outbox import OutboxStore
from distributed_runtime.state.workers import WorkerRegistry
from distributed_runtime.worker.continuous import ContinuousSupervisor

from .conftest import requires_postgres

W0 = RuntimeInstanceId("instance:cw-0")
W1 = RuntimeInstanceId("instance:cw-1")


def _envelope(pid: str, seq: int) -> VersionedEnvelope:
    return VersionedEnvelope(
        schema_version=1,
        message_kind="continuous.emit",
        run_id="deployment:x",
        execution_id=f"partition:{pid}",
        idempotency_key=f"{pid}:{seq}",
        application="example-product",
        workload="event.stream",
        workload_version="1.0.0",
        handler="event.stream",
        attempt_generation=1,
        execution_class="stateful-stream",
        runtime_pool_revision="local",
        published_at="2026-01-01T00:00:00Z",
        payload={"p": pid, "seq": seq},
    )


class EmitOnceStream:
    name = "event.stream"
    version = "1.0.0"
    mode = WorkloadMode.CONTINUOUS

    def __init__(self, count: int = 2) -> None:
        self.count = count

    async def discover_partitions(self) -> Sequence[Partition]:
        return [
            Partition(partition_id=PartitionId(f"p-{i}"), payload={}) for i in range(self.count)
        ]

    async def run_partition(self, context: PartitionContext, partition: Partition) -> None:
        context.cancellation.raise_if_cancelled()
        await context.emit(
            "events",
            _envelope(str(partition.partition_id), 0),
            stable_id=f"{partition.partition_id}:0",
        )


def _app(stream: EmitOnceStream) -> RuntimeApplication:
    app = RuntimeApplication("example-product")

    class _Sink:
        guarantee = SinkGuarantee.RUNTIME_FENCED

        async def emit(
            self,
            event: VersionedEnvelope,
            *,
            stable_id: str,
            lease: LeaseHandle,
        ) -> None:
            return None

    app.registry.register_continuous(
        name="event.stream",
        semantic_version="1.0.0",
        execution_class="stateful-stream",
        workload=stream,
    )
    app.registry.register_sink("events", _Sink())
    return app


async def _setup(
    dsn: str, stream: EmitOnceStream | None = None
) -> tuple[StateEngine, RuntimeApplication, ContinuousAdmin, Reconciler, WorkerRegistry]:
    engine = await StateEngine.connect(dsn)
    await migrate(engine)
    app = _app(stream or EmitOnceStream())
    admin = ContinuousAdmin(engine, application="example-product")
    reconciler = Reconciler(engine)
    workers = WorkerRegistry(engine)
    return engine, app, admin, reconciler, workers


@requires_postgres
def test_deploy_reconcile_assigns_to_live_workers(clean_state: str) -> None:
    async def exercise() -> None:
        engine, app, admin, reconciler, workers = await _setup(clean_state)
        try:
            await workers.register(
                W0, execution_classes=["stateful-stream"], revision=RevisionId("rev:1")
            )
            deployment = await admin.deploy(workload="event.stream", version="1.0.0")
            reg0 = app.registry.workload("event.stream", "1.0.0", WorkloadMode.CONTINUOUS)
            assert isinstance(reg0, ContinuousRegistration)
            result = await reconciler.reconcile(
                deployment.deployment_id,
                await reg0.workload.discover_partitions(),
            )
            assert result.assigned == 2
            partitions = await reconciler._store.partitions(deployment.deployment_id)
            assert all(p.owner_id == W0 for p in partitions)
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_supervisor_runs_partitions_and_emits(clean_state: str) -> None:
    async def exercise() -> int:
        engine, app, admin, reconciler, workers = await _setup(clean_state)
        try:
            await workers.register(
                W0, execution_classes=["stateful-stream"], revision=RevisionId("rev:1")
            )
            deployment = await admin.deploy(workload="event.stream", version="1.0.0")
            reg = app.registry.workload("event.stream", "1.0.0", WorkloadMode.CONTINUOUS)
            assert isinstance(reg, ContinuousRegistration)
            desired = await reg.workload.discover_partitions()
            await reconciler.reconcile(deployment.deployment_id, desired)
            supervisor = ContinuousSupervisor(registry=app.registry, engine=engine, instance_id=W0)
            running = await supervisor.tick(deployment.deployment_id)
            assert running == 2
            await asyncio.sleep(0)  # let handler tasks run
            await asyncio.sleep(0)
            await supervisor.stop_all()
            pending = await OutboxStore(engine).pending()
            return len(pending)
        finally:
            await engine.close()

    assert asyncio.run(exercise()) == 2  # one emission per partition


@requires_postgres
def test_dead_worker_partitions_get_reclaimed(clean_state: str) -> None:
    async def exercise() -> None:
        engine, app, admin, reconciler, workers = await _setup(clean_state)
        try:
            await workers.register(
                W0, execution_classes=["stateful-stream"], revision=RevisionId("rev:1")
            )
            await workers.register(
                W1, execution_classes=["stateful-stream"], revision=RevisionId("rev:1")
            )
            deployment = await admin.deploy(workload="event.stream", version="1.0.0")
            reg = app.registry.workload("event.stream", "1.0.0", WorkloadMode.CONTINUOUS)
            assert isinstance(reg, ContinuousRegistration)
            desired = await reg.workload.discover_partitions()
            await reconciler.reconcile(deployment.deployment_id, desired)

            # W0 dies: heartbeat goes stale
            async with engine.acquire() as connection:
                await connection.execute(
                    """
                    UPDATE runtime_state.worker_instances
                    SET last_heartbeat_at = now() - interval '1 hour'
                    WHERE instance_id = $1
                    """,
                    str(W0),
                )
                await connection.execute(
                    """
                    UPDATE runtime_state.continuous_partitions
                    SET lease_expires_at = now() - interval '1 second'
                    WHERE deployment_id = $1
                    """,
                    str(deployment.deployment_id),
                )
            result = await reconciler.reconcile(deployment.deployment_id, desired)
            assert result.dead_workers == 1
            partitions = await reconciler._store.partitions(deployment.deployment_id)
            assert all(p.owner_id == W1 for p in partitions)
            assert all(p.fencing_token >= 2 for p in partitions)
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_stale_owner_emission_rejected(clean_state: str) -> None:
    async def exercise() -> None:
        engine, app, admin, reconciler, workers = await _setup(clean_state)
        try:
            await workers.register(
                W0, execution_classes=["stateful-stream"], revision=RevisionId("rev:1")
            )
            await workers.register(
                W1, execution_classes=["stateful-stream"], revision=RevisionId("rev:1")
            )
            deployment = await admin.deploy(workload="event.stream", version="1.0.0")
            reg = app.registry.workload("event.stream", "1.0.0", WorkloadMode.CONTINUOUS)
            assert isinstance(reg, ContinuousRegistration)
            desired = await reg.workload.discover_partitions()
            await reconciler.reconcile(deployment.deployment_id, desired)

            from distributed_runtime.state.continuous import ContinuousStateStore

            store = ContinuousStateStore(engine)
            stale = LeaseHandle(
                deployment_id=deployment.deployment_id,
                partition_id=PartitionId("p-0"),
                owner_id=W1,
                fencing_token=999,
            )
            with pytest.raises(InvariantViolationError, match="stale"):
                await store.emit(
                    stale,
                    destination="events",
                    stable_id="x",
                    envelope=_envelope("p-0", 0),
                )
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_drain_releases_partitions(clean_state: str) -> None:
    async def exercise() -> None:
        engine, app, admin, reconciler, workers = await _setup(clean_state)
        try:
            await workers.register(
                W0, execution_classes=["stateful-stream"], revision=RevisionId("rev:1")
            )
            deployment = await admin.deploy(workload="event.stream", version="1.0.0")
            reg = app.registry.workload("event.stream", "1.0.0", WorkloadMode.CONTINUOUS)
            assert isinstance(reg, ContinuousRegistration)
            desired = await reg.workload.discover_partitions()
            await reconciler.reconcile(deployment.deployment_id, desired)
            await admin.drain(deployment.deployment_id)
            view = await admin.view(deployment.deployment_id)
            assert view.deployment.status.value == "draining"
            assert all(p.status is PartitionStatus.UNASSIGNED for p in view.partitions)
            # draining deployment does not reassign
            result = await reconciler.reconcile(deployment.deployment_id, desired)
            assert result.assigned == 0
        finally:
            await engine.close()

    asyncio.run(exercise())
