"""IT-PLANNER / IT-RETRY-01 / IT-WORKER-FIN / FT-OUTBOX / client API.

End-to-end finite path against real Postgres: submit → plan (with outbox
dispatch rows) → worker claims and executes → product reads results.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping

from distributed_runtime import RuntimeApplication
from distributed_runtime.control.client import RuntimeClient
from distributed_runtime.control.dispatcher import OutboxDispatcher
from distributed_runtime.control.planner import PlannerRunner
from distributed_runtime.core import (
    FinitePolicy,
    JsonValue,
    PermanentExecutionError,
    RetryableExecutionError,
    RevisionId,
    RunStatus,
    RuntimeInstanceId,
    WorkloadMode,
)
from distributed_runtime.finite import ExecutionContext, ExecutionUnit, WorkloadRequest
from distributed_runtime.state import StateEngine, migrate
from distributed_runtime.state.finite import FiniteStateStore
from distributed_runtime.state.outbox import OutboxMessage, OutboxStore
from distributed_runtime.worker.finite import FiniteWorker

from .conftest import requires_postgres


class RangeSumPlanner:
    name = "range.sum"
    version = "1.0.0"
    mode = WorkloadMode.FINITE

    async def __call__(self, request: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
        lo = int(str(request.payload["start"]))
        hi = int(str(request.payload["end"]))
        while lo < hi:
            step = min(lo + 5, hi)
            yield ExecutionUnit(
                unit_key=f"range:{lo}-{step}",
                handler="range_sum.partial",
                payload={"start": lo, "end": step},
                execution_class="lightweight",
                timeout_seconds=30,
                max_attempts=3,
            )
            lo = step


async def sum_partial(context: ExecutionContext, payload: Mapping[str, JsonValue]) -> JsonValue:
    return {"subtotal": sum(range(int(str(payload["start"])), int(str(payload["end"]))))}


def _app() -> RuntimeApplication:
    app = RuntimeApplication("example-product")
    app.registry.register_finite(
        name="range.sum",
        semantic_version="1.0.0",
        execution_class="lightweight",
        planner=RangeSumPlanner(),
        handler=sum_partial,
    )
    return app


async def _setup(dsn: str) -> tuple[StateEngine, RuntimeClient, PlannerRunner, FiniteWorker]:
    engine = await StateEngine.connect(dsn)
    await migrate(engine)
    app = _app()
    client = RuntimeClient(
        engine,
        application="example-product",
        planner_revision=RevisionId("rev:1"),
        execution_revision=RevisionId("rev:1"),
    )
    planner = PlannerRunner(
        registry=app.registry,
        engine=engine,
        dispatch_topic="runtime-units",
        application="example-product",
    )
    worker = FiniteWorker(
        registry=app.registry,
        engine=engine,
        instance_id=RuntimeInstanceId("instance:w-0"),
        policy=FinitePolicy(timeout_seconds=60),
    )
    return engine, client, planner, worker


@requires_postgres
def test_full_finite_path_submit_plan_execute_results(clean_state: str) -> None:
    async def exercise() -> tuple[int, RunStatus]:
        engine, client, planner, worker = await _setup(clean_state)
        try:
            submitted = await client.submit(
                workload="range.sum",
                version="1.0.0",
                input={"start": 0, "end": 20},
            )
            outcome = await planner.plan(submitted.run_id)
            assert outcome.unit_count == 4  # 0-5,5-10,10-15,15-20
            for _ in range(4):
                await worker.poll_once(limit=4)
            status = await client.wait(submitted.run_id, poll_seconds=0.05)
            partials = await client.results(submitted.run_id)
            total = 0
            for partial in partials:
                assert isinstance(partial, dict)
                total += int(str(partial["subtotal"]))
            return total, status
        finally:
            await engine.close()

    total, status = asyncio.run(exercise())
    assert status is RunStatus.SUCCEEDED
    assert total == sum(range(20))  # 190


@requires_postgres
def test_plan_persists_dispatch_outbox_rows(clean_state: str) -> None:
    async def exercise() -> int:
        engine, client, planner, _ = await _setup(clean_state)
        try:
            submitted = await client.submit(
                workload="range.sum",
                version="1.0.0",
                input={"start": 0, "end": 10},
            )
            await planner.plan(submitted.run_id)
            pending = await OutboxStore(engine).pending()
            assert all(m.kind == "unit_dispatch" for m in pending)
            # dispatch wakeups carry the run's pinned execution revision so
            # per-revision subscriptions route them to the right pool
            assert all(m.envelope.runtime_pool_revision == "rev:1" for m in pending)
            return len(pending)
        finally:
            await engine.close()

    assert asyncio.run(exercise()) == 2


@requires_postgres
def test_retryable_failure_reschedules_then_succeeds(clean_state: str) -> None:
    calls: list[int] = []

    class FlakyPlanner(RangeSumPlanner):
        pass

    async def flaky(context: ExecutionContext, payload: Mapping[str, JsonValue]) -> JsonValue:
        calls.append(1)
        if len(calls) == 1:
            raise RetryableExecutionError("try again")
        return await sum_partial(context, payload)

    async def exercise() -> RunStatus:
        engine = await StateEngine.connect(clean_state)
        await migrate(engine)
        app = RuntimeApplication("example-product")
        app.registry.register_finite(
            name="range.sum",
            semantic_version="1.0.0",
            execution_class="lightweight",
            planner=FlakyPlanner(),
            handler=flaky,
        )
        client = RuntimeClient(
            engine,
            application="example-product",
            planner_revision=RevisionId("rev:1"),
            execution_revision=RevisionId("rev:1"),
        )
        planner = PlannerRunner(
            registry=app.registry,
            engine=engine,
            dispatch_topic="runtime-units",
            application="example-product",
        )
        worker = FiniteWorker(
            registry=app.registry,
            engine=engine,
            instance_id=RuntimeInstanceId("instance:w-0"),
            policy=FinitePolicy(retry_base_seconds=1, retry_cap_seconds=1),
        )
        try:
            submitted = await client.submit(
                workload="range.sum",
                version="1.0.0",
                input={"start": 0, "end": 5},
            )
            await planner.plan(submitted.run_id)
            first = await worker.poll_once(limit=1)
            assert first[0].outcome == "retryable"
            # make the retry due now and re-claim
            async with engine.acquire() as connection:
                await connection.execute(
                    "UPDATE runtime_state.finite_units SET next_attempt_at = now()"
                )
            second = await worker.poll_once(limit=1)
            assert second[0].outcome == "succeeded"
            return await client.wait(submitted.run_id, poll_seconds=0.05)
        finally:
            await engine.close()

    assert asyncio.run(exercise()) is RunStatus.SUCCEEDED


@requires_postgres
def test_permanent_failure_dead_letters(clean_state: str) -> None:
    async def always_fails(
        context: ExecutionContext, payload: Mapping[str, JsonValue]
    ) -> JsonValue:
        raise PermanentExecutionError("nope")

    async def exercise() -> RunStatus:
        engine = await StateEngine.connect(clean_state)
        await migrate(engine)
        app = RuntimeApplication("example-product")
        app.registry.register_finite(
            name="range.sum",
            semantic_version="1.0.0",
            execution_class="lightweight",
            planner=RangeSumPlanner(),
            handler=always_fails,
        )
        client = RuntimeClient(
            engine,
            application="example-product",
            planner_revision=RevisionId("rev:1"),
            execution_revision=RevisionId("rev:1"),
        )
        planner = PlannerRunner(
            registry=app.registry,
            engine=engine,
            dispatch_topic="runtime-units",
            application="example-product",
        )
        worker = FiniteWorker(
            registry=app.registry,
            engine=engine,
            instance_id=RuntimeInstanceId("instance:w-0"),
        )
        try:
            submitted = await client.submit(
                workload="range.sum",
                version="1.0.0",
                input={"start": 0, "end": 5},
            )
            await planner.plan(submitted.run_id)
            handled = await worker.poll_once(limit=1)
            assert handled[0].outcome == "permanent"
            store = FiniteStateStore(engine)
            states = await store.unit_states(submitted.run_id)
            from distributed_runtime.core import ExecutionStatus

            assert states[0].status is ExecutionStatus.DEAD_LETTERED
            return await client.wait(submitted.run_id, poll_seconds=0.05)
        finally:
            await engine.close()

    assert asyncio.run(exercise()) is RunStatus.FAILED


@requires_postgres
def test_crash_claim_expires_and_unit_recovers(clean_state: str) -> None:
    async def exercise() -> RunStatus:
        engine, client, planner, worker = await _setup(clean_state)
        try:
            submitted = await client.submit(
                workload="range.sum",
                version="1.0.0",
                input={"start": 0, "end": 5},
            )
            await planner.plan(submitted.run_id)
            store = FiniteStateStore(engine)
            # worker 0 claims then "crashes" — never completes
            crashed = await store.claim_units(RuntimeInstanceId("instance:dead"), limit=1)
            assert len(crashed) == 1
            # expire the claim; another worker must be able to reclaim
            async with engine.acquire() as connection:
                await connection.execute(
                    """
                    UPDATE runtime_state.finite_units
                    SET claim_expires_at = now() - interval '1 second',
                        status = 'ready'
                    WHERE run_id = $1
                    """,
                    str(submitted.run_id),
                )
            handled = await worker.poll_once(limit=1)
            assert handled[0].outcome == "succeeded"
            return await client.wait(submitted.run_id, poll_seconds=0.05)
        finally:
            await engine.close()

    assert asyncio.run(exercise()) is RunStatus.SUCCEEDED


@requires_postgres
def test_dispatcher_enqueues_due_retries(clean_state: str) -> None:
    async def exercise() -> int:
        engine, client, planner, _worker = await _setup(clean_state)
        try:
            submitted = await client.submit(
                workload="range.sum",
                version="1.0.0",
                input={"start": 0, "end": 5},
            )
            await planner.plan(submitted.run_id)
            # mark a unit retry_scheduled and due
            async with engine.acquire() as connection:
                await connection.execute(
                    """
                    UPDATE runtime_state.finite_units
                    SET status = 'retry_scheduled', next_attempt_at = now(),
                        attempt_count = 1
                    WHERE run_id = $1 AND unit_key = 'range:0-5'
                    """,
                    str(submitted.run_id),
                )
            dispatcher = OutboxDispatcher(
                engine,
                application="example-product",
                dispatch_topic="runtime-unit-dispatch",
            )
            sent: list[OutboxMessage] = []

            async def sender(m: OutboxMessage) -> None:
                sent.append(m)

            cycle = await dispatcher.cycle(sender)
            assert cycle.retries_enqueued == 1
            assert cycle.published >= 1
            keys = [m.dedup_key for m in sent]
            assert any("retry:1" in k for k in keys)
            retry = next(m for m in sent if "retry:1" in m.dedup_key)
            # retries route to the run's pinned execution revision, not the
            # control plane's own pool revision
            assert retry.envelope.runtime_pool_revision == "rev:1"
            return cycle.retries_enqueued
        finally:
            await engine.close()

    assert asyncio.run(exercise()) == 1
