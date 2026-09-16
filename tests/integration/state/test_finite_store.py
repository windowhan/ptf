"""IT-PLAN-REPO / IT-SQL-FIN / IT-CLAIM: finite state repository tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from distributed_runtime.core import (
    ExecutionStatus,
    InvariantViolationError,
    RevisionId,
    RunId,
    RunStatus,
    RuntimeInstanceId,
    WorkloadId,
)
from distributed_runtime.finite import ExecutionUnit, PlanningRecord, WorkloadRequest
from distributed_runtime.state import StateEngine, migrate
from distributed_runtime.state.finite import FiniteStateStore

from .conftest import requires_postgres

OWNER = RuntimeInstanceId("instance:test-0")
OTHER = RuntimeInstanceId("instance:test-1")


def _units(count: int = 3) -> list[ExecutionUnit]:
    return [
        ExecutionUnit(
            unit_key=f"u:{index}",
            handler="h.noop",
            payload={"n": index},
            execution_class="lightweight",
            timeout_seconds=60,
            max_attempts=3,
        )
        for index in range(count)
    ]


async def _store(dsn: str) -> tuple[StateEngine, FiniteStateStore]:
    engine = await StateEngine.connect(dsn)
    await migrate(engine)
    return engine, FiniteStateStore(engine)


async def _seed(dsn: str, count: int = 3) -> tuple[StateEngine, FiniteStateStore, RunId]:
    engine, store = await _store(dsn)
    run_id = RunId("run:test-1")
    await store.submit_run(
        run_id=run_id,
        workload_id=WorkloadId("workload:test"),
        workload_name="test.work",
        workload_version="1.0.0",
        planner_revision=RevisionId("rev:1"),
        execution_revision=RevisionId("rev:1"),
        input={"x": 1},
    )
    await store.record_plan(run_id, _units(count))
    return engine, store, run_id


def _request(run_id: RunId) -> WorkloadRequest:
    return WorkloadRequest(
        run_id=run_id,
        workload_id=WorkloadId("workload:test"),
        planner_revision=RevisionId("rev:1"),
        execution_revision=RevisionId("rev:1"),
        payload={},
    )


@requires_postgres
def test_submit_is_idempotent_and_plan_persists(clean_state: str) -> None:
    async def exercise() -> None:
        engine, store = await _store(clean_state)
        try:
            run_id = RunId("run:dup")
            kwargs = dict(
                run_id=run_id,
                workload_id=WorkloadId("workload:t"),
                workload_name="t",
                workload_version="1.0.0",
                planner_revision=RevisionId("rev:1"),
                execution_revision=RevisionId("rev:1"),
                input={},
            )
            first = await store.submit_run(**kwargs)  # type: ignore[arg-type]
            second = await store.submit_run(**kwargs)  # type: ignore[arg-type]
            assert first == second

            await store.record_plan(run_id, _units(2))
            plan = await store.load_plan(run_id, planning_generation=1)
            assert [r.unit_key for r in plan] == ["u:0", "u:1"]
            assert all(isinstance(r, PlanningRecord) for r in plan)
            run = await store.get_run(run_id)
            assert run is not None and run.status is RunStatus.RUNNING
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_claim_grants_exclusive_ownership(clean_state: str) -> None:
    async def exercise() -> None:
        engine, store, run_id = await _seed(clean_state)
        try:
            claimed = await store.claim_units(OWNER, limit=2)
            assert len(claimed) == 2
            assert {c.attempt for c in claimed} == {1}
            again = await store.claim_units(OTHER, limit=5)
            assert len(again) == 1  # only the unclaimed third unit
            assert again[0].claim_token == 1
            states = await store.unit_states(run_id)
            running = [s for s in states if s.status is ExecutionStatus.RUNNING]
            assert len(running) == 3
            owners = {s.unit_key: s.claimed_by for s in running}
            assert OTHER in owners.values()
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_complete_attempt_records_result(clean_state: str) -> None:
    async def exercise() -> None:
        engine, store, run_id = await _seed(clean_state, count=1)
        try:
            (claimed,) = await store.claim_units(OWNER, limit=1)
            await store.complete_attempt(
                claimed,
                execution_id=claimed.unit.execution_id(_request(run_id)),
                outcome="succeeded",
                result={"subtotal": 42},
            )
            states = await store.unit_states(run_id)
            assert states[0].status is ExecutionStatus.SUCCEEDED
            assert states[0].result == {"subtotal": 42}
            results = await store.results(run_id)
            assert results == ({"subtotal": 42},)
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_stale_claim_token_rejected(clean_state: str) -> None:
    async def exercise() -> None:
        engine, store, run_id = await _seed(clean_state, count=1)
        try:
            (claimed,) = await store.claim_units(OWNER, limit=1)
            # simulate claim expiry then reclaim by another instance
            async with engine.acquire() as connection:
                await connection.execute(
                    """
                    UPDATE runtime_state.finite_units
                    SET claim_expires_at = now() - interval '1 second',
                        status = 'ready'
                    WHERE run_id = $1
                    """,
                    str(run_id),
                )
            (reclaimed,) = await store.claim_units(OTHER, limit=1)
            assert reclaimed.claim_token > claimed.claim_token
            with pytest.raises(InvariantViolationError, match="stale claim"):
                await store.complete_attempt(
                    claimed,
                    execution_id=claimed.unit.execution_id(_request(run_id)),
                    outcome="succeeded",
                    result={"subtotal": 1},
                )
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_retry_schedules_next_attempt(clean_state: str) -> None:
    async def exercise() -> None:
        engine, store, run_id = await _seed(clean_state, count=1)
        try:
            (claimed,) = await store.claim_units(OWNER, limit=1)
            later = datetime.now(UTC) + timedelta(hours=1)
            await store.complete_attempt(
                claimed,
                execution_id=claimed.unit.execution_id(_request(run_id)),
                outcome="retryable",
                error={"kind": "retryable_execution", "message": "flaky"},
                retry_at=later,
            )
            assert await store.claim_units(OTHER, limit=1) == ()
            states = await store.unit_states(run_id)
            assert states[0].status is ExecutionStatus.RETRY_SCHEDULED
            assert states[0].last_error is not None
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_finalize_run_status(clean_state: str) -> None:
    async def exercise() -> None:
        engine, store, run_id = await _seed(clean_state, count=2)
        try:
            claimed = await store.claim_units(OWNER, limit=2)
            for unit in claimed:
                await store.complete_attempt(
                    unit,
                    execution_id=unit.unit.execution_id(_request(run_id)),
                    outcome="succeeded",
                    result={"ok": True},
                )
            status = await store.finalize_run_if_done(run_id)
            assert status is RunStatus.SUCCEEDED
            run = await store.get_run(run_id)
            assert run is not None and run.status is RunStatus.SUCCEEDED
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_dead_lettered_unit_fails_run(clean_state: str) -> None:
    async def exercise() -> None:
        engine, store, run_id = await _seed(clean_state, count=1)
        try:
            (claimed,) = await store.claim_units(OWNER, limit=1)
            await store.complete_attempt(
                claimed,
                execution_id=claimed.unit.execution_id(_request(run_id)),
                outcome="permanent",
                error={"kind": "permanent_execution", "message": "bad"},
            )
            async with engine.acquire() as connection:
                await connection.execute(
                    """
                    UPDATE runtime_state.finite_units SET status = 'dead_lettered'
                    WHERE run_id = $1
                    """,
                    str(run_id),
                )
            status = await store.finalize_run_if_done(run_id)
            assert status is RunStatus.FAILED
        finally:
            await engine.close()

    asyncio.run(exercise())
