"""IT-LOCAL-FIN: deterministic finite local test kit behavior."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from datetime import timedelta
from typing import cast

import pytest

from distributed_runtime import RuntimeApplication
from distributed_runtime.core import (
    ExecutionStatus,
    JsonValue,
    PermanentExecutionError,
    PlanningDriftError,
    RateLimitedExecutionError,
    RetryableExecutionError,
    RunId,
    RunStatus,
    WorkloadMode,
)
from distributed_runtime.finite import (
    ExecutionContext,
    ExecutionResult,
    ExecutionUnit,
    FiniteHandler,
    FinitePlanner,
    WorkloadRequest,
)
from distributed_runtime.registry import RegistrationError
from distributed_runtime.testing import AttemptRecord, FiniteRuntimeTestKit


class RangePlanner:
    """Split ``[start, end)`` into chunk-sized units, in request order."""

    name = "range.sum"
    version = "1.0.0"
    mode = WorkloadMode.FINITE

    def __init__(self, chunk: int = 10, extra: tuple[ExecutionUnit, ...] = ()) -> None:
        self.chunk = chunk
        self.extra = list(extra)

    async def __call__(self, request: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
        lo = cast(int, request.payload["start"])
        hi = cast(int, request.payload["end"])
        while lo < hi:
            step = min(lo + self.chunk, hi)
            yield ExecutionUnit(
                unit_key=f"range:{lo}-{step}",
                handler="range_sum.partial",
                payload={"start": lo, "end": step},
                execution_class="lightweight",
            )
            lo = step
        for unit in self.extra:
            yield unit


async def sum_partial(
    _: ExecutionContext,
    payload: Mapping[str, JsonValue],
) -> ExecutionResult:
    return {"subtotal": sum(range(cast(int, payload["start"]), cast(int, payload["end"])))}


class ScriptedHandler:
    """Record invocations and fail a chosen unit a fixed number of times."""

    def __init__(self, *, failures: dict[str, list[Exception]] | None = None) -> None:
        self.calls: list[tuple[str, int]] = []
        self._failures = {} if failures is None else failures

    async def __call__(
        self,
        context: ExecutionContext,
        payload: Mapping[str, JsonValue],
    ) -> ExecutionResult:
        self.calls.append((context.idempotency_key, context.attempt))
        scripted = self._failures.get(context.idempotency_key)
        if scripted:
            raise scripted.pop(0)
        return await sum_partial(context, payload)


def _kit(
    handler: FiniteHandler | None = None,
    planner: FinitePlanner | None = None,
) -> FiniteRuntimeTestKit:
    app = RuntimeApplication("test-product")
    app.registry.register_finite(
        name="range.sum",
        semantic_version="1.0.0",
        execution_class="lightweight",
        planner=planner or RangePlanner(),
        handler=cast(FiniteHandler, sum_partial) if handler is None else handler,
    )
    return FiniteRuntimeTestKit(registry=app.registry)


def test_submit_run_pending_and_product_side_aggregation() -> None:
    async def exercise() -> tuple[FiniteRuntimeTestKit, RunId]:
        kit = _kit()
        run_id = await kit.submit(
            workload="range.sum",
            version="1.0.0",
            payload={"start": 0, "end": 25},
        )
        await kit.run_pending()
        return kit, run_id

    kit, run_id = asyncio.run(exercise())
    run = kit.run(run_id)
    assert run.status is RunStatus.SUCCEEDED
    assert [a.unit_key for a in run.attempts] == [
        "range:0-10",
        "range:10-20",
        "range:20-25",
    ]
    assert all(a.status is ExecutionStatus.SUCCEEDED for a in run.attempts)
    total = 0
    for result in run.results:
        assert isinstance(result, dict)
        total += cast(int, result["subtotal"])
    assert total == sum(range(25))


def test_retryable_error_waits_for_backoff_before_succeeding() -> None:
    handler = ScriptedHandler(
        failures={"range:0-10": [RetryableExecutionError("temporary")]},
    )

    async def exercise() -> None:
        kit = _kit(handler)
        run_id = await kit.submit(
            workload="range.sum", version="1.0.0", payload={"start": 0, "end": 10}
        )
        first = await kit.run_pending()
        assert [a.status for a in first] == [ExecutionStatus.RETRY_SCHEDULED]
        assert await kit.step() is None
        kit.clock.advance(4)
        assert await kit.step() is None
        kit.clock.advance(1)
        await kit.run_pending()
        assert kit.run(run_id).status is RunStatus.SUCCEEDED

    asyncio.run(exercise())
    assert handler.calls == [("range:0-10", 1), ("range:0-10", 2)]


def test_permanent_error_fails_without_retry() -> None:
    handler = ScriptedHandler(
        failures={"range:0-10": [PermanentExecutionError("bad input")]},
    )

    async def exercise() -> tuple[FiniteRuntimeTestKit, RunId]:
        kit = _kit(handler)
        run_id = await kit.submit(
            workload="range.sum", version="1.0.0", payload={"start": 0, "end": 10}
        )
        await kit.run_pending()
        return kit, run_id

    kit, run_id = asyncio.run(exercise())
    run = kit.run(run_id)
    assert run.status is RunStatus.FAILED
    assert [a.status for a in run.attempts] == [ExecutionStatus.FAILED]
    assert run.attempts[0].error is not None
    assert run.attempts[0].error["code"] == "permanent_execution"


def test_exhausting_max_attempts_dead_letters_the_unit() -> None:
    planner = RangePlanner(
        extra=(
            ExecutionUnit(
                unit_key="range:0-1",
                handler="range_sum.partial",
                payload={"start": 0, "end": 1},
                execution_class="lightweight",
                max_attempts=2,
            ),
        ),
    )
    handler = ScriptedHandler(
        failures={"range:0-1": [RetryableExecutionError("x"), RetryableExecutionError("y")]},
    )

    async def exercise() -> tuple[FiniteRuntimeTestKit, RunId]:
        kit = _kit(handler, planner=planner)
        run_id = await kit.submit(
            workload="range.sum", version="1.0.0", payload={"start": 0, "end": 0}
        )
        await kit.run_pending()
        kit.clock.advance(5)
        await kit.run_pending()
        return kit, run_id

    kit, run_id = asyncio.run(exercise())
    run = kit.run(run_id)
    assert run.status is RunStatus.FAILED
    assert [a.status for a in run.attempts] == [
        ExecutionStatus.RETRY_SCHEDULED,
        ExecutionStatus.DEAD_LETTERED,
    ]


def test_rate_limited_unit_waits_for_retry_after() -> None:
    handler = ScriptedHandler(
        failures={
            "range:0-10": [
                RateLimitedExecutionError("slow down", retry_after=timedelta(seconds=30))
            ]
        },
    )

    async def exercise() -> FiniteRuntimeTestKit:
        kit = _kit(handler)
        await kit.submit(workload="range.sum", version="1.0.0", payload={"start": 0, "end": 10})
        await kit.run_pending()
        kit.clock.advance(29)
        assert await kit.step() is None
        kit.clock.advance(1)
        await kit.run_pending()
        return kit

    kit = asyncio.run(exercise())
    assert [a.status for a in kit.attempts()] == [
        ExecutionStatus.RETRY_SCHEDULED,
        ExecutionStatus.SUCCEEDED,
    ]


def test_cancelling_a_run_marks_pending_units_cancelled() -> None:
    handler = ScriptedHandler()

    async def exercise() -> tuple[FiniteRuntimeTestKit, RunId]:
        kit = _kit(handler)
        run_id = await kit.submit(
            workload="range.sum", version="1.0.0", payload={"start": 0, "end": 20}
        )
        kit.cancel(run_id, reason="operator stop")
        assert await kit.step() is None
        return kit, run_id

    kit, run_id = asyncio.run(exercise())
    run = kit.run(run_id)
    assert run.status is RunStatus.CANCELLED
    assert handler.calls == []
    assert run.attempts == ()


def test_duplicate_delivery_invokes_handler_again_and_is_recorded() -> None:
    handler = ScriptedHandler()

    async def exercise() -> tuple[FiniteRuntimeTestKit, RunId, AttemptRecord]:
        kit = _kit(handler)
        run_id = await kit.submit(
            workload="range.sum", version="1.0.0", payload={"start": 0, "end": 10}
        )
        await kit.run_pending()
        record = await kit.redeliver("range:0-10")
        return kit, run_id, record

    kit, run_id, record = asyncio.run(exercise())
    assert record.duplicate is True
    assert record.status is ExecutionStatus.SUCCEEDED
    assert handler.calls == [("range:0-10", 1), ("range:0-10", 2)]
    assert len(kit.results(run_id)) == 1


def test_replan_detects_drift_against_durable_records() -> None:
    planner = RangePlanner()

    async def exercise() -> None:
        kit = _kit(planner=planner)
        run_id = await kit.submit(
            workload="range.sum", version="1.0.0", payload={"start": 0, "end": 10}
        )
        assert len(await kit.replan(run_id)) == 1
        planner.chunk = 5
        with pytest.raises(PlanningDriftError):
            await kit.replan(run_id)

    asyncio.run(exercise())


def test_empty_plan_succeeds_immediately() -> None:
    async def exercise() -> tuple[FiniteRuntimeTestKit, RunId]:
        kit = _kit()
        run_id = await kit.submit(
            workload="range.sum", version="1.0.0", payload={"start": 5, "end": 5}
        )
        assert await kit.step() is None
        return kit, run_id

    kit, run_id = asyncio.run(exercise())
    assert kit.run(run_id).status is RunStatus.SUCCEEDED


def test_execution_ids_are_stable_per_unit_but_bound_to_the_run() -> None:
    async def exercise() -> tuple[FiniteRuntimeTestKit, RunId, RunId]:
        kit = _kit()
        first = await kit.submit(
            workload="range.sum", version="1.0.0", payload={"start": 0, "end": 10}
        )
        second = await kit.submit(
            workload="range.sum", version="1.0.0", payload={"start": 0, "end": 10}
        )
        await kit.run_pending()
        return kit, first, second

    kit, first, second = asyncio.run(exercise())
    first_id = kit.attempts(first)[0].execution_id
    second_id = kit.attempts(second)[0].execution_id
    assert first_id != second_id
    assert str(first_id).startswith("execution:")


def test_submit_rejects_unknown_or_mismatched_workloads() -> None:
    kit = _kit()

    async def exercise() -> None:
        with pytest.raises(RegistrationError, match="unknown workload"):
            await kit.submit(workload="missing", version="1.0.0", payload={})
        with pytest.raises(RegistrationError, match="unknown workload"):
            await kit.submit(workload="range.sum", version="9.9.9", payload={})

    asyncio.run(exercise())


def test_lookups_and_redeliver_reject_unknown_or_unfinished_state() -> None:
    async def exercise() -> FiniteRuntimeTestKit:
        kit = _kit()
        await kit.submit(workload="range.sum", version="1.0.0", payload={"start": 0, "end": 10})
        with pytest.raises(LookupError, match="has not completed"):
            await kit.redeliver("range:0-10")
        with pytest.raises(LookupError, match="expected exactly one"):
            await kit.redeliver("range:nope")
        return kit

    kit = asyncio.run(exercise())
    with pytest.raises(LookupError, match="unknown run"):
        kit.run(RunId("run:missing"))
