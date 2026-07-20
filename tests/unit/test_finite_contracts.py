"""UT-FIN-01: finite planning, handler, and context contracts."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import replace
from typing import Any, cast

import pytest

from distributed_runtime.core import (
    CancellationError,
    CancellationSource,
    JsonValue,
    PlanningDriftError,
    RevisionId,
    RunId,
    WorkloadId,
)
from distributed_runtime.finite import (
    ExecutionUnit,
    FinitePlanner,
    PlanningRecord,
    WorkloadRequest,
    validate_plan,
)


def make_request() -> WorkloadRequest:
    return WorkloadRequest(
        run_id=RunId("run:1"),
        workload_id=WorkloadId("snapshot"),
        planner_revision=RevisionId("planner:v1"),
        execution_revision=RevisionId("worker:v2"),
        payload={"items": ["A", "B"]},
    )


def make_unit(
    *,
    unit_key: str = "item:A",
    handler: str = "snapshot.item",
    payload: Mapping[str, JsonValue] | None = None,
    execution_class: str = "browser",
    timeout_seconds: int = 300,
    max_attempts: int = 5,
    idempotency_key: str | None = None,
) -> ExecutionUnit:
    return ExecutionUnit(
        unit_key=unit_key,
        handler=handler,
        payload=({"item_id": "A", "options": {"fresh": True}} if payload is None else payload),
        execution_class=execution_class,
        timeout_seconds=timeout_seconds,
        max_attempts=max_attempts,
        idempotency_key=idempotency_key,
    )


def test_request_pins_revisions_and_freezes_payload() -> None:
    source_items: list[JsonValue] = ["A"]
    request = WorkloadRequest(
        run_id=RunId("run:1"),
        workload_id=WorkloadId("snapshot"),
        planner_revision=RevisionId("planner:v1"),
        execution_revision=RevisionId("worker:v2"),
        payload={"items": source_items},
    )
    source_items.append("late")

    assert cast(object, request.payload["items"]) == ("A",)
    with pytest.raises(TypeError):
        request.payload["other"] = True  # type: ignore[index]


def test_execution_unit_defaults_are_explicit_and_immutable() -> None:
    unit = make_unit()

    assert unit.timeout_seconds == 300
    assert unit.max_attempts == 5
    assert unit.idempotency_key == "item:A"
    assert unit.payload["options"] == {"fresh": True}
    with pytest.raises(TypeError):
        unit.payload["item_id"] = "B"  # type: ignore[index]


def test_payload_hash_is_canonical_and_sensitive_to_content() -> None:
    first = make_unit(payload={"b": 2, "a": [1, True]})
    reordered = make_unit(payload={"a": [1, True], "b": 2})
    changed = make_unit(payload={"a": [1, False], "b": 2})

    assert first.payload_sha256 == reordered.payload_sha256
    assert first.payload_sha256 != changed.payload_sha256


def test_planning_record_binds_ordinal_key_and_payload_hash() -> None:
    unit = make_unit()

    assert unit.planning_record(3) == PlanningRecord(
        ordinal=3,
        unit_key="item:A",
        payload_sha256=unit.payload_sha256,
    )


def test_execution_id_is_stable_and_revision_pinned() -> None:
    request = make_request()
    unit = make_unit()
    assert unit.execution_id(request) == unit.execution_id(request)
    changed = WorkloadRequest(
        run_id=request.run_id,
        workload_id=request.workload_id,
        planner_revision=request.planner_revision,
        execution_revision=RevisionId("worker:v3"),
        payload=request.payload,
    )
    assert unit.execution_id(request) != unit.execution_id(changed)


def test_validate_plan_handles_cardinality_drift_duplicates_and_cancellation() -> None:
    async def empty(_: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
        if _.payload.get("never") is True:
            yield make_unit()

    async def duplicate(_: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
        yield make_unit(payload={"value": 1})
        yield make_unit(payload={"value": 2})

    async def exercise() -> None:
        request = make_request()
        assert await validate_plan(request, cast(FinitePlanner, empty)) == ()
        units = await validate_plan(request, cast(FinitePlanner, planner))
        assert len(units) == 2
        expected = tuple(unit.planning_record(index) for index, unit in enumerate(units))
        assert (
            await validate_plan(request, cast(FinitePlanner, planner), expected=expected) == units
        )
        with pytest.raises(PlanningDriftError, match="duplicate unit key"):
            await validate_plan(request, cast(FinitePlanner, duplicate))
        with pytest.raises(PlanningDriftError, match="durable sequence"):
            await validate_plan(request, cast(FinitePlanner, empty), expected=expected)
        with pytest.raises(PlanningDriftError, match="durable sequence"):
            await validate_plan(request, cast(FinitePlanner, planner), expected=())
        cancellation = CancellationSource()
        cancellation.cancel("run cancelled")
        invoked = False

        async def must_not_start(_: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
            nonlocal invoked
            invoked = True
            yield make_unit()

        with pytest.raises(CancellationError, match="run cancelled"):
            await validate_plan(
                request,
                cast(FinitePlanner, must_not_start),
                cancellation=cancellation.token,
            )
        assert invoked is False

    asyncio.run(exercise())


def test_validate_plan_rejects_identical_duplicate_unit_keys() -> None:
    request = make_request()
    duplicate = make_unit()

    async def duplicate_planner(_: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
        yield duplicate
        yield duplicate

    with pytest.raises(PlanningDriftError, match="duplicate unit key"):
        asyncio.run(validate_plan(request, cast(FinitePlanner, duplicate_planner)))


def test_finite_payloads_reject_non_string_json_mapping_keys() -> None:
    invalid = cast(Mapping[str, JsonValue], {1: "top-level"})
    nested = cast(Mapping[str, JsonValue], {"nested": {1: "invalid"}})

    with pytest.raises(TypeError, match="payload keys must be strings"):
        WorkloadRequest(
            run_id=RunId("run:1"),
            workload_id=WorkloadId("snapshot"),
            planner_revision=RevisionId("planner:v1"),
            execution_revision=RevisionId("worker:v2"),
            payload=invalid,
        )
    with pytest.raises(TypeError, match="payload keys must be strings"):
        make_unit(payload=nested)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"unit_key": ""}, "unit_key"),
        ({"handler": "contains space"}, "handler"),
        ({"execution_class": ""}, "execution_class"),
        ({"idempotency_key": ""}, "idempotency_key"),
        ({"timeout_seconds": 0}, "timeout_seconds"),
        ({"timeout_seconds": 3_601}, "timeout_seconds"),
        ({"max_attempts": 0}, "max_attempts"),
    ],
)
def test_execution_unit_rejects_invalid_boundaries(
    overrides: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        replace(make_unit(), **cast(Any, overrides))


@pytest.mark.parametrize(
    ("factory", "value"),
    [
        (lambda value: make_unit(timeout_seconds=value), 1.0),
        (lambda value: make_unit(max_attempts=value), 1.0),
        (
            lambda value: PlanningRecord(
                ordinal=value,
                unit_key="item:A",
                payload_sha256="0" * 64,
            ),
            0.0,
        ),
    ],
)
def test_finite_integer_boundaries_reject_floats(
    factory: Any,
    value: float,
) -> None:
    with pytest.raises(ValueError, match=r"integer|between"):
        factory(value)


async def planner(request: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
    items = cast(tuple[object, ...], request.payload["items"])
    for item in items:
        assert isinstance(item, str)
        yield make_unit(unit_key=f"item:{item}", payload={"item_id": item})
