"""UT-CORE: identifiers, enumerations, and structured failures."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import cast

import pytest

from distributed_runtime.core import (
    CancelledExecutionError,
    ErrorKind,
    ExecutionId,
    ExecutionStatus,
    InvalidIdentifierError,
    PartitionStatus,
    PermanentExecutionError,
    PlanningDriftError,
    RateLimitedExecutionError,
    RetryableExecutionError,
    RunId,
    RunStatus,
    RuntimeContractError,
    WorkloadId,
    WorkloadMode,
)
from distributed_runtime.core.identifiers import Identifier


def test_identifier_is_immutable_and_string_form_is_stable() -> None:
    run_id = RunId.parse("run:2026-07-20/0001")

    assert str(run_id) == "run:2026-07-20/0001"
    assert run_id.value == "run:2026-07-20/0001"
    assert hash(run_id) == hash(RunId("run:2026-07-20/0001"))

    with pytest.raises(AttributeError):
        run_id.value = "replacement"  # type: ignore[misc]


def test_identifier_types_do_not_compare_equal() -> None:
    run_id: object = RunId("shared")
    execution_id: object = ExecutionId("shared")
    workload_id: object = WorkloadId("shared")

    assert run_id != execution_id
    assert workload_id != run_id


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        ("", "must not be empty"),
        (" leading", "leading or trailing whitespace"),
        ("trailing ", "leading or trailing whitespace"),
        ("contains space", "unsupported characters"),
        ("snowman-☃", "unsupported characters"),
        ("x" * 256, "at most 255 characters"),
    ],
)
def test_identifier_rejects_ambiguous_boundary_values(value: str, reason: str) -> None:
    with pytest.raises(InvalidIdentifierError, match=reason) as raised:
        RunId(value)

    assert raised.value.kind is ErrorKind.INVALID_IDENTIFIER
    assert raised.value.details["identifier_kind"] == "run_id"
    assert raised.value.details["value"] == value


def test_identifier_base_supports_domain_neutral_extension() -> None:
    class AdapterResourceId(Identifier):
        kind = "adapter_resource_id"

    resource_id = AdapterResourceId.parse("adapter/resource-1")

    assert type(resource_id) is AdapterResourceId
    assert resource_id.value == "adapter/resource-1"


def test_enums_are_stable_strings() -> None:
    assert WorkloadMode.FINITE.value == "finite"
    assert WorkloadMode.CONTINUOUS.value == "continuous"
    assert RunStatus.PLANNING.value == "planning"
    assert ExecutionStatus.DEAD_LETTERED.value == "dead_lettered"
    assert PartitionStatus.BLOCKED_SINK.value == "blocked_sink"
    assert ErrorKind.CANCELLED.value == "cancelled"


@pytest.mark.parametrize(
    ("error", "kind", "code", "retryable"),
    [
        (
            CancelledExecutionError("shutdown requested"),
            ErrorKind.CANCELLED,
            "cancelled_execution",
            False,
        ),
        (
            RetryableExecutionError("temporary"),
            ErrorKind.RETRYABLE_EXECUTION,
            "retryable_execution",
            True,
        ),
        (
            PermanentExecutionError("invalid payload"),
            ErrorKind.PERMANENT_EXECUTION,
            "permanent_execution",
            False,
        ),
        (
            PlanningDriftError("checksum changed"),
            ErrorKind.PLANNING_DRIFT,
            "planning_drift",
            False,
        ),
    ],
)
def test_execution_error_classification(
    error: RuntimeContractError,
    kind: ErrorKind,
    code: str,
    retryable: bool,
) -> None:
    diagnostic = error.as_dict()

    assert error.kind is kind
    assert error.retryable is retryable
    assert diagnostic == {
        "code": code,
        "kind": kind.value,
        "message": str(error),
        "retryable": retryable,
        "details": {},
    }


def test_error_details_are_defensively_copied() -> None:
    source_details: dict[str, object] = {"attempt": 2}
    error = RetryableExecutionError("temporary", details=source_details)

    source_details["attempt"] = 99

    assert error.details == {"attempt": 2}
    with pytest.raises(TypeError):
        error.details["attempt"] = 3


def test_error_details_are_deeply_immutable_and_json_compatible() -> None:
    nested: dict[str, object] = {"items": [{"attempt": 1}]}
    error = RetryableExecutionError("temporary", details=nested)
    nested_items = nested["items"]
    assert isinstance(nested_items, list)
    nested_items[0]["attempt"] = 99

    assert error.details["items"] == ({"attempt": 1},)
    with pytest.raises(TypeError):
        error.details["items"][0]["attempt"] = 2
    assert json.loads(json.dumps(error.as_dict()))["details"] == {"items": [{"attempt": 1}]}


@pytest.mark.parametrize(
    "details",
    [
        {1: "invalid-key"},
        {"value": object()},
        {"value": float("nan")},
        {"value": float("inf")},
    ],
)
def test_error_details_reject_non_json_values(details: object) -> None:
    with pytest.raises(
        (TypeError, ValueError),
        match=r"detail|JSON|finite|keys|unsupported",
    ):
        RetryableExecutionError(
            "temporary",
            details=cast(dict[str, object], details),
        )


class FalseyDetails(dict[str, object]):
    def __bool__(self) -> bool:
        return False


def test_runtime_error_preserves_falsey_mapping_details() -> None:
    error = RetryableExecutionError(
        "temporary",
        details=FalseyDetails(provider="example"),
    )

    assert error.as_dict()["details"] == {"provider": "example"}


def test_rate_limit_error_preserves_falsey_mapping_details() -> None:
    error = RateLimitedExecutionError(
        "quota exhausted",
        retry_after=timedelta(seconds=60),
        details=FalseyDetails(provider="example"),
    )

    assert error.as_dict()["details"] == {
        "provider": "example",
        "retry_after_seconds": 60.0,
    }


def test_rate_limit_error_requires_positive_retry_delay() -> None:
    error = RateLimitedExecutionError(
        "quota exhausted",
        retry_after=timedelta(seconds=60),
        details={"provider": "example"},
    )

    assert error.retryable is True
    assert error.kind is ErrorKind.RATE_LIMITED
    assert error.retry_after == timedelta(seconds=60)
    assert error.as_dict()["details"] == {
        "provider": "example",
        "retry_after_seconds": 60.0,
    }

    with pytest.raises(ValueError, match="retry_after must be positive"):
        RateLimitedExecutionError("quota exhausted", retry_after=timedelta(0))


def test_runtime_error_rejects_empty_message() -> None:
    with pytest.raises(ValueError, match="message must not be empty"):
        RuntimeContractError("")
