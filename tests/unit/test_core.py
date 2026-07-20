"""UT-CORE: identifier and enumeration baseline."""

from __future__ import annotations

import pytest

from distributed_runtime.core import (
    ErrorKind,
    ExecutionId,
    ExecutionStatus,
    InvalidIdentifierError,
    PartitionStatus,
    RunId,
    RunStatus,
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
