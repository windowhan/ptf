"""Finite planner, execution-unit, handler, and context contracts."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from distributed_runtime.core.artifacts import ArtifactReference
from distributed_runtime.core.enums import WorkloadMode
from distributed_runtime.core.envelope import JsonValue, freeze_json_mapping
from distributed_runtime.core.errors import PlanningDriftError
from distributed_runtime.core.identifiers import (
    ExecutionId,
    RevisionId,
    RunId,
    WorkloadId,
)
from distributed_runtime.core.lifecycle import CancellationSource, CancellationToken, Deadline
from distributed_runtime.core.logging import LogContext

_CONTRACT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$")

type ExecutionResult = JsonValue | ArtifactReference


def _immutable_payload(payload: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
    return freeze_json_mapping(payload)


def _canonical_payload(payload: Mapping[str, JsonValue]) -> bytes:
    return json.dumps(
        _mutable_json(payload),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _mutable_json(value: object) -> JsonValue:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("payload keys must be strings")
        return {key: _mutable_json(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_mutable_json(item) for item in value]
    if value is None or isinstance(value, str | int | float | bool):
        return value
    raise TypeError(f"unsupported payload value: {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class WorkloadRequest:
    """Immutable planner input with revisions pinned at submission."""

    run_id: RunId
    workload_id: WorkloadId
    planner_revision: RevisionId
    execution_revision: RevisionId
    payload: Mapping[str, JsonValue]

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", _immutable_payload(self.payload))


@dataclass(frozen=True, slots=True)
class ExecutionUnit:
    """One deterministic, independently retried unit emitted by a planner."""

    unit_key: str
    handler: str
    payload: Mapping[str, JsonValue]
    execution_class: str
    timeout_seconds: int = 300
    max_attempts: int = 5
    idempotency_key: str | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("unit_key", self.unit_key),
            ("handler", self.handler),
            ("execution_class", self.execution_class),
        ):
            _require_contract_name(name, value)
        if type(self.timeout_seconds) is not int or not 1 <= self.timeout_seconds <= 3_600:
            raise ValueError("timeout_seconds must be between 1 and 3600")
        if type(self.max_attempts) is not int or self.max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        idempotency_key = self.unit_key if self.idempotency_key is None else self.idempotency_key
        _require_contract_name("idempotency_key", idempotency_key)
        object.__setattr__(self, "idempotency_key", idempotency_key)
        object.__setattr__(self, "payload", _immutable_payload(self.payload))

    @property
    def payload_sha256(self) -> str:
        """Return the stable digest used for durable planning drift checks."""
        return hashlib.sha256(_canonical_payload(self.payload)).hexdigest()

    def planning_record(self, ordinal: int) -> PlanningRecord:
        """Bind the unit's deterministic fields to one planner position."""
        return PlanningRecord(
            ordinal=ordinal,
            unit_key=self.unit_key,
            payload_sha256=self.payload_sha256,
        )

    def execution_id(self, request: WorkloadRequest) -> ExecutionId:
        """Derive stable identity from the pinned run/revision and unit key."""
        identity = "\0".join(
            (str(request.run_id), str(request.execution_revision), self.unit_key)
        ).encode()
        return ExecutionId(f"execution:{hashlib.sha256(identity).hexdigest()}")


@dataclass(frozen=True, slots=True, order=True)
class PlanningRecord:
    """Minimal durable prefix entry used to compare planner retries."""

    ordinal: int
    unit_key: str
    payload_sha256: str

    def __post_init__(self) -> None:
        if type(self.ordinal) is not int or self.ordinal < 0:
            raise ValueError("ordinal must be a non-negative integer")
        _require_contract_name("unit_key", self.unit_key)
        if len(self.payload_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.payload_sha256
        ):
            raise ValueError("payload_sha256 must be a lowercase SHA-256 digest")


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    """Stable identity and attempt metadata supplied to a finite handler."""

    run_id: RunId
    execution_id: ExecutionId
    attempt: int
    idempotency_key: str
    metadata: Mapping[str, str] = field(default_factory=dict)
    cancellation: CancellationToken = field(default_factory=lambda: CancellationSource().token)
    log_context: LogContext = field(default_factory=LogContext)
    deadline: Deadline | None = None

    def __post_init__(self) -> None:
        if type(self.attempt) is not int or self.attempt < 1:
            raise ValueError("attempt must be a positive integer")
        _require_contract_name("idempotency_key", self.idempotency_key)
        if any(
            not isinstance(key, str) or not key or not isinstance(value, str)
            for key, value in self.metadata.items()
        ):
            raise ValueError("metadata must contain non-empty string keys and values")
        object.__setattr__(
            self,
            "metadata",
            MappingProxyType(dict(self.metadata)),
        )


@runtime_checkable
class FinitePlanner(Protocol):
    """Product planner that lazily emits a finite async unit sequence."""

    name: str
    version: str
    mode: WorkloadMode

    def __call__(self, request: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
        """Plan deterministic units for the immutable request."""
        ...


@runtime_checkable
class FiniteHandler(Protocol):
    """Product handler for one execution attempt."""

    async def __call__(
        self,
        context: ExecutionContext,
        payload: Mapping[str, JsonValue],
    ) -> ExecutionResult:
        """Execute one unit using only runtime-provided context."""
        ...


async def validate_plan(
    request: WorkloadRequest,
    planner: FinitePlanner,
    *,
    expected: tuple[PlanningRecord, ...] | None = None,
    cancellation: CancellationToken | None = None,
) -> tuple[ExecutionUnit, ...]:
    """Materialize and validate a deterministic zero/one/many plan."""
    units: list[ExecutionUnit] = []
    records: list[PlanningRecord] = []
    seen: dict[str, str] = {}
    if cancellation is not None:
        cancellation.raise_if_cancelled()
    async for unit in planner(request):
        if cancellation is not None:
            cancellation.raise_if_cancelled()
        digest = unit.payload_sha256
        previous = seen.get(unit.unit_key)
        if previous is not None:
            raise PlanningDriftError(
                f"duplicate unit key: {unit.unit_key}",
                details={
                    "unit_key": unit.unit_key,
                    "first_sha256": previous,
                    "next_sha256": digest,
                },
            )
        seen[unit.unit_key] = digest
        units.append(unit)
        records.append(unit.planning_record(len(records)))
    if cancellation is not None:
        cancellation.raise_if_cancelled()
    if expected is not None and tuple(records) != expected:
        raise PlanningDriftError(
            "planned sequence differs from durable sequence",
            details={"expected_count": len(expected), "actual_count": len(records)},
        )
    return tuple(units)


def _require_contract_name(name: str, value: str) -> None:
    if _CONTRACT_NAME.fullmatch(value) is None:
        raise ValueError(f"{name} must be a non-empty runtime name")
