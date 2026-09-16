"""Deterministic in-memory test kit for finite workloads."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from distributed_runtime.core.config import FinitePolicy
from distributed_runtime.core.enums import ExecutionStatus, RunStatus, WorkloadMode
from distributed_runtime.core.envelope import JsonValue
from distributed_runtime.core.errors import (
    CancelledExecutionError,
    PermanentExecutionError,
    RateLimitedExecutionError,
    RetryableExecutionError,
    RuntimeContractError,
)
from distributed_runtime.core.identifiers import (
    ExecutionId,
    RevisionId,
    RunId,
    WorkloadId,
)
from distributed_runtime.core.lifecycle import (
    CancellationError,
    CancellationSource,
    Deadline,
    FakeClock,
)
from distributed_runtime.finite import (
    ExecutionContext,
    ExecutionResult,
    ExecutionUnit,
    PlanningRecord,
    WorkloadRequest,
    validate_plan,
)
from distributed_runtime.registry import FiniteRegistration, RuntimeRegistry

_CLAIMABLE = (ExecutionStatus.READY, ExecutionStatus.RETRY_SCHEDULED)
_TERMINAL = (
    ExecutionStatus.SUCCEEDED,
    ExecutionStatus.FAILED,
    ExecutionStatus.DEAD_LETTERED,
    ExecutionStatus.CANCELLED,
)


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    """One recorded handler invocation or simulated duplicate delivery."""

    run_id: RunId
    execution_id: ExecutionId
    unit_key: str
    attempt: int
    status: ExecutionStatus
    result: ExecutionResult | None = None
    error: Mapping[str, object] | None = None
    duplicate: bool = False

    def __post_init__(self) -> None:
        if type(self.attempt) is not int or self.attempt < 1:
            raise ValueError("attempt must be a positive integer")
        if self.error is not None:
            object.__setattr__(self, "error", MappingProxyType(dict(self.error)))


@dataclass(frozen=True, slots=True)
class RunRecord:
    """Aggregated local view of one submitted run."""

    run_id: RunId
    status: RunStatus
    results: tuple[ExecutionResult, ...]
    attempts: tuple[AttemptRecord, ...]


@dataclass(slots=True)
class _UnitState:
    unit: ExecutionUnit
    execution_id: ExecutionId
    status: ExecutionStatus = ExecutionStatus.READY
    attempts: int = 0
    next_attempt_at: float = 0.0
    result: ExecutionResult | None = None


@dataclass(slots=True)
class _RunState:
    request: WorkloadRequest
    registration: FiniteRegistration
    records: tuple[PlanningRecord, ...]
    cancellation: CancellationSource
    units: list[_UnitState]
    attempts: list[AttemptRecord] = field(default_factory=list)


class FiniteRuntimeTestKit:
    """Drive registered finite workloads through explicit deterministic steps.

    The kit never advances time or delivers messages on its own. Tests call
    :meth:`step`/:meth:`run_pending` for execution and ``kit.clock.advance()``
    for time. See ``docs/design/local-testing-kits.md`` for the contract.
    """

    def __init__(
        self,
        *,
        registry: RuntimeRegistry,
        clock: FakeClock | None = None,
        policy: FinitePolicy | None = None,
    ) -> None:
        self._registry = registry
        self.clock = FakeClock() if clock is None else clock
        self._policy = FinitePolicy() if policy is None else policy
        self._runs: dict[RunId, _RunState] = {}
        self._run_counter = 0

    async def submit(
        self,
        *,
        workload: str,
        version: str,
        payload: Mapping[str, JsonValue],
        planner_revision: str | None = None,
        execution_revision: str | None = None,
    ) -> RunId:
        """Plan one run locally and queue its units for explicit execution."""
        registration = self._registry.workload(workload, version, WorkloadMode.FINITE)
        if not isinstance(registration, FiniteRegistration):
            raise LookupError(f"workload is not finite: {workload}@{version}")
        self._run_counter += 1
        request = WorkloadRequest(
            run_id=RunId(f"run:local-{self._run_counter}"),
            workload_id=WorkloadId(str(registration.name)),
            planner_revision=RevisionId(planner_revision or registration.semantic_version),
            execution_revision=RevisionId(execution_revision or registration.semantic_version),
            payload=payload,
        )
        cancellation = CancellationSource(clock=self.clock)
        units = await validate_plan(
            request,
            registration.planner,
            cancellation=cancellation.token,
        )
        self._runs[request.run_id] = _RunState(
            request=request,
            registration=registration,
            records=tuple(unit.planning_record(index) for index, unit in enumerate(units)),
            cancellation=cancellation,
            units=[
                _UnitState(unit=unit, execution_id=unit.execution_id(request)) for unit in units
            ],
        )
        return request.run_id

    async def step(self, run_id: RunId | None = None) -> AttemptRecord | None:
        """Run the next due unit attempt in plan order; return its record."""
        now = self.clock.monotonic()
        for state in self._select_runs(run_id):
            for unit in state.units:
                if unit.status in _CLAIMABLE and unit.next_attempt_at <= now:
                    return await self._invoke(state, unit, duplicate=False)
        return None

    async def run_pending(self, run_id: RunId | None = None) -> tuple[AttemptRecord, ...]:
        """Run attempts until no unit is currently deliverable."""
        records: list[AttemptRecord] = []
        while (record := await self.step(run_id)) is not None:
            records.append(record)
        return tuple(records)

    async def redeliver(self, unit_key: str, run_id: RunId | None = None) -> AttemptRecord:
        """Simulate an at-least-once duplicate delivery of a completed unit.

        The handler is invoked again and the call is recorded with
        ``duplicate=True``. Preventing duplicate side effects is the product
        handler's responsibility via ``idempotency_key``.
        """
        matches = [
            (state, unit)
            for state in self._select_runs(run_id)
            for unit in state.units
            if unit.unit.unit_key == unit_key
        ]
        if len(matches) != 1:
            raise LookupError(f"expected exactly one unit {unit_key!r}, found {len(matches)}")
        state, unit = matches[0]
        if unit.status not in _TERMINAL:
            raise LookupError(f"unit {unit_key!r} has not completed")
        return await self._invoke(state, unit, duplicate=True)

    def cancel(self, run_id: RunId, reason: str = "cancelled") -> None:
        """Cancel a run and mark its unfinished units cancelled."""
        state = self._state(run_id)
        state.cancellation.cancel(reason)
        for unit in state.units:
            if unit.status not in _TERMINAL:
                unit.status = ExecutionStatus.CANCELLED

    async def replan(self, run_id: RunId) -> tuple[ExecutionUnit, ...]:
        """Re-run the planner and compare against the durable record prefix."""
        state = self._state(run_id)
        return await validate_plan(
            state.request,
            state.registration.planner,
            expected=state.records,
            cancellation=state.cancellation.token,
        )

    def run(self, run_id: RunId) -> RunRecord:
        """Return the aggregated local view of one run."""
        state = self._state(run_id)
        statuses = {unit.status for unit in state.units}
        if not state.units or statuses == {ExecutionStatus.SUCCEEDED}:
            status = RunStatus.SUCCEEDED
        elif statuses & {ExecutionStatus.FAILED, ExecutionStatus.DEAD_LETTERED}:
            status = RunStatus.FAILED
        elif state.cancellation.is_cancelled or ExecutionStatus.CANCELLED in statuses:
            status = RunStatus.CANCELLED
        else:
            status = RunStatus.RUNNING
        return RunRecord(
            run_id=run_id,
            status=status,
            results=self.results(run_id),
            attempts=tuple(state.attempts),
        )

    def results(self, run_id: RunId) -> tuple[ExecutionResult, ...]:
        """Return succeeded unit results in plan order for product aggregation."""
        state = self._state(run_id)
        return tuple(
            unit.result for unit in state.units if unit.status is ExecutionStatus.SUCCEEDED
        )

    def attempts(self, run_id: RunId | None = None) -> tuple[AttemptRecord, ...]:
        """Return recorded attempts, optionally limited to one run."""
        return tuple(record for state in self._select_runs(run_id) for record in state.attempts)

    def _select_runs(self, run_id: RunId | None) -> list[_RunState]:
        if run_id is not None:
            return [self._state(run_id)]
        return list(self._runs.values())

    def _state(self, run_id: RunId) -> _RunState:
        try:
            return self._runs[run_id]
        except KeyError as error:
            raise LookupError(f"unknown run: {run_id}") from error

    async def _invoke(
        self,
        state: _RunState,
        unit: _UnitState,
        *,
        duplicate: bool,
    ) -> AttemptRecord:
        unit.attempts += 1
        attempt = unit.attempts
        cancellation = state.cancellation.child()
        context = ExecutionContext(
            run_id=state.request.run_id,
            execution_id=unit.execution_id,
            attempt=attempt,
            idempotency_key=unit.unit.idempotency_key or unit.unit.unit_key,
            cancellation=cancellation.token,
            deadline=Deadline.after(self.clock, unit.unit.timeout_seconds),
        )
        if not duplicate:
            unit.status = ExecutionStatus.RUNNING
        try:
            context.cancellation.raise_if_cancelled()
            result = await state.registration.handler(context, unit.unit.payload)
        except (CancelledExecutionError, CancellationError) as error:
            return self._finish(state, unit, attempt, ExecutionStatus.CANCELLED, error, duplicate)
        except RateLimitedExecutionError as error:
            retry_at = self.clock.monotonic() + error.retry_after.total_seconds()
            return self._retry(state, unit, attempt, retry_at, error, duplicate)
        except RetryableExecutionError as error:
            retry_at = self.clock.monotonic() + self._retry_delay(attempt)
            return self._retry(state, unit, attempt, retry_at, error, duplicate)
        except PermanentExecutionError as error:
            return self._finish(state, unit, attempt, ExecutionStatus.FAILED, error, duplicate)
        except Exception as error:
            retry_at = self.clock.monotonic() + self._retry_delay(attempt)
            return self._retry(state, unit, attempt, retry_at, error, duplicate)
        if not duplicate:
            unit.status = ExecutionStatus.SUCCEEDED
            unit.result = result
        return self._record(state, unit, attempt, ExecutionStatus.SUCCEEDED, duplicate, result)

    def _retry(
        self,
        state: _RunState,
        unit: _UnitState,
        attempt: int,
        retry_at: float,
        error: BaseException,
        duplicate: bool,
    ) -> AttemptRecord:
        if attempt >= unit.unit.max_attempts:
            return self._finish(
                state, unit, attempt, ExecutionStatus.DEAD_LETTERED, error, duplicate
            )
        if not duplicate:
            unit.status = ExecutionStatus.RETRY_SCHEDULED
            unit.next_attempt_at = retry_at
        return self._record(
            state, unit, attempt, ExecutionStatus.RETRY_SCHEDULED, duplicate, error=error
        )

    def _finish(
        self,
        state: _RunState,
        unit: _UnitState,
        attempt: int,
        status: ExecutionStatus,
        error: BaseException,
        duplicate: bool,
    ) -> AttemptRecord:
        if not duplicate:
            unit.status = status
        return self._record(state, unit, attempt, status, duplicate, error=error)

    def _retry_delay(self, attempt: int) -> float:
        return min(
            self._policy.retry_base_seconds * 2.0 ** (attempt - 1),
            float(self._policy.retry_cap_seconds),
        )

    def _record(
        self,
        state: _RunState,
        unit: _UnitState,
        attempt: int,
        status: ExecutionStatus,
        duplicate: bool,
        result: ExecutionResult | None = None,
        error: BaseException | None = None,
    ) -> AttemptRecord:
        record = AttemptRecord(
            run_id=state.request.run_id,
            execution_id=unit.execution_id,
            unit_key=unit.unit.unit_key,
            attempt=attempt,
            status=status,
            result=result,
            error=None if error is None else _error_details(error),
            duplicate=duplicate,
        )
        state.attempts.append(record)
        return record


def _error_details(error: BaseException) -> Mapping[str, object]:
    if isinstance(error, RuntimeContractError):
        return error.as_dict()
    return {"type": type(error).__name__, "message": str(error)}
