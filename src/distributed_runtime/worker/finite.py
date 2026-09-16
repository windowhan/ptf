"""Finite worker: claim due units, run handlers, record fenced outcomes.

Ack discipline (extension-boundaries): the caller acks the Pub/Sub message
only after :meth:`handle_envelope` returns — by then the attempt result is
durable in the state store. A claim miss still acks: the wakeup did its job
even when another worker holds the unit.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from distributed_runtime.core.config import FinitePolicy
from distributed_runtime.core.enums import WorkloadMode
from distributed_runtime.core.envelope import JsonValue, VersionedEnvelope
from distributed_runtime.core.identifiers import (
    ExecutionId,
    RunId,
    RuntimeInstanceId,
)
from distributed_runtime.core.lifecycle import CancellationSource, Deadline, SystemClock
from distributed_runtime.finite import (
    ExecutionContext,
    ExecutionResult,
    WorkloadRequest,
)
from distributed_runtime.registry import FiniteRegistration, RuntimeRegistry
from distributed_runtime.state.engine import StateEngine
from distributed_runtime.state.finite import ClaimedUnit, FiniteStateStore
from distributed_runtime.worker.retry import (
    AttemptOutcome,
    classify_error,
    retries_exhausted,
)


@dataclass(frozen=True, slots=True)
class HandledUnit:
    run_id: RunId
    unit_key: str
    outcome: str


class FiniteWorker:
    """Execute claimed finite units through registered handlers."""

    def __init__(
        self,
        *,
        registry: RuntimeRegistry,
        engine: StateEngine,
        instance_id: RuntimeInstanceId,
        policy: FinitePolicy | None = None,
        application: str = "",
    ) -> None:
        self._registry = registry
        self._store = FiniteStateStore(engine)
        self._instance_id = instance_id
        self._policy = FinitePolicy() if policy is None else policy
        self._application = application or registry.application

    async def handle_envelope(self, envelope: VersionedEnvelope) -> HandledUnit | None:
        """Claim the named unit and run one attempt. None = nothing claimed."""
        run_id = RunId(envelope.run_id)
        payload = envelope.payload or {}
        unit_key = str(payload.get("unit_key", ""))
        if not unit_key:
            return None
        claimed = await self._store.claim_unit(
            self._instance_id,
            run_id,
            unit_key,
            claim_grace_seconds=self._policy.claim_grace_seconds,
        )
        if claimed is None:
            return None
        return await self.execute_claimed(claimed)

    async def poll_once(self, *, limit: int = 1) -> tuple[HandledUnit, ...]:
        """Claim and execute up to ``limit`` due units — no Pub/Sub needed."""
        claimed = await self._store.claim_units(
            self._instance_id,
            limit=limit,
            claim_grace_seconds=self._policy.claim_grace_seconds,
        )
        results: list[HandledUnit] = []
        for unit in claimed:
            results.append(await self.execute_claimed(unit))
        return tuple(results)

    async def execute_claimed(self, claimed: ClaimedUnit) -> HandledUnit:
        """Run the handler for one claimed unit and commit the outcome."""
        run = await self._store.get_run(claimed.run_id)
        assert run is not None
        registration = self._registry.workload(
            run.workload_name, run.workload_version, WorkloadMode.FINITE
        )
        assert isinstance(registration, FiniteRegistration)

        request = WorkloadRequest(
            run_id=run.run_id,
            workload_id=run.workload_id,
            planner_revision=run.planner_revision,
            execution_revision=run.execution_revision,
            payload=run.input,
        )
        execution_id = claimed.unit.execution_id(request)
        cancellation = CancellationSource()
        context = ExecutionContext(
            run_id=claimed.run_id,
            execution_id=execution_id,
            attempt=claimed.attempt,
            idempotency_key=claimed.unit.idempotency_key or claimed.unit_key,
            cancellation=cancellation.token,
            deadline=Deadline.after(SystemClock(), claimed.unit.timeout_seconds),
        )
        try:
            result = await asyncio.wait_for(
                registration.handler(context, claimed.unit.payload),
                timeout=claimed.unit.timeout_seconds,
            )
        except BaseException as exc:
            outcome = classify_error(exc, claimed.attempt, self._policy)
            await self._record_failure(claimed, execution_id, outcome)
            return HandledUnit(
                run_id=claimed.run_id,
                unit_key=claimed.unit_key,
                outcome=outcome.outcome,
            )
        await self._store.complete_attempt(
            claimed,
            execution_id=execution_id,
            outcome="succeeded",
            result=_result_to_json(result),
        )
        await self._store.finalize_run_if_done(claimed.run_id)
        return HandledUnit(run_id=claimed.run_id, unit_key=claimed.unit_key, outcome="succeeded")

    async def _record_failure(
        self,
        claimed: ClaimedUnit,
        execution_id: ExecutionId,
        outcome: AttemptOutcome,
    ) -> None:
        exhausted = retries_exhausted(claimed.unit.max_attempts, claimed.attempt)
        if exhausted and outcome.outcome in {"retryable", "rate_limited"}:
            final = "permanent"
            retry_at = None
        else:
            final = outcome.outcome
            retry_at = (
                None if outcome.retry_delay is None else datetime.now(UTC) + outcome.retry_delay
            )
        await self._store.complete_attempt(
            claimed,
            execution_id=execution_id,
            outcome=final,
            error=outcome.error,
            retry_at=retry_at,
        )
        if final == "permanent":
            await self._store.dead_letter(claimed)
        await self._store.finalize_run_if_done(claimed.run_id)


def _result_to_json(result: ExecutionResult) -> JsonValue:
    from distributed_runtime.core.artifacts import ArtifactReference

    if isinstance(result, ArtifactReference):
        return cast(JsonValue, result.to_payload())
    return result
