"""Revision-pinned finite planner runner (control plane).

Runs a registered planner against the immutable WorkloadRequest, persists
the plan, and enqueues unit-dispatch wakeups — the durable state commit and
the outbox rows share one transaction so a planned unit can never lack its
dispatch intent.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from distributed_runtime.core.enums import WorkloadMode
from distributed_runtime.core.envelope import VersionedEnvelope
from distributed_runtime.core.errors import InvariantViolationError
from distributed_runtime.core.identifiers import RunId
from distributed_runtime.finite import ExecutionUnit, WorkloadRequest, validate_plan
from distributed_runtime.registry import FiniteRegistration, RuntimeRegistry
from distributed_runtime.state.engine import StateEngine
from distributed_runtime.state.finite import (
    FiniteStateStore,
    StoredRun,
    _canonical_json,
    unit_to_document,
)
from distributed_runtime.state.outbox import insert_outbox


@dataclass(frozen=True, slots=True)
class PlanOutcome:
    run_id: RunId
    unit_count: int
    planning_generation: int


class PlannerRunner:
    """Execute planners with revisions pinned at submission time."""

    def __init__(
        self,
        *,
        registry: RuntimeRegistry,
        engine: StateEngine,
        dispatch_topic: str,
        application: str,
    ) -> None:
        self._registry = registry
        self._engine = engine
        self._store = FiniteStateStore(engine)
        self._dispatch_topic = dispatch_topic
        self._application = application

    async def plan(self, run_id: RunId) -> PlanOutcome:
        """Run the pinned planner, persist plan + dispatch intents atomically."""
        run = await self._store.get_run(run_id)
        if run is None:
            raise LookupError(f"unknown run: {run_id}")
        registration = self._registry.workload(
            run.workload_name, run.workload_version, WorkloadMode.FINITE
        )
        if not isinstance(registration, FiniteRegistration):
            raise InvariantViolationError(
                "workload is not finite",
                details={"workload": run.workload_name},
            )
        request = WorkloadRequest(
            run_id=run.run_id,
            workload_id=run.workload_id,
            planner_revision=run.planner_revision,
            execution_revision=run.execution_revision,
            payload=run.input,
        )
        generation = run.planning_generation
        expected = await self._store.load_plan(run_id, planning_generation=generation)
        units = await validate_plan(
            request,
            registration.planner,
            expected=expected if expected else None,
        )
        await self._persist(run, units, generation)
        return PlanOutcome(
            run_id=run_id,
            unit_count=len(units),
            planning_generation=generation,
        )

    def _dispatch_envelope(
        self, run: StoredRun, unit: ExecutionUnit, request: WorkloadRequest
    ) -> VersionedEnvelope:
        return VersionedEnvelope(
            schema_version=1,
            message_kind="unit.dispatch",
            run_id=str(run.run_id),
            execution_id=str(unit.execution_id(request)),
            idempotency_key=unit.idempotency_key or unit.unit_key,
            application=self._application,
            workload=run.workload_name,
            workload_version=run.workload_version,
            handler=unit.handler,
            attempt_generation=1,
            execution_class=unit.execution_class,
            runtime_pool_revision=str(run.execution_revision),
            published_at=datetime.now(UTC).isoformat(),
            payload={"unit_key": unit.unit_key},
        )

    async def _persist(
        self,
        run: StoredRun,
        units: Sequence[ExecutionUnit],
        generation: int,
    ) -> None:
        request = WorkloadRequest(
            run_id=run.run_id,
            workload_id=run.workload_id,
            planner_revision=run.planner_revision,
            execution_revision=run.execution_revision,
            payload=run.input,
        )
        async with self._engine.acquire() as connection, connection.transaction():
            for ordinal, unit in enumerate(units):
                await connection.execute(
                    """
                        INSERT INTO runtime_state.finite_plan_units
                            (run_id, planning_generation, ordinal, unit_key,
                             payload_hash, unit)
                        VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                        """,
                    str(run.run_id),
                    generation,
                    ordinal,
                    unit.unit_key,
                    unit.payload_sha256,
                    _canonical_json(unit_to_document(unit)),
                )
                await connection.execute(
                    """
                        INSERT INTO runtime_state.finite_units
                            (run_id, unit_key, status)
                        VALUES ($1, $2, 'ready')
                        ON CONFLICT (run_id, unit_key) DO NOTHING
                        """,
                    str(run.run_id),
                    unit.unit_key,
                )
                await insert_outbox(
                    connection,
                    kind="unit_dispatch",
                    dedup_key=f"{run.run_id}:{unit.unit_key}",
                    destination=self._dispatch_topic,
                    envelope=self._dispatch_envelope(run, unit, request),
                )
            changed = await connection.execute(
                """
                    UPDATE runtime_state.finite_runs
                    SET status = 'running', updated_at = now()
                    WHERE run_id = $1 AND status IN ('pending', 'planning')
                    """,
                str(run.run_id),
            )
        if changed == "UPDATE 0":
            raise InvariantViolationError(
                "run not plannable in current state",
                details={"run_id": str(run.run_id)},
            )
