"""Finite run/plan/unit/attempt repositories over runtime_state tables.

Implements the transaction-boundary rules from
``docs/design/state-schema.md`` §6 items 1-4. Every multi-row state change
happens in one transaction; queue ack is the caller's responsibility after
the commit returns.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

import asyncpg

from distributed_runtime.core.enums import ExecutionStatus, RunStatus
from distributed_runtime.core.envelope import JsonValue
from distributed_runtime.core.errors import InvariantViolationError
from distributed_runtime.core.identifiers import (
    ExecutionId,
    RevisionId,
    RunId,
    RuntimeInstanceId,
    WorkloadId,
)
from distributed_runtime.finite import ExecutionUnit, PlanningRecord
from distributed_runtime.state.engine import StateEngine


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def unit_to_document(unit: ExecutionUnit) -> dict[str, JsonValue]:
    """Serialize an ExecutionUnit for the finite_plan_units.unit column."""
    return {
        "unit_key": unit.unit_key,
        "handler": unit.handler,
        "payload": dict(unit.payload),
        "execution_class": unit.execution_class,
        "timeout_seconds": unit.timeout_seconds,
        "max_attempts": unit.max_attempts,
        "idempotency_key": unit.idempotency_key,
    }


def unit_from_document(document: Mapping[str, JsonValue]) -> ExecutionUnit:
    payload = document.get("payload")
    if not isinstance(payload, Mapping):
        raise InvariantViolationError("stored unit payload must be an object", details={})
    return ExecutionUnit(
        unit_key=str(document["unit_key"]),
        handler=str(document["handler"]),
        payload=payload,
        execution_class=str(document["execution_class"]),
        timeout_seconds=int(str(document["timeout_seconds"])),
        max_attempts=int(str(document["max_attempts"])),
        idempotency_key=(
            None if document.get("idempotency_key") is None else str(document["idempotency_key"])
        ),
    )


@dataclass(frozen=True, slots=True)
class StoredRun:
    run_id: RunId
    workload_id: WorkloadId
    workload_name: str
    workload_version: str
    planner_revision: RevisionId
    execution_revision: RevisionId
    status: RunStatus
    input: Mapping[str, JsonValue]
    planning_generation: int


@dataclass(frozen=True, slots=True)
class UnitState:
    run_id: RunId
    unit_key: str
    status: ExecutionStatus
    attempt_count: int
    claim_token: int
    claimed_by: RuntimeInstanceId | None
    result: JsonValue
    last_error: JsonValue


@dataclass(frozen=True, slots=True)
class ClaimedUnit:
    """A unit whose claim this instance owns until claim_expires_at."""

    run_id: RunId
    unit_key: str
    unit: ExecutionUnit
    attempt: int
    claim_token: int
    claim_expires_at: datetime


def _run_from_row(row: asyncpg.Record) -> StoredRun:
    return StoredRun(
        run_id=RunId(str(row["run_id"])),
        workload_id=WorkloadId(str(row["workload_id"])),
        workload_name=str(row["workload_name"]),
        workload_version=str(row["workload_version"]),
        planner_revision=RevisionId(str(row["planner_revision"])),
        execution_revision=RevisionId(str(row["execution_revision"])),
        status=RunStatus(str(row["status"])),
        input=json.loads(str(row["input"])),
        planning_generation=int(row["planning_generation"]),
    )


def _unit_state_from_row(row: asyncpg.Record) -> UnitState:
    claimed = row["claimed_by"]
    return UnitState(
        run_id=RunId(str(row["run_id"])),
        unit_key=str(row["unit_key"]),
        status=ExecutionStatus(str(row["status"])),
        attempt_count=int(row["attempt_count"]),
        claim_token=int(row["claim_token"]),
        claimed_by=None if claimed is None else RuntimeInstanceId(str(claimed)),
        result=json.loads(str(row["result"])) if row["result"] is not None else None,
        last_error=(json.loads(str(row["last_error"])) if row["last_error"] is not None else None),
    )


class FiniteStateStore:
    """Repository for finite run lifecycle and unit claiming."""

    def __init__(self, engine: StateEngine) -> None:
        self._engine = engine

    async def submit_run(
        self,
        *,
        run_id: RunId,
        workload_id: WorkloadId,
        workload_name: str,
        workload_version: str,
        planner_revision: RevisionId,
        execution_revision: RevisionId,
        input: Mapping[str, JsonValue],
    ) -> StoredRun:
        """Insert a pending run; resubmission returns the existing row."""
        async with self._engine.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                    INSERT INTO runtime_state.finite_runs
                        (run_id, workload_id, workload_name, workload_version,
                         planner_revision, execution_revision, status, input)
                    VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7::jsonb)
                    ON CONFLICT (run_id) DO NOTHING
                    """,
                str(run_id),
                str(workload_id),
                workload_name,
                workload_version,
                str(planner_revision),
                str(execution_revision),
                _canonical_json(dict(input)),
            )
            row = await connection.fetchrow(
                "SELECT * FROM runtime_state.finite_runs WHERE run_id = $1",
                str(run_id),
            )
        assert row is not None
        return _run_from_row(row)

    async def get_run(self, run_id: RunId) -> StoredRun | None:
        async with self._engine.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT * FROM runtime_state.finite_runs WHERE run_id = $1",
                str(run_id),
            )
        return None if row is None else _run_from_row(row)

    async def list_pending_runs(self, *, limit: int = 100) -> tuple[StoredRun, ...]:
        """Runs awaiting planning, in submission order (control-loop feed)."""
        async with self._engine.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT * FROM runtime_state.finite_runs
                WHERE status IN ('pending', 'planning')
                ORDER BY created_at, run_id
                LIMIT $1
                """,
                limit,
            )
        return tuple(_run_from_row(row) for row in rows)

    async def transition_run(self, run_id: RunId, status: RunStatus) -> None:
        """Apply a forward-only run status transition."""
        async with self._engine.acquire() as connection:
            changed = await connection.execute(
                """
                UPDATE runtime_state.finite_runs
                SET status = $2, updated_at = now(),
                    cancelled_at = CASE WHEN $2 = 'cancelled' THEN now()
                                        ELSE cancelled_at END
                WHERE run_id = $1
                """,
                str(run_id),
                status.value,
            )
        if changed == "UPDATE 0":
            raise LookupError(f"unknown run: {run_id}")

    async def record_plan(
        self,
        run_id: RunId,
        units: Sequence[ExecutionUnit],
        *,
        planning_generation: int = 1,
    ) -> None:
        """Persist the plan, materialize ready units, mark the run running.

        One transaction: plan rows + unit rows + run status (schema doc §6.2).
        """
        async with self._engine.acquire() as connection, connection.transaction():
            await connection.executemany(
                """
                    INSERT INTO runtime_state.finite_plan_units
                        (run_id, planning_generation, ordinal, unit_key,
                         payload_hash, unit)
                    VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                    """,
                [
                    (
                        str(run_id),
                        planning_generation,
                        ordinal,
                        unit.unit_key,
                        unit.payload_sha256,
                        _canonical_json(unit_to_document(unit)),
                    )
                    for ordinal, unit in enumerate(units)
                ],
            )
            await connection.executemany(
                """
                    INSERT INTO runtime_state.finite_units
                        (run_id, unit_key, status)
                    VALUES ($1, $2, 'ready')
                    ON CONFLICT (run_id, unit_key) DO NOTHING
                    """,
                [(str(run_id), unit.unit_key) for unit in units],
            )
            changed = await connection.execute(
                """
                    UPDATE runtime_state.finite_runs
                    SET status = 'running', planning_generation = $2,
                        updated_at = now()
                    WHERE run_id = $1 AND status IN ('pending', 'planning')
                    """,
                str(run_id),
                planning_generation,
            )
        if changed == "UPDATE 0":
            raise InvariantViolationError(
                "run not plannable in current state",
                details={"run_id": str(run_id)},
            )

    async def load_plan(
        self, run_id: RunId, *, planning_generation: int
    ) -> tuple[PlanningRecord, ...]:
        """Return the durable plan prefix for drift comparison."""
        async with self._engine.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT ordinal, unit_key, payload_hash
                FROM runtime_state.finite_plan_units
                WHERE run_id = $1 AND planning_generation = $2
                ORDER BY ordinal
                """,
                str(run_id),
                planning_generation,
            )
        return tuple(
            PlanningRecord(
                ordinal=int(row["ordinal"]),
                unit_key=str(row["unit_key"]),
                payload_sha256=str(row["payload_hash"]),
            )
            for row in rows
        )

    async def claim_units(
        self,
        owner: RuntimeInstanceId,
        *,
        limit: int = 1,
        claim_grace_seconds: int = 120,
        required_revision: RevisionId | None = None,
    ) -> tuple[ClaimedUnit, ...]:
        """Claim due units via SKIP LOCKED; returns units this owner holds.

        Claim expiry uses the database clock plus the unit's own
        timeout_seconds and the shared grace window.

        ``required_revision`` restricts claims to runs pinned to that
        execution revision — the poll path's counterpart to the Pub/Sub
        subscription filter, so a pool cannot steal another revision's
        work by polling the store directly.
        """
        async with self._engine.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                    WITH due AS (
                        SELECT u.run_id, u.unit_key,
                               (p.unit->>'timeout_seconds')::int
                                   AS timeout_seconds
                        FROM runtime_state.finite_units u
                        JOIN runtime_state.finite_plan_units p
                          ON p.run_id = u.run_id
                         AND p.unit_key = u.unit_key
                         AND p.planning_generation = (
                            SELECT max(planning_generation)
                            FROM runtime_state.finite_plan_units g
                            WHERE g.run_id = u.run_id
                              AND g.unit_key = u.unit_key)
                        JOIN runtime_state.finite_runs r
                          ON r.run_id = u.run_id
                        WHERE u.status IN ('ready', 'retry_scheduled')
                          AND (u.next_attempt_at IS NULL
                               OR u.next_attempt_at <= now())
                          AND (u.claim_expires_at IS NULL
                               OR u.claim_expires_at < now())
                          AND ($4::text IS NULL
                               OR r.execution_revision = $4)
                        ORDER BY u.next_attempt_at NULLS FIRST, u.unit_key
                        LIMIT $2
                        FOR UPDATE OF u SKIP LOCKED
                    )
                    UPDATE runtime_state.finite_units u
                    SET status = 'running',
                        claimed_by = $1,
                        claim_token = u.claim_token + 1,
                        claim_expires_at = now() + make_interval(
                            secs => due.timeout_seconds + $3),
                        attempt_count = u.attempt_count + 1,
                        next_attempt_at = NULL,
                        updated_at = now()
                    FROM due
                    WHERE u.run_id = due.run_id AND u.unit_key = due.unit_key
                    RETURNING u.run_id, u.unit_key, u.attempt_count,
                              u.claim_token, u.claim_expires_at
                    """,
                str(owner),
                limit,
                claim_grace_seconds,
                None if required_revision is None else str(required_revision),
            )
        claimed: list[ClaimedUnit] = []
        for row in rows:
            unit = await self._load_unit(RunId(str(row["run_id"])), str(row["unit_key"]))
            claimed.append(
                ClaimedUnit(
                    run_id=RunId(str(row["run_id"])),
                    unit_key=str(row["unit_key"]),
                    unit=unit,
                    attempt=int(row["attempt_count"]),
                    claim_token=int(row["claim_token"]),
                    claim_expires_at=row["claim_expires_at"],
                )
            )
        return tuple(claimed)

    async def claim_unit(
        self,
        owner: RuntimeInstanceId,
        run_id: RunId,
        unit_key: str,
        *,
        claim_grace_seconds: int = 120,
    ) -> ClaimedUnit | None:
        """Claim one specific due unit — the Pub/Sub wakeup path.

        Returns None when the unit is already claimed, done, or not yet due.
        """
        async with self._engine.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """
                    WITH due AS (
                        SELECT u.run_id, u.unit_key,
                               (p.unit->>'timeout_seconds')::int
                                   AS timeout_seconds
                        FROM runtime_state.finite_units u
                        JOIN runtime_state.finite_plan_units p
                          ON p.run_id = u.run_id
                         AND p.unit_key = u.unit_key
                         AND p.planning_generation = (
                            SELECT max(planning_generation)
                            FROM runtime_state.finite_plan_units g
                            WHERE g.run_id = u.run_id
                              AND g.unit_key = u.unit_key)
                        WHERE u.run_id = $2 AND u.unit_key = $3
                          AND u.status IN ('ready', 'retry_scheduled')
                          AND (u.next_attempt_at IS NULL
                               OR u.next_attempt_at <= now())
                          AND (u.claim_expires_at IS NULL
                               OR u.claim_expires_at < now())
                        FOR UPDATE OF u
                    )
                    UPDATE runtime_state.finite_units u
                    SET status = 'running',
                        claimed_by = $1,
                        claim_token = u.claim_token + 1,
                        claim_expires_at = now() + make_interval(
                            secs => due.timeout_seconds + $4),
                        attempt_count = u.attempt_count + 1,
                        next_attempt_at = NULL,
                        updated_at = now()
                    FROM due
                    WHERE u.run_id = due.run_id AND u.unit_key = due.unit_key
                    RETURNING u.attempt_count, u.claim_token, u.claim_expires_at
                    """,
                str(owner),
                str(run_id),
                unit_key,
                claim_grace_seconds,
            )
        if row is None:
            return None
        unit = await self._load_unit(run_id, unit_key)
        return ClaimedUnit(
            run_id=run_id,
            unit_key=unit_key,
            unit=unit,
            attempt=int(row["attempt_count"]),
            claim_token=int(row["claim_token"]),
            claim_expires_at=row["claim_expires_at"],
        )

    async def _load_unit(self, run_id: RunId, unit_key: str) -> ExecutionUnit:
        async with self._engine.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT unit FROM runtime_state.finite_plan_units
                WHERE run_id = $1 AND unit_key = $2
                ORDER BY planning_generation DESC
                LIMIT 1
                """,
                str(run_id),
                unit_key,
            )
        if row is None:
            raise LookupError(f"unit not in plan: {run_id}/{unit_key}")
        return unit_from_document(json.loads(str(row["unit"])))

    async def complete_attempt(
        self,
        claimed: ClaimedUnit,
        *,
        execution_id: ExecutionId,
        outcome: str,
        result: JsonValue = None,
        error: JsonValue = None,
        retry_at: datetime | None = None,
    ) -> None:
        """Record one attempt and the unit's next state in one transaction.

        The write is fenced by claim_token — a worker whose lease expired
        cannot overwrite state owned by a newer claim holder.
        """
        final_status = {
            "succeeded": "succeeded",
            "permanent": "failed",
            "retryable": "retry_scheduled",
            "rate_limited": "retry_scheduled",
            "cancelled": "cancelled",
        }.get(outcome)
        if final_status is None:
            raise ValueError(f"unknown outcome: {outcome}")
        async with self._engine.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                    INSERT INTO runtime_state.finite_attempts
                        (run_id, unit_key, attempt, execution_id,
                         finished_at, outcome, error)
                    VALUES ($1, $2, $3, $4, now(), $5, $6::jsonb)
                    ON CONFLICT (run_id, unit_key, attempt) DO NOTHING
                    """,
                str(claimed.run_id),
                claimed.unit_key,
                claimed.attempt,
                str(execution_id),
                outcome,
                None if error is None else _canonical_json(error),
            )
            changed = await connection.execute(
                """
                    UPDATE runtime_state.finite_units
                    SET status = $4,
                        result = COALESCE($5::jsonb, result),
                        last_error = COALESCE($6::jsonb, last_error),
                        next_attempt_at = $7,
                        claimed_by = NULL,
                        claim_expires_at = NULL,
                        updated_at = now()
                    WHERE run_id = $1 AND unit_key = $2 AND claim_token = $3
                    """,
                str(claimed.run_id),
                claimed.unit_key,
                claimed.claim_token,
                final_status,
                None if result is None else _canonical_json(result),
                None if error is None else _canonical_json(error),
                retry_at,
            )
            if changed == "UPDATE 0":
                raise InvariantViolationError(
                    "stale claim token: unit reclaimed by another worker",
                    details={
                        "run_id": str(claimed.run_id),
                        "unit_key": claimed.unit_key,
                        "claim_token": claimed.claim_token,
                    },
                )

    async def dead_letter(self, claimed: ClaimedUnit) -> None:
        """Move a unit to dead_lettered, fenced by the caller's claim token."""
        async with self._engine.acquire() as connection:
            changed = await connection.execute(
                """
                UPDATE runtime_state.finite_units
                SET status = 'dead_lettered', updated_at = now()
                WHERE run_id = $1 AND unit_key = $2 AND claim_token = $3
                """,
                str(claimed.run_id),
                claimed.unit_key,
                claimed.claim_token,
            )
        if changed == "UPDATE 0":
            raise InvariantViolationError(
                "stale claim token: cannot dead-letter",
                details={"run_id": str(claimed.run_id), "unit_key": claimed.unit_key},
            )

    async def unit_states(self, run_id: RunId) -> tuple[UnitState, ...]:
        async with self._engine.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT * FROM runtime_state.finite_units
                WHERE run_id = $1 ORDER BY unit_key
                """,
                str(run_id),
            )
        return tuple(_unit_state_from_row(row) for row in rows)

    async def results(self, run_id: RunId) -> tuple[JsonValue, ...]:
        """Return unit results in unit_key order for product aggregation."""
        async with self._engine.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT result FROM runtime_state.finite_units
                WHERE run_id = $1 AND status = 'succeeded'
                ORDER BY unit_key
                """,
                str(run_id),
            )
        return tuple(json.loads(str(row["result"])) for row in rows)

    async def finalize_run_if_done(self, run_id: RunId) -> RunStatus:
        """Mark the run succeeded/failed once every unit reached a final state."""
        async with self._engine.acquire() as connection, connection.transaction():
            counts = await connection.fetchrow(
                """
                    SELECT
                        count(*) FILTER (WHERE status NOT IN
                            ('succeeded', 'dead_lettered', 'cancelled')) AS open,
                        count(*) FILTER (WHERE status = 'dead_lettered') AS dead
                    FROM runtime_state.finite_units
                    WHERE run_id = $1
                    """,
                str(run_id),
            )
            assert counts is not None
            if int(counts["open"]) > 0:
                row = await connection.fetchrow(
                    "SELECT status FROM runtime_state.finite_runs WHERE run_id = $1",
                    str(run_id),
                )
                return RunStatus(str(row["status"])) if row else RunStatus.PENDING
            new_status = RunStatus.FAILED if int(counts["dead"]) > 0 else RunStatus.SUCCEEDED
            await connection.execute(
                """
                    UPDATE runtime_state.finite_runs
                    SET status = $2, updated_at = now()
                    WHERE run_id = $1 AND status = 'running'
                    """,
                str(run_id),
                new_status.value,
            )
            row = await connection.fetchrow(
                "SELECT status FROM runtime_state.finite_runs WHERE run_id = $1",
                str(run_id),
            )
            assert row is not None
            return RunStatus(str(row["status"]))
