"""Outbox dispatcher: move pending outbox rows to their transport.

The dispatcher is transport-agnostic — it calls a sender callback per
message (Pub/Sub publish on GCP, anything in tests). Retried units become
claimable purely by next_attempt_at passing; the dispatcher additionally
re-enqueues dispatch wakeups for due retry_scheduled units so workers get
notified without polling.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from distributed_runtime.core.envelope import VersionedEnvelope
from distributed_runtime.state.engine import StateEngine
from distributed_runtime.state.outbox import OutboxMessage, OutboxStore, insert_outbox

Sender = Callable[[OutboxMessage], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class DispatchCycle:
    published: int
    retries_enqueued: int


class OutboxDispatcher:
    """Drain the outbox and re-notify workers about due retries."""

    def __init__(
        self,
        engine: StateEngine,
        *,
        application: str,
        runtime_pool_revision: str,
    ) -> None:
        self._engine = engine
        self._outbox = OutboxStore(engine)
        self._application = application
        self._pool_revision = runtime_pool_revision

    async def cycle(self, sender: Sender, *, limit: int = 100) -> DispatchCycle:
        """One dispatch pass: enqueue due retries, publish pending rows."""
        enqueued = await self._enqueue_due_retries()
        published = await self._outbox.dispatch(sender, limit=limit)
        return DispatchCycle(published=published, retries_enqueued=enqueued)

    async def _enqueue_due_retries(self) -> int:
        """Insert dispatch wakeups for retry_scheduled units that are due."""
        async with self._engine.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                    SELECT u.run_id, u.unit_key, u.attempt_count,
                           p.unit->>'handler' AS handler,
                           p.unit->>'execution_class' AS execution_class,
                           r.workload_name, r.workload_version
                    FROM runtime_state.finite_units u
                    JOIN runtime_state.finite_plan_units p
                      ON p.run_id = u.run_id AND p.unit_key = u.unit_key
                     AND p.planning_generation = (
                        SELECT max(planning_generation)
                        FROM runtime_state.finite_plan_units g
                        WHERE g.run_id = u.run_id AND g.unit_key = u.unit_key)
                    JOIN runtime_state.finite_runs r ON r.run_id = u.run_id
                    WHERE u.status = 'retry_scheduled'
                      AND u.next_attempt_at <= now()
                    ORDER BY u.next_attempt_at
                    LIMIT 500
                    """
            )
            for row in rows:
                envelope = VersionedEnvelope(
                    schema_version=1,
                    message_kind="unit.dispatch",
                    run_id=str(row["run_id"]),
                    execution_id=f"{row['run_id']}:{row['unit_key']}",
                    idempotency_key=str(row["unit_key"]),
                    application=self._application,
                    workload=str(row["workload_name"]),
                    workload_version=str(row["workload_version"]),
                    handler=str(row["handler"]),
                    attempt_generation=int(row["attempt_count"]) + 1,
                    execution_class=str(row["execution_class"]),
                    runtime_pool_revision=self._pool_revision,
                    published_at=datetime.now(UTC).isoformat(),
                    payload={"unit_key": str(row["unit_key"])},
                )
                await insert_outbox(
                    connection,
                    kind="unit_dispatch",
                    dedup_key=(f"{row['run_id']}:{row['unit_key']}:retry:{row['attempt_count']}"),
                    destination="runtime-units",
                    envelope=envelope,
                )
        return len(rows)
