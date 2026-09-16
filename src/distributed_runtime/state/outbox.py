"""At-least-once outbox over runtime_state.outbox_messages.

Publishers insert rows inside the caller's state-change transaction (the
outbox rule). A dispatcher drains ``pending`` rows, publishes via a supplied
sender, then marks them published — also in one transaction so a crash
between publish and mark just re-publishes (at-least-once, deduped by key).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

import asyncpg

from distributed_runtime.core.envelope import VersionedEnvelope
from distributed_runtime.state.engine import StateEngine


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    outbox_id: int
    kind: str
    dedup_key: str
    destination: str
    envelope: VersionedEnvelope
    fencing_token: int | None
    status: str
    attempts: int
    created_at: datetime


def _row_to_message(row: asyncpg.Record) -> OutboxMessage:
    fencing = row["fencing_token"]
    return OutboxMessage(
        outbox_id=int(row["outbox_id"]),
        kind=str(row["kind"]),
        dedup_key=str(row["dedup_key"]),
        destination=str(row["destination"]),
        envelope=VersionedEnvelope.from_json(str(row["envelope"])),
        fencing_token=None if fencing is None else int(fencing),
        status=str(row["status"]),
        attempts=int(row["attempts"]),
        created_at=row["created_at"],
    )


async def insert_outbox(
    connection: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,
    *,
    kind: str,
    dedup_key: str,
    destination: str,
    envelope: VersionedEnvelope,
    fencing_token: int | None = None,
) -> int:
    """Insert one pending outbox row inside the caller's transaction.

    Returns the outbox_id. Duplicate (kind, dedup_key) returns the existing
    row's id — replayed work converges on one message.
    """
    row = await connection.fetchrow(
        """
        INSERT INTO runtime_state.outbox_messages
            (kind, dedup_key, destination, envelope, fencing_token)
        VALUES ($1, $2, $3, $4::jsonb, $5)
        ON CONFLICT (kind, dedup_key) DO UPDATE
            SET fencing_token = GREATEST(
                outbox_messages.fencing_token, EXCLUDED.fencing_token)
        RETURNING outbox_id
        """,
        kind,
        dedup_key,
        destination,
        envelope.to_json().decode(),
        fencing_token,
    )
    assert row is not None
    return int(row["outbox_id"])


class OutboxStore:
    """Read/drain pending outbox rows."""

    def __init__(self, engine: StateEngine) -> None:
        self._engine = engine

    async def pending(self, *, limit: int = 100) -> tuple[OutboxMessage, ...]:
        async with self._engine.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT * FROM runtime_state.outbox_messages
                WHERE status = 'pending'
                ORDER BY outbox_id
                LIMIT $1
                """,
                limit,
            )
        return tuple(_row_to_message(row) for row in rows)

    async def mark_published(self, outbox_id: int) -> None:
        async with self._engine.acquire() as connection:
            changed = await connection.execute(
                """
                UPDATE runtime_state.outbox_messages
                SET status = 'published', published_at = now()
                WHERE outbox_id = $1
                """,
                outbox_id,
            )
        if changed == "UPDATE 0":
            raise LookupError(f"unknown outbox message: {outbox_id}")

    async def mark_failed(self, outbox_id: int) -> None:
        async with self._engine.acquire() as connection:
            await connection.execute(
                """
                UPDATE runtime_state.outbox_messages
                SET status = 'failed', attempts = attempts + 1
                WHERE outbox_id = $1
                """,
                outbox_id,
            )

    async def dispatch(
        self,
        sender: Callable[[OutboxMessage], Awaitable[None]],
        *,
        limit: int = 100,
    ) -> int:
        """Publish pending rows through ``sender``; mark on success.

        A sender failure marks the row failed and continues with the next —
        one poisoned message must not starve the queue.
        """
        sent = 0
        for message in await self.pending(limit=limit):
            try:
                await sender(message)
            except Exception:
                await self.mark_failed(message.outbox_id)
                continue
            await self.mark_published(message.outbox_id)
            sent += 1
        return sent
