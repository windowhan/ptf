"""Worker instance registry: capabilities and heartbeats (state-schema §5.3)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

import asyncpg

from distributed_runtime.core.identifiers import RevisionId, RuntimeInstanceId
from distributed_runtime.state.engine import StateEngine


@dataclass(frozen=True, slots=True)
class WorkerInstance:
    instance_id: RuntimeInstanceId
    execution_classes: tuple[str, ...]
    revision: RevisionId
    last_heartbeat_at: datetime
    status: str


def _worker_from_row(row: asyncpg.Record) -> WorkerInstance:
    return WorkerInstance(
        instance_id=RuntimeInstanceId(str(row["instance_id"])),
        execution_classes=tuple(str(c) for c in json.loads(str(row["execution_classes"]))),
        revision=RevisionId(str(row["revision"])),
        last_heartbeat_at=row["last_heartbeat_at"],
        status=str(row["status"]),
    )


class WorkerRegistry:
    """Track live workers, their classes, and heartbeat freshness."""

    def __init__(self, engine: StateEngine) -> None:
        self._engine = engine

    async def register(
        self,
        instance_id: RuntimeInstanceId,
        *,
        execution_classes: Sequence[str],
        revision: RevisionId,
    ) -> WorkerInstance:
        async with self._engine.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO runtime_state.worker_instances
                    (instance_id, execution_classes, revision, status,
                     last_heartbeat_at)
                VALUES ($1, $2::jsonb, $3, 'active', now())
                ON CONFLICT (instance_id) DO UPDATE
                    SET execution_classes = EXCLUDED.execution_classes,
                        revision = EXCLUDED.revision,
                        status = 'active',
                        last_heartbeat_at = now()
                """,
                str(instance_id),
                json.dumps(list(execution_classes)),
                str(revision),
            )
        worker = await self.get(instance_id)
        assert worker is not None
        return worker

    async def heartbeat(
        self,
        instance_id: RuntimeInstanceId,
        *,
        revision: RevisionId | None = None,
    ) -> bool:
        """Refresh liveness; returns False for unknown instances."""
        async with self._engine.acquire() as connection:
            if revision is None:
                changed = await connection.execute(
                    """
                    UPDATE runtime_state.worker_instances
                    SET last_heartbeat_at = now()
                    WHERE instance_id = $1
                    """,
                    str(instance_id),
                )
            else:
                changed = await connection.execute(
                    """
                    UPDATE runtime_state.worker_instances
                    SET last_heartbeat_at = now(), revision = $2
                    WHERE instance_id = $1
                    """,
                    str(instance_id),
                    str(revision),
                )
        return changed == "UPDATE 1"

    async def mark_status(self, instance_id: RuntimeInstanceId, status: str) -> None:
        if status not in {"active", "draining", "dead"}:
            raise ValueError(f"invalid worker status: {status}")
        async with self._engine.acquire() as connection:
            changed = await connection.execute(
                """
                UPDATE runtime_state.worker_instances
                SET status = $2 WHERE instance_id = $1
                """,
                str(instance_id),
                status,
            )
        if changed == "UPDATE 0":
            raise LookupError(f"unknown worker instance: {instance_id}")

    async def mark_stale_dead(self, *, stale_after_seconds: int) -> int:
        """Mark workers whose heartbeat is older than the threshold dead."""
        async with self._engine.acquire() as connection:
            changed = await connection.execute(
                """
                UPDATE runtime_state.worker_instances
                SET status = 'dead'
                WHERE status <> 'dead'
                  AND last_heartbeat_at < now() - make_interval(secs => $1)
                """,
                stale_after_seconds,
            )
        return int(changed.removeprefix("UPDATE "))

    async def get(self, instance_id: RuntimeInstanceId) -> WorkerInstance | None:
        async with self._engine.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT * FROM runtime_state.worker_instances
                WHERE instance_id = $1
                """,
                str(instance_id),
            )
        return None if row is None else _worker_from_row(row)

    async def list_active(self) -> tuple[WorkerInstance, ...]:
        async with self._engine.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT * FROM runtime_state.worker_instances
                WHERE status = 'active' ORDER BY instance_id
                """
            )
        return tuple(_worker_from_row(row) for row in rows)
