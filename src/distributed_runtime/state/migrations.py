"""Ordered SQL migration runner for the runtime_state schema.

Migrations live in ``state/migrations/`` as ``NNNN_name.sql`` files applied
strictly in filename order. Applied versions are recorded in
``runtime_state.schema_migrations`` with a checksum; a changed checksum on an
already-applied version is a hard failure — migrations are immutable once run.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from importlib import resources

from distributed_runtime.core.errors import InvariantViolationError
from distributed_runtime.state.engine import StateEngine

_MIGRATION_RE = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")
_PACKAGE = "distributed_runtime.state"
_MIGRATIONS_DIR = "migrations"

_BASE_DDL = """
CREATE SCHEMA IF NOT EXISTS runtime_state;

CREATE TABLE IF NOT EXISTS runtime_state.schema_migrations (
    version     integer PRIMARY KEY,
    name        text NOT NULL,
    checksum    text NOT NULL,
    applied_at  timestamptz NOT NULL DEFAULT now()
);
"""


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    sql: str
    checksum: str


def available_migrations() -> tuple[Migration, ...]:
    """Return packaged migrations sorted by version."""
    migrations: list[Migration] = []
    for entry in (resources.files(_PACKAGE) / _MIGRATIONS_DIR).iterdir():
        match = _MIGRATION_RE.match(entry.name)
        if match is None:
            continue
        sql = entry.read_text(encoding="utf-8")
        migrations.append(
            Migration(
                version=int(match.group(1)),
                name=match.group(2),
                sql=sql,
                checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            )
        )
    migrations.sort(key=lambda m: m.version)
    versions = [m.version for m in migrations]
    if len(set(versions)) != len(versions):
        raise InvariantViolationError("duplicate migration versions", details={})
    return tuple(migrations)


async def applied_versions(engine: StateEngine) -> dict[int, str]:
    """Return {version: checksum} already recorded in the database."""
    async with engine.acquire() as connection:
        await connection.execute(_BASE_DDL)
        rows = await connection.fetch(
            "SELECT version, checksum FROM runtime_state.schema_migrations"
        )
    return {int(row["version"]): str(row["checksum"]) for row in rows}


async def migrate(engine: StateEngine) -> tuple[Migration, ...]:
    """Apply all pending migrations under an advisory lock.

    Returns the migrations applied by this call (empty when up to date).
    """
    async with engine.migration_lock() as connection:
        await connection.execute(_BASE_DDL)
        rows = await connection.fetch(
            "SELECT version, checksum FROM runtime_state.schema_migrations"
        )
        applied = {int(row["version"]): str(row["checksum"]) for row in rows}

        pending: list[Migration] = []
        for migration in available_migrations():
            existing = applied.get(migration.version)
            if existing is None:
                pending.append(migration)
            elif existing != migration.checksum:
                raise InvariantViolationError(
                    "applied migration checksum mismatch",
                    details={
                        "version": migration.version,
                        "name": migration.name,
                    },
                )

        for migration in pending:
            await connection.execute(migration.sql)
            await connection.execute(
                """
                INSERT INTO runtime_state.schema_migrations
                    (version, name, checksum)
                VALUES ($1, $2, $3)
                """,
                migration.version,
                migration.name,
                migration.checksum,
            )
    return tuple(pending)
