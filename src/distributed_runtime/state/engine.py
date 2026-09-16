"""PostgreSQL engine bootstrap for the runtime state layer.

The state layer uses asyncpg directly — no ORM. All statements are explicit
SQL so transaction boundaries stay visible at the call site.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import asyncpg
from asyncpg.pool import PoolConnectionProxy

# Lock key for migration/advisory operations: arbitrary but stable constant
# ("ptf" as a base36-ish number). Keeps concurrent migrators serialized.
MIGRATION_LOCK_KEY = 7278


@dataclass(slots=True)
class StateEngine:
    """Thin pool wrapper owning connections to the runtime_state schema."""

    pool: asyncpg.Pool

    @classmethod
    async def connect(
        cls,
        dsn: str,
        *,
        min_size: int = 1,
        max_size: int = 10,
    ) -> StateEngine:
        """Open a connection pool. The caller owns shutdown via close()."""
        pool = await asyncpg.create_pool(dsn=dsn, min_size=min_size, max_size=max_size)
        return cls(pool=pool)

    async def close(self) -> None:
        await self.pool.close()

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[PoolConnectionProxy]:
        async with self.pool.acquire() as connection:
            yield connection

    @asynccontextmanager
    async def migration_lock(self) -> AsyncIterator[PoolConnectionProxy]:
        """Hold a transaction-level advisory lock for serialized migrations."""
        async with (
            self.pool.acquire() as connection,
            connection.transaction(),
        ):
            await connection.execute("SELECT pg_advisory_xact_lock($1)", MIGRATION_LOCK_KEY)
            yield connection
