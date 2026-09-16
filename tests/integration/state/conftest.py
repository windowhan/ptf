"""Shared fixtures for state-layer integration tests.

Requires a reachable PostgreSQL. Set RUNTIME_TEST_DSN or rely on the local
docker default. Tests skip cleanly when no server answers.
"""

from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest

DSN = os.environ.get(
    "RUNTIME_TEST_DSN",
    "postgresql://postgres:postgres@localhost:55432/runtime",
)


def _reachable() -> bool:
    async def probe() -> None:
        connection = await asyncpg.connect(DSN, timeout=3)
        await connection.close()

    try:
        asyncio.run(probe())
    except OSError:
        return False
    return True


requires_postgres = pytest.mark.skipif(
    not _reachable(), reason="PostgreSQL not reachable at RUNTIME_TEST_DSN"
)


@pytest.fixture()
def dsn() -> str:
    return DSN


@pytest.fixture()
def clean_state(dsn: str) -> str:
    """Drop and recreate runtime_state so each test sees empty schema."""

    async def reset() -> None:
        connection = await asyncpg.connect(dsn)
        try:
            await connection.execute("DROP SCHEMA IF EXISTS runtime_state CASCADE")
        finally:
            await connection.close()

    asyncio.run(reset())
    return dsn
