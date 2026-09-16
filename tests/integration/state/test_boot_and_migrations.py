"""IT-SQL-BOOT + FT-MIGRATE-BASE: engine bootstrap and migration ordering."""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from distributed_runtime.core.errors import InvariantViolationError
from distributed_runtime.state import (
    StateEngine,
    applied_versions,
    available_migrations,
    migrate,
)

from .conftest import requires_postgres


def test_packaged_migrations_are_ordered_and_unique() -> None:
    migrations = available_migrations()
    versions = [m.version for m in migrations]
    assert versions == sorted(versions)
    assert len(set(versions)) == len(versions)
    assert {m.name for m in migrations} >= {
        "finite_planning",
        "finite_execution",
        "outbox",
        "continuous",
    }
    for migration in migrations:
        assert "runtime_state" in migration.sql
        assert len(migration.checksum) == 64


@requires_postgres
def test_engine_connects_and_pools(clean_state: str) -> None:
    async def exercise() -> None:
        engine = await StateEngine.connect(clean_state, min_size=1, max_size=2)
        try:
            async with engine.acquire() as connection:
                assert await connection.fetchval("SELECT 1") == 1
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_migrate_applies_all_and_is_idempotent(clean_state: str) -> None:
    async def exercise() -> tuple[int, int]:
        engine = await StateEngine.connect(clean_state)
        try:
            first = await migrate(engine)
            second = await migrate(engine)
            return len(first), len(second)
        finally:
            await engine.close()

    first, second = asyncio.run(exercise())
    assert first == len(available_migrations())
    assert second == 0


@requires_postgres
def test_migrate_creates_all_tables(clean_state: str) -> None:
    async def exercise() -> set[str]:
        engine = await StateEngine.connect(clean_state)
        try:
            await migrate(engine)
            async with engine.acquire() as connection:
                rows = await connection.fetch(
                    """
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'runtime_state'
                    """
                )
            return {str(row["table_name"]) for row in rows}
        finally:
            await engine.close()

    tables = asyncio.run(exercise())
    assert {
        "schema_migrations",
        "finite_runs",
        "finite_plan_units",
        "finite_units",
        "finite_attempts",
        "outbox_messages",
        "continuous_deployments",
        "continuous_partitions",
        "worker_instances",
    } <= tables


@requires_postgres
def test_applied_versions_record_checksums(clean_state: str) -> None:
    async def exercise() -> dict[int, str]:
        engine = await StateEngine.connect(clean_state)
        try:
            await migrate(engine)
            return await applied_versions(engine)
        finally:
            await engine.close()

    applied = asyncio.run(exercise())
    expected = {m.version: m.checksum for m in available_migrations()}
    assert applied == expected


@requires_postgres
def test_checksum_mismatch_fails(clean_state: str) -> None:
    async def exercise() -> None:
        engine = await StateEngine.connect(clean_state)
        try:
            await migrate(engine)
            async with engine.acquire() as connection:
                await connection.execute(
                    """
                    UPDATE runtime_state.schema_migrations
                    SET checksum = 'tampered' WHERE version = 1
                    """
                )
            with pytest.raises(InvariantViolationError, match="checksum"):
                await migrate(engine)
        finally:
            await engine.close()

    asyncio.run(exercise())


@requires_postgres
def test_concurrent_migrations_serialize(clean_state: str) -> None:
    async def exercise() -> int:
        engine = await StateEngine.connect(clean_state, min_size=2, max_size=4)
        try:
            results = await asyncio.gather(migrate(engine), migrate(engine), migrate(engine))
            return sum(len(batch) for batch in results)
        finally:
            await engine.close()

    total_applied = asyncio.run(exercise())
    assert total_applied == len(available_migrations())


@requires_postgres
def test_unique_plan_unit_key_enforced(clean_state: str) -> None:
    async def exercise() -> None:
        engine = await StateEngine.connect(clean_state)
        try:
            await migrate(engine)
            async with engine.acquire() as connection:
                await connection.execute(
                    """
                    INSERT INTO runtime_state.finite_runs
                        (run_id, workload_id, workload_name, workload_version,
                         planner_revision, execution_revision, input)
                    VALUES ('run:1', 'w:1', 'w', '1.0.0', 'rev:1', 'rev:1', '{}')
                    """
                )
                insert = """
                    INSERT INTO runtime_state.finite_plan_units
                        (run_id, planning_generation, ordinal, unit_key,
                         payload_hash, unit)
                    VALUES ('run:1', 1, $1, 'u:dup', 'h', '{}')
                """
                await connection.execute(insert, 0)
                with pytest.raises(asyncpg.UniqueViolationError):
                    await connection.execute(insert, 1)
        finally:
            await engine.close()

    asyncio.run(exercise())
