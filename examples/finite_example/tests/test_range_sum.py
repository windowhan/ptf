"""Local-kit verification for the range-sum finite workload."""

from __future__ import annotations

import asyncio

from example_product.app import build_application
from example_product.workloads.range_sum import aggregate_subtotals

from distributed_runtime.core.enums import ExecutionStatus
from distributed_runtime.testing import FiniteRuntimeTestKit


def test_range_sum_end_to_end() -> None:
    async def scenario() -> None:
        app = build_application()
        kit = FiniteRuntimeTestKit(registry=app.registry)
        run_id = await kit.submit(
            workload="range.sum",
            version="1.0.0",
            payload={"start": 0, "end": 25_000},
        )
        attempts = await kit.run_pending(run_id)
        assert len(attempts) == 3  # 0-10000, 10000-20000, 20000-25000
        assert all(a.status is ExecutionStatus.SUCCEEDED for a in attempts)
        total = aggregate_subtotals(kit.results(run_id))
        assert total == sum(range(25_000))

    asyncio.run(scenario())


def test_range_sum_empty_range() -> None:
    async def scenario() -> None:
        app = build_application()
        kit = FiniteRuntimeTestKit(registry=app.registry)
        run_id = await kit.submit(
            workload="range.sum",
            version="1.0.0",
            payload={"start": 5, "end": 5},
        )
        await kit.run_pending(run_id)
        assert aggregate_subtotals(kit.results(run_id)) == 0

    asyncio.run(scenario())
