"""Local-kit verification for the shard-pulse continuous workload."""

from __future__ import annotations

import asyncio

from example_stream.app import build_application
from example_stream.workloads.shard_pulse import PULSES_PER_SHARD, SHARD_COUNT

from distributed_runtime.core.enums import PartitionStatus
from distributed_runtime.testing import ContinuousRuntimeTestKit


def test_shard_pulse_emits_per_partition() -> None:
    async def scenario() -> None:
        app = build_application()
        kit = ContinuousRuntimeTestKit(registry=app.registry)
        deployment_id = kit.deploy(workload="shard.pulse", version="1.0.0")
        partitions = await kit.reconcile(deployment_id)
        assert len(partitions) == SHARD_COUNT
        assert all(p.status is PartitionStatus.ASSIGNED for p in partitions)

        await kit.run_for(2.0)
        emissions = kit.emissions()
        assert len(emissions) == SHARD_COUNT * PULSES_PER_SHARD
        stable_ids = {e.stable_id for e in emissions}
        assert stable_ids == {
            f"shard:{shard}:{tick}"
            for shard in range(SHARD_COUNT)
            for tick in range(PULSES_PER_SHARD)
        }

    asyncio.run(scenario())


def test_shard_pulse_stops_on_drain() -> None:
    async def scenario() -> None:
        app = build_application()
        kit = ContinuousRuntimeTestKit(registry=app.registry)
        deployment_id = kit.deploy(workload="shard.pulse", version="1.0.0")
        await kit.reconcile(deployment_id)
        kit.drain(deployment_id)
        await kit.run_for(1.0)
        # drain cancels every context before handlers emit
        assert kit.emissions() == ()

    asyncio.run(scenario())
