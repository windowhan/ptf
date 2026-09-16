"""Shard-pulse continuous workload.

Discovery returns a fixed set of shards; each partition handler emits a
bounded sequence of pulse events through the fenced ``pulses`` sink.
Emissions stop the moment ownership is cancelled — a stale owner can
never emit past a fencing-token move.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

from distributed_runtime.continuous import Partition, PartitionContext
from distributed_runtime.core.enums import WorkloadMode
from distributed_runtime.core.envelope import JsonValue, VersionedEnvelope
from distributed_runtime.core.identifiers import PartitionId

SHARD_COUNT = 4
PULSES_PER_SHARD = 3


class ShardPulseWorkload:
    """Discover fixed shards and emit a bounded pulse stream per shard."""

    name = "shard.pulse"
    version = "1.0.0"
    mode = WorkloadMode.CONTINUOUS

    async def discover_partitions(self) -> Sequence[Partition]:
        """Return the desired shard set; the reconciler assigns owners."""
        return [
            Partition(
                partition_id=PartitionId(f"shard:{i}"),
                payload={"shard": i},
            )
            for i in range(SHARD_COUNT)
        ]

    async def run_partition(self, context: PartitionContext, partition: Partition) -> None:
        """Emit ``PULSES_PER_SHARD`` pulse events, then stop.

        Each event carries the running cumulative count for its shard and
        uses a deterministic ``stable_id`` so at-least-once redelivery
        deduplicates instead of double-counting.
        """
        shard = int(partition.payload["shard"])
        for tick in range(PULSES_PER_SHARD):
            context.cancellation.raise_if_cancelled()
            await context.emit(
                "pulses",
                self._pulse(partition, shard, tick, tick + 1),
                stable_id=f"{partition.partition_id}:{tick}",
            )
            await asyncio.sleep(0)

    def _pulse(
        self, partition: Partition, shard: int, tick: int, cumulative: int
    ) -> VersionedEnvelope:
        payload: Mapping[str, JsonValue] = {
            "shard": shard,
            "tick": tick,
            "cumulative": cumulative,
        }
        return VersionedEnvelope(
            schema_version=1,
            message_kind="continuous.emit",
            run_id=f"deployment:{self.name}",
            execution_id=str(partition.partition_id),
            idempotency_key=f"{partition.partition_id}:{tick}",
            application="example-stream",
            workload=self.name,
            workload_version=self.version,
            handler=self.name,
            attempt_generation=1,
            execution_class="stateful-stream",
            runtime_pool_revision="local",
            published_at=datetime.now(UTC).isoformat(),
            payload=payload,
        )
