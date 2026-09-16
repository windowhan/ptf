"""Importable app factories for deploy-entrypoint tests.

``python -m distributed_runtime worker`` resolves product apps through
``RUNTIME_FINITE_APP=module:callable`` — these factories live in a plain
module (no conftest side effects) so the entrypoint can import them by
spec the same way a product wheel provides its factory.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence

from distributed_runtime import RuntimeApplication
from distributed_runtime.continuous import (
    LeaseHandle,
    Partition,
    PartitionContext,
    SinkGuarantee,
)
from distributed_runtime.core import (
    JsonValue,
    PartitionId,
    VersionedEnvelope,
    WorkloadMode,
)
from distributed_runtime.finite import ExecutionContext, ExecutionUnit, WorkloadRequest


class RangeSumPlanner:
    name = "range.sum"
    version = "1.0.0"
    mode = WorkloadMode.FINITE

    async def __call__(self, request: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
        lo = int(str(request.payload["start"]))
        hi = int(str(request.payload["end"]))
        while lo < hi:
            step = min(lo + 5, hi)
            yield ExecutionUnit(
                unit_key=f"range:{lo}-{step}",
                handler="range_sum.partial",
                payload={"start": lo, "end": step},
                execution_class="lightweight",
                timeout_seconds=30,
                max_attempts=3,
            )
            lo = step


async def sum_partial(context: ExecutionContext, payload: Mapping[str, JsonValue]) -> JsonValue:
    return {"subtotal": sum(range(int(str(payload["start"])), int(str(payload["end"]))))}


def _envelope(pid: str, seq: int) -> VersionedEnvelope:
    return VersionedEnvelope(
        schema_version=1,
        message_kind="continuous.emit",
        run_id="deployment:x",
        execution_id=f"partition:{pid}",
        idempotency_key=f"{pid}:{seq}",
        application="example-product",
        workload="event.stream",
        workload_version="1.0.0",
        handler="event.stream",
        attempt_generation=1,
        execution_class="stateful-stream",
        runtime_pool_revision="local",
        published_at="2026-01-01T00:00:00Z",
        payload={"p": pid, "seq": seq},
    )


class EmitOnceStream:
    name = "event.stream"
    version = "1.0.0"
    mode = WorkloadMode.CONTINUOUS

    async def discover_partitions(self) -> Sequence[Partition]:
        return [Partition(partition_id=PartitionId("p-0"), payload={})]

    async def run_partition(self, context: PartitionContext, partition: Partition) -> None:
        context.cancellation.raise_if_cancelled()
        await context.emit(
            "events",
            _envelope(str(partition.partition_id), 0),
            stable_id=f"{partition.partition_id}:0",
        )


class _Sink:
    guarantee = SinkGuarantee.RUNTIME_FENCED

    async def emit(
        self,
        event: VersionedEnvelope,
        *,
        stable_id: str,
        lease: LeaseHandle,
    ) -> None:
        return None


def build_finite_app() -> RuntimeApplication:
    app = RuntimeApplication("example-product")
    app.registry.register_finite(
        name="range.sum",
        semantic_version="1.0.0",
        execution_class="lightweight",
        planner=RangeSumPlanner(),
        handler=sum_partial,
    )
    return app


def build_continuous_app() -> RuntimeApplication:
    app = RuntimeApplication("example-product")
    app.registry.register_continuous(
        name="event.stream",
        semantic_version="1.0.0",
        execution_class="stateful-stream",
        workload=EmitOnceStream(),
    )
    app.registry.register_sink("events", _Sink())
    return app
