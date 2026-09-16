"""Range-sum finite workload.

The planner splits ``[start, end)`` into fixed-size chunks; each unit
computes its subtotal. Final aggregation is product-owned (ADR-007):
the submitter reads unit results with ``client.results(run_id)`` and
combines them with :func:`aggregate_subtotals`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from typing import cast

from distributed_runtime.core.enums import WorkloadMode
from distributed_runtime.core.envelope import JsonValue
from distributed_runtime.finite import (
    ExecutionContext,
    ExecutionResult,
    ExecutionUnit,
    WorkloadRequest,
)

CHUNK_SIZE = 10_000


class RangeSumPlanner:
    """Split ``request.payload``'s ``[start, end)`` into chunk units."""

    name = "range.sum"
    version = "1.0.0"
    mode = WorkloadMode.FINITE

    async def __call__(self, request: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
        payload = request.payload or {}
        lo = cast(int, payload["start"])
        hi = cast(int, payload["end"])
        while lo < hi:
            step = min(lo + CHUNK_SIZE, hi)
            yield ExecutionUnit(
                unit_key=f"range:{lo}-{step}",
                handler="range_sum.partial",
                payload={"start": lo, "end": step},
                execution_class="lightweight",
            )
            lo = step


async def sum_partial(
    context: ExecutionContext, payload: Mapping[str, JsonValue]
) -> ExecutionResult:
    """Sum one ``[start, end)`` chunk and return it as a small JSON result."""
    context.cancellation.raise_if_cancelled()
    lo = cast(int, payload["start"])
    hi = cast(int, payload["end"])
    return {"subtotal": sum(range(lo, hi))}


def aggregate_subtotals(results: Sequence[ExecutionResult]) -> int:
    """Combine unit results into the final total.

    Product-owned final aggregation: the runtime stores and returns unit
    results; the product decides how to combine them and refuses to
    aggregate a unit whose result is not a ``{"subtotal": int}`` mapping.
    """
    total = 0
    for result in results:
        if not isinstance(result, Mapping):
            raise TypeError(f"unexpected unit result: {result!r}")
        total += cast(int, result["subtotal"])
    return total
