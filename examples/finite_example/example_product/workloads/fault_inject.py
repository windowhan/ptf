"""Fault-injection finite workload for E2E verification.

The product (not the runtime) decides how a unit fails: each unit's
payload carries a failure script that applies to its first
``fail_first`` attempts, then the unit echoes ``result``. The runtime
only sees ordinary contract errors, so retry/DLQ/timeout behavior is
exercised through the real dispatch → claim → outcome path.

Unit payload spec::

    {
        "key": "dlq",  # required — unit_key suffix
        "fail_first": 1,  # attempts to fail (default 0)
        "failure": "retryable",  # permanent|retryable|rate_limited|sleep
        "retry_after_seconds": 5,  # rate_limited floor (default 5)
        "sleep_seconds": 30,  # sleep mode duration (default 30)
        "timeout_seconds": 60,  # per-attempt unit timeout
        "max_attempts": 3,  # per-unit attempt bound
        "result": {"ok": true},  # echoed on the surviving attempt
    }
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from datetime import timedelta
from typing import cast

from distributed_runtime.core.enums import WorkloadMode
from distributed_runtime.core.envelope import JsonValue
from distributed_runtime.core.errors import (
    PermanentExecutionError,
    RateLimitedExecutionError,
    RetryableExecutionError,
)
from distributed_runtime.finite import (
    ExecutionContext,
    ExecutionResult,
    ExecutionUnit,
    WorkloadRequest,
)


class FaultInjectPlanner:
    """Turn each ``input.units`` spec into one execution unit."""

    name = "fault.inject"
    version = "1.0.0"
    mode = WorkloadMode.FINITE

    async def __call__(self, request: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
        specs = cast(list[Mapping[str, JsonValue]], (request.payload or {})["units"])
        for spec in specs:
            yield ExecutionUnit(
                unit_key=f"fault:{spec['key']}",
                handler="fault.inject.unit",
                payload=dict(spec),
                execution_class="lightweight",
                timeout_seconds=int(str(spec.get("timeout_seconds", 60))),
                max_attempts=int(str(spec.get("max_attempts", 3))),
            )


async def inject_unit(
    context: ExecutionContext, payload: Mapping[str, JsonValue]
) -> ExecutionResult:
    """Fail for ``fail_first`` attempts per the spec, then echo ``result``."""
    context.cancellation.raise_if_cancelled()
    fail_first = int(str(payload.get("fail_first", 0)))
    if context.attempt <= fail_first:
        failure = str(payload["failure"])
        if failure == "permanent":
            raise PermanentExecutionError(f"injected permanent failure ({payload['key']})")
        if failure == "rate_limited":
            raise RateLimitedExecutionError(
                f"injected rate limit ({payload['key']})",
                retry_after=timedelta(seconds=float(str(payload.get("retry_after_seconds", 5)))),
            )
        if failure == "sleep":
            await asyncio.sleep(float(str(payload.get("sleep_seconds", 30))))
        else:  # "retryable" and anything unknown — transient by default
            raise RetryableExecutionError(f"injected retryable failure ({payload['key']})")
    return {
        "key": payload["key"],
        "attempt": context.attempt,
        "result": payload.get("result", {"ok": True}),
    }
