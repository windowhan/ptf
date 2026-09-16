"""Product-facing client API for finite runs.

submit → plan → wait → results. Aggregation stays with the product
(ADR-007): results() returns per-unit results in unit_key order.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Mapping
from dataclasses import dataclass

from distributed_runtime.core.enums import RunStatus
from distributed_runtime.core.envelope import JsonValue
from distributed_runtime.core.identifiers import (
    RevisionId,
    RunId,
    WorkloadId,
)
from distributed_runtime.state.engine import StateEngine
from distributed_runtime.state.finite import FiniteStateStore, StoredRun


@dataclass(frozen=True, slots=True)
class SubmittedRun:
    run_id: RunId
    status: RunStatus


class RuntimeClient:
    """Submit finite runs and read their state/results."""

    def __init__(
        self,
        engine: StateEngine,
        *,
        application: str,
        planner_revision: RevisionId,
        execution_revision: RevisionId,
    ) -> None:
        self._store = FiniteStateStore(engine)
        self._application = application
        self._planner_revision = planner_revision
        self._execution_revision = execution_revision

    async def submit(
        self,
        *,
        workload: str,
        version: str,
        input: Mapping[str, JsonValue],
        run_id: RunId | None = None,
    ) -> SubmittedRun:
        """Create a pending run; idempotent on run_id."""
        if run_id is None:
            run_id = RunId(f"run:{uuid.uuid4().hex[:16]}")
        stored = await self._store.submit_run(
            run_id=run_id,
            workload_id=WorkloadId(f"{self._application}.{workload}"),
            workload_name=workload,
            workload_version=version,
            planner_revision=self._planner_revision,
            execution_revision=self._execution_revision,
            input=input,
        )
        return SubmittedRun(run_id=stored.run_id, status=stored.status)

    async def get(self, run_id: RunId) -> StoredRun | None:
        return await self._store.get_run(run_id)

    async def wait(
        self,
        run_id: RunId,
        *,
        poll_seconds: float = 0.5,
        timeout_seconds: float = 300,
    ) -> RunStatus:
        """Poll until the run reaches a terminal status."""
        terminal = {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}
        waited = 0.0
        while waited < timeout_seconds:
            run = await self.get(run_id)
            if run is not None and run.status in terminal:
                return run.status
            await asyncio.sleep(poll_seconds)
            waited += poll_seconds
        raise TimeoutError(f"run {run_id} did not finish in {timeout_seconds}s")

    async def cancel(self, run_id: RunId) -> None:
        await self._store.transition_run(run_id, RunStatus.CANCELLED)

    async def results(self, run_id: RunId) -> tuple[JsonValue, ...]:
        """Per-unit results in unit_key order — aggregation is the product's."""
        return await self._store.results(run_id)
