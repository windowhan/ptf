"""GCP E2E driver — runs inside the VPC as a Cloud Run Job execution.

Exercises the deployed runtime end to end against the real control/worker
processes:

1. finite     — submit range.sum → workers claim via dispatch → SUCCEEDED
                → product aggregates per-unit results
2. continuous — deploy shard.pulse → reconciler assigns partitions to live
                workers → supervisors emit fenced pulses → events topic
3. failover   — stale one worker's heartbeat → reconciler marks it dead →
                partitions move with bumped fencing tokens

Env is the same contract the worker/control roles consume
(RUNTIME_DB_*, RUNTIME_PROJECT, RUNTIME_EVENTS_TOPIC, …).
Exits non-zero on failure; prints one ``[e2e]`` line per milestone.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from distributed_runtime.continuous import LeaseHandle
from distributed_runtime.control.admin import ContinuousAdmin
from distributed_runtime.control.client import RuntimeClient
from distributed_runtime.core import RevisionId, RunStatus
from distributed_runtime.core.envelope import VersionedEnvelope
from distributed_runtime.core.errors import InvariantViolationError
from distributed_runtime.core.identifiers import PartitionId, RuntimeInstanceId
from distributed_runtime.deploy.config import DeployConfig
from distributed_runtime.state import StateEngine
from distributed_runtime.state.continuous import ContinuousStateStore
from distributed_runtime.state.workers import WorkerRegistry

FINITE_END = 200
EXPECTED_PULSES = 12  # SHARD_COUNT(4) x PULSES_PER_SHARD(3)


async def _wait[T](probe: Callable[[], Awaitable[T | None]], timeout: float, label: str) -> T:
    waited = 0.0
    while waited < timeout:
        value = await probe()
        if value:
            return value
        await asyncio.sleep(2)
        waited += 2
    raise TimeoutError(label)


def _pull_events(project: str, subscription: str, want: int, timeout: float) -> int:
    """Pull+ack messages on the events subscription until ``want`` arrive."""
    import time

    from google.cloud import pubsub_v1

    subscriber = pubsub_v1.SubscriberClient()
    path = subscriber.subscription_path(project, subscription)
    deadline = time.time() + timeout
    received = 0
    while received < want and time.time() < deadline:
        response = subscriber.pull(
            request={
                "subscription": path,
                "max_messages": min(20, want - received),
                "return_immediately": True,
            },
            timeout=30,
        )
        for message in response.received_messages:
            received += 1
            print(f"[e2e] event: {message.message.data[:120]!r}")
        ack_ids = [m.ack_id for m in response.received_messages]
        if ack_ids:
            subscriber.acknowledge(request={"subscription": path, "ack_ids": ack_ids})
    return received


async def main() -> int:
    config = DeployConfig.from_env()
    engine = await StateEngine.connect(config.dsn)
    try:
        workers = WorkerRegistry(engine)

        live = await _wait(lambda: workers.list_active(), 420, "no live worker registered")
        print(f"[e2e] workers live: {[str(w.instance_id) for w in live]}")

        # ── 1. finite range.sum ─────────────────────────────────────
        client = RuntimeClient(
            engine,
            application="example-product",
            planner_revision=RevisionId("rev:1"),
            execution_revision=RevisionId("rev:1"),
        )
        run = await client.submit(
            workload="range.sum",
            version="1.0.0",
            input={"start": 0, "end": FINITE_END},
        )
        status = await client.wait(run.run_id, poll_seconds=2, timeout_seconds=420)
        if status is not RunStatus.SUCCEEDED:
            raise RuntimeError(f"finite run ended {status}")
        partials = await client.results(run.run_id)
        total = sum(int(p["subtotal"]) for p in partials)  # type: ignore[index]
        expected = sum(range(FINITE_END))
        if total != expected:
            raise RuntimeError(f"aggregation mismatch: {total} != {expected}")
        print(f"[e2e] finite PASS: run={run.run_id} units={len(partials)} total={total}")

        # ── 2. continuous shard.pulse ───────────────────────────────
        admin = ContinuousAdmin(engine, application="example-stream")
        deployment = await admin.deploy(workload="shard.pulse", version="1.0.0")

        async def assigned():
            view = await admin.view(deployment.deployment_id)
            if view.partitions and all(p.owner_id is not None for p in view.partitions):
                return view.partitions
            return None

        partitions = await _wait(assigned, 300, "partitions never assigned")
        owners = {str(p.partition_id): str(p.owner_id) for p in partitions}
        tokens = {str(p.partition_id): p.fencing_token for p in partitions}
        print(f"[e2e] continuous assigned: {owners}")

        pulled = await asyncio.to_thread(
            _pull_events, config.project, "runtime-events-all", EXPECTED_PULSES, 240
        )
        if pulled < EXPECTED_PULSES:
            raise RuntimeError(f"only {pulled}/{EXPECTED_PULSES} pulses reached events topic")
        print(f"[e2e] continuous emissions PASS: {pulled} messages on runtime-events")

        # ── 3. failover: suppress a partition owner's heartbeats ────
        victim = next(iter(set(owners.values())))

        async def stale_victim() -> None:
            async with engine.acquire() as connection:
                # Dead-worker simulation from the reconciler's side:
                # heartbeats stop AND the victim's partition leases run
                # out (a real death stops its renew_lease calls too —
                # without this the still-running victim keeps renewing
                # and acquire_lease rightly refuses to hand over a live
                # lease).
                await connection.execute(
                    """
                    UPDATE runtime_state.worker_instances
                    SET last_heartbeat_at = now() - interval '10 minutes'
                    WHERE instance_id = $1
                    """,
                    victim,
                )
                await connection.execute(
                    """
                    UPDATE runtime_state.continuous_partitions
                    SET lease_expires_at = now() - interval '1 second'
                    WHERE deployment_id = $1 AND owner_id = $2
                    """,
                    str(deployment.deployment_id),
                    victim,
                )

        # The victim process is still alive — its heartbeat loop would
        # overwrite a single stale write before a reconcile tick sees it.
        # Re-staling on every probe is exactly what a dead worker looks
        # like from the reconciler's side: the row stays stale past the
        # threshold, mark_stale_dead lands, partitions move. The victim
        # stays alive-but-fenced, which the split-brain check below uses.
        print(f"[e2e] suppressing heartbeats for {victim} — waiting for reclaim")

        async def moved():
            await stale_victim()
            view = await admin.view(deployment.deployment_id)
            victims_parts = [p for p in view.partitions if owners[str(p.partition_id)] == victim]
            if victims_parts and all(
                str(p.owner_id) != victim and p.fencing_token > tokens[str(p.partition_id)]
                for p in victims_parts
            ):
                return victims_parts
            return None

        moved_parts = await _wait(moved, 300, "failover did not reassign partitions")
        print(
            f"[e2e] failover PASS: {len(moved_parts)} partitions left {victim} "
            f"with bumped fencing tokens"
        )

        # ── 4. split-brain fence: displaced owner cannot emit ───────
        store = ContinuousStateStore(engine)
        stale_partition = moved_parts[0]
        stale_lease = LeaseHandle(
            deployment_id=deployment.deployment_id,
            partition_id=PartitionId(str(stale_partition.partition_id)),
            owner_id=RuntimeInstanceId(victim),
            fencing_token=tokens[str(stale_partition.partition_id)],
        )
        probe = VersionedEnvelope(
            schema_version=1,
            message_kind="continuous.emit",
            run_id=f"deployment:{deployment.deployment_id}",
            execution_id=str(stale_partition.partition_id),
            idempotency_key="e2e:fence-probe",
            application="example-stream",
            workload="shard.pulse",
            workload_version="1.0.0",
            handler="shard.pulse",
            attempt_generation=1,
            execution_class="stateful-stream",
            runtime_pool_revision=config.pool_revision,
            published_at=datetime.now(UTC).isoformat(),
            payload={"probe": "stale-owner"},
        )
        try:
            await store.emit(
                stale_lease,
                destination="pulses",
                stable_id="e2e:fence-probe",
                envelope=probe,
            )
        except InvariantViolationError:
            print("[e2e] fencing PASS: displaced owner's emission rejected")
        else:
            raise RuntimeError("stale owner emission was accepted — fence broken")

        print("[e2e] ALL CHECKS PASSED")
        return 0
    finally:
        await engine.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
