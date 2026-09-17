"""GCP E2E driver — runs inside the VPC as a Cloud Run Job execution.

Exercises the deployed runtime end to end against the real control/worker
processes. Scenarios (``RUNTIME_E2E_SCENARIOS`` comma-filter, default all):

  finite      range.sum through submit → dispatch → claim → results
  faults      fault.inject units: permanent→dead-letter, retryable,
              rate-limited floor, per-attempt timeout — all via workers
  pin         run pinned to a missing revision must stay unclaimed
  duplicate   republished dispatch wakeup does not re-execute the unit
  dlq         poison message nacks through to the dead-letter topic
  continuous  shard.pulse + shard.hex assignment, spread, emissions
  api-iam     client API answers, admin API denies the driver identity
  scale       MIG resize up/down converges registered worker count
  failover    owner heartbeats stop → partitions move with bumped fences
  fence       displaced owner's emission is rejected
  alerts      dead-letter alert policy exists and its metric recorded

Env is the same contract the worker/control roles consume plus driver-only
RUNTIME_API_URL / RUNTIME_ADMIN_URL / RUNTIME_MIG_NAME /
RUNTIME_DLQ_SUBSCRIPTION / RUNTIME_EVENTS_SUBSCRIPTION / RUNTIME_REGION.
Exits non-zero on failure; prints one ``[e2e]`` line per milestone.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from distributed_runtime.continuous import LeaseHandle
from distributed_runtime.control.admin import ContinuousAdmin
from distributed_runtime.control.client import RuntimeClient
from distributed_runtime.core import ExecutionStatus, RevisionId, RunStatus
from distributed_runtime.core.envelope import VersionedEnvelope
from distributed_runtime.core.errors import InvariantViolationError
from distributed_runtime.core.identifiers import PartitionId, RunId, RuntimeInstanceId
from distributed_runtime.deploy.config import DeployConfig
from distributed_runtime.state import StateEngine
from distributed_runtime.state.continuous import ContinuousStateStore
from distributed_runtime.state.finite import FiniteStateStore
from distributed_runtime.state.workers import WorkerRegistry

FINITE_END = 200
EXPECTED_PULSES = 12  # shard.pulse: 4 shards x 3 pulses
EXPECTED_HEX_PULSES = 12  # shard.hex: 6 shards x 2 pulses


async def _wait[T](probe: Callable[[], Awaitable[T | None]], timeout: float, label: str) -> T:
    waited = 0.0
    while waited < timeout:
        value = await probe()
        if value:
            return value
        await asyncio.sleep(2)
        waited += 2
    raise TimeoutError(label)


def _env(name: str) -> str | None:
    value = os.environ.get(name)
    return value if value else None


def _pull_events(
    project: str, subscription: str, want: dict[str, int], timeout: float
) -> dict[str, int]:
    """Pull+ack events until every workload's ``want`` count arrives.

    All pulled messages are acked — including non-matching ones — so one
    drain loop must observe every workload at once. Pulling per-workload
    would ack and discard the other workload's events before its turn.
    """
    import time

    from google.cloud import pubsub_v1

    subscriber = pubsub_v1.SubscriberClient()
    path = subscriber.subscription_path(project, subscription)
    deadline = time.time() + timeout
    received = {workload: 0 for workload in want}
    total_want = sum(want.values())
    while sum(received.values()) < total_want and time.time() < deadline:
        response = subscriber.pull(
            request={
                "subscription": path,
                "max_messages": min(40, total_want - sum(received.values()) + 10),
                "return_immediately": True,
            },
            timeout=30,
        )
        ack_ids: list[str] = []
        for message in response.received_messages:
            ack_ids.append(message.ack_id)
            try:
                data = json.loads(message.message.data)
            except Exception:
                continue
            workload = data.get("workload")
            if workload in want and received[workload] < want[workload]:
                received[workload] += 1
        if ack_ids:
            subscriber.acknowledge(request={"subscription": path, "ack_ids": ack_ids})
    return received


def _pull_raw(project: str, subscription: str, needle: bytes, timeout: float) -> bytes | None:
    """Pull messages until one contains ``needle`` bytes (acks nothing)."""
    import time

    from google.cloud import pubsub_v1

    subscriber = pubsub_v1.SubscriberClient()
    path = subscriber.subscription_path(project, subscription)
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = subscriber.pull(
            request={
                "subscription": path,
                "max_messages": 20,
                "return_immediately": True,
            },
            timeout=30,
        )
        for message in response.received_messages:
            if needle in message.message.data:
                return bytes(message.message.data)
        time.sleep(2)
    return None


def _authed_session():
    """AuthorizedSession for the ambient (driver) identity."""
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(credentials)  # type: ignore[no-untyped-call]


def _id_token(audience: str) -> str:
    import google.oauth2.id_token
    from google.auth.transport.requests import Request

    return google.oauth2.id_token.fetch_id_token(Request(), audience)


# ── scenarios ────────────────────────────────────────────────────────────


async def scenario_finite(client: RuntimeClient, **_ctx: object) -> None:
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
    if total != sum(range(FINITE_END)):
        raise RuntimeError(f"aggregation mismatch: {total}")
    print(f"[e2e] finite PASS: run={run.run_id} units={len(partials)} total={total}")
    return {"finished_run_id": run.run_id}


async def scenario_faults(client: RuntimeClient, engine: StateEngine, **_ctx: object) -> None:
    store = FiniteStateStore(engine)

    # permanent failure → dead-letter on first attempt → run FAILED
    run_dlq = await client.submit(
        workload="fault.inject",
        version="1.0.0",
        input={
            "units": [{"key": "perm", "failure": "permanent", "fail_first": 1, "max_attempts": 5}]
        },
    )
    status = await client.wait(run_dlq.run_id, poll_seconds=2, timeout_seconds=300)
    states = {s.unit_key: s for s in await store.unit_states(run_dlq.run_id)}
    if status is not RunStatus.FAILED:
        raise RuntimeError(f"permanent run ended {status}, want FAILED")
    perm = states["fault:perm"]
    if perm.status is not ExecutionStatus.DEAD_LETTERED or perm.attempt_count != 1:
        raise RuntimeError(f"permanent unit state wrong: {perm}")
    print("[e2e] fault: permanent → dead_lettered after 1 attempt, run FAILED")

    # retryable + rate-limited + timeout all recover on the second attempt
    run_mix = await client.submit(
        workload="fault.inject",
        version="1.0.0",
        input={
            "units": [
                {"key": "ret", "failure": "retryable", "fail_first": 1, "max_attempts": 3},
                {
                    "key": "rate",
                    "failure": "rate_limited",
                    "fail_first": 1,
                    "retry_after_seconds": 3,
                    "max_attempts": 3,
                },
                {
                    "key": "slow",
                    "failure": "sleep",
                    "fail_first": 1,
                    "sleep_seconds": 30,
                    "timeout_seconds": 3,
                    "max_attempts": 3,
                },
            ]
        },
    )
    status = await client.wait(run_mix.run_id, poll_seconds=2, timeout_seconds=420)
    if status is not RunStatus.SUCCEEDED:
        raise RuntimeError(f"mixed-fault run ended {status}, want SUCCEEDED")
    states = {s.unit_key: s for s in await store.unit_states(run_mix.run_id)}
    for key in ("fault:ret", "fault:rate", "fault:slow"):
        unit = states[key]
        if unit.status is not ExecutionStatus.SUCCEEDED or unit.attempt_count != 2:
            raise RuntimeError(f"{key}: status={unit.status} attempts={unit.attempt_count}")
    print("[e2e] fault: retryable / rate-limited / timeout all retried → SUCCEEDED")


async def scenario_pin(
    client: RuntimeClient, engine: StateEngine, config: DeployConfig, **_ctx: object
) -> None:
    """A run pinned to a revision with no pool must never be claimed."""
    missing_rev = RevisionId(f"{config.pool_revision}-missing")
    client_missing = RuntimeClient(
        engine,
        application="example-product",
        planner_revision=RevisionId(config.pool_revision),
        execution_revision=missing_rev,
    )
    run = await client_missing.submit(
        workload="range.sum", version="1.0.0", input={"start": 0, "end": 10}
    )
    store = FiniteStateStore(engine)
    await asyncio.sleep(90)
    states = await store.unit_states(run.run_id)
    if not states:
        raise RuntimeError("pinned run never planned — control path broken")
    bad = [s for s in states if s.attempt_count > 0 or s.status is not ExecutionStatus.READY]
    if bad:
        raise RuntimeError(f"cross-revision claim leaked: {bad}")
    await client.cancel(run.run_id)
    run_row = await client.get(run.run_id)
    if run_row is None or run_row.status is not RunStatus.CANCELLED:
        raise RuntimeError("cancel did not land")
    print(
        f"[e2e] pin PASS: {len(states)} units stayed ready on {missing_rev}, run cancelled cleanly"
    )


async def scenario_duplicate(
    client: RuntimeClient, engine: StateEngine, config: DeployConfig, **ctx: object
) -> None:
    """Republishing a finished unit's dispatch wakeup is a no-op."""
    run_id = ctx.get("finished_run_id")
    if not isinstance(run_id, RunId):
        raise RuntimeError("duplicate scenario needs a finished run")
    store = FiniteStateStore(engine)
    states = await store.unit_states(run_id)
    target = next(s for s in states if s.status is ExecutionStatus.SUCCEEDED)

    envelope = VersionedEnvelope(
        schema_version=1,
        message_kind="unit.dispatch",
        run_id=str(run_id),
        execution_id="exec:e2e-duplicate",
        idempotency_key=target.unit_key,
        application="example-product",
        workload="range.sum",
        workload_version="1.0.0",
        handler="range_sum.partial",
        attempt_generation=1,
        execution_class="lightweight",
        runtime_pool_revision=config.pool_revision,
        published_at=datetime.now(UTC).isoformat(),
        payload={"unit_key": target.unit_key},
    )

    def _republish() -> None:
        from google.cloud import pubsub_v1

        publisher = pubsub_v1.PublisherClient()
        topic = publisher.topic_path(config.project, config.dispatch_topic)
        for _ in range(3):  # at-least-once, pushed hard
            publisher.publish(
                topic,
                envelope.to_json(),
                runtime_pool_revision=config.pool_revision,
                message_kind="unit.dispatch",
            ).result(timeout=30)

    await asyncio.to_thread(_republish)
    await asyncio.sleep(30)  # deliveries + claims settle
    after = {s.unit_key: s for s in await store.unit_states(run_id)}[target.unit_key]
    if after.attempt_count != target.attempt_count:
        raise RuntimeError(
            f"duplicate dispatch re-executed {target.unit_key}: "
            f"{target.attempt_count} → {after.attempt_count}"
        )
    print(f"[e2e] duplicate PASS: {target.unit_key} re-delivered, still 1 attempt")


async def scenario_dlq(config: DeployConfig, **_ctx: object) -> None:
    """Malformed message is nacked through to the dead-letter topic."""
    dlq_sub = _env("RUNTIME_DLQ_SUBSCRIPTION")
    if not dlq_sub:
        print("[e2e] dlq SKIP: RUNTIME_DLQ_SUBSCRIPTION unset")
        return
    marker = f"e2e-poison-{datetime.now(UTC).timestamp()}".encode()

    def _publish_poison() -> None:
        from google.cloud import pubsub_v1

        publisher = pubsub_v1.PublisherClient()
        publisher.publish(
            publisher.topic_path(config.project, config.dispatch_topic),
            b'{"poison": "' + marker + b'"',  # not a valid envelope
            runtime_pool_revision=config.pool_revision,
        ).result(timeout=30)

    await asyncio.to_thread(_publish_poison)
    found = await asyncio.to_thread(_pull_raw, config.project, dlq_sub, marker, 300)
    if found is None:
        raise RuntimeError("poison message never reached the dead-letter topic")
    print("[e2e] dlq PASS: poison delivery dead-lettered to runtime-dead-letter")


async def scenario_continuous(
    admin: ContinuousAdmin, config: DeployConfig, **_ctx: object
) -> dict[str, object]:
    events_sub = _env("RUNTIME_EVENTS_SUBSCRIPTION") or "runtime-events-all"

    async def assigned(deployment_id: object) -> object:
        view = await admin.view(deployment_id)  # type: ignore[arg-type]
        if view.partitions and all(p.owner_id is not None for p in view.partitions):
            return view.partitions
        return None

    pulse = await admin.deploy(workload="shard.pulse", version="1.0.0")
    partitions = await _wait(
        lambda: assigned(pulse.deployment_id), 300, "pulse partitions never assigned"
    )
    owners = {str(p.partition_id): str(p.owner_id) for p in partitions}
    tokens = {str(p.partition_id): p.fencing_token for p in partitions}
    print(f"[e2e] continuous assigned: {owners}")

    hex_dep = await admin.deploy(workload="shard.hex", version="1.0.0")
    hex_parts = await _wait(
        lambda: assigned(hex_dep.deployment_id), 300, "hex partitions never assigned"
    )
    spread: dict[str, int] = {}
    for p in hex_parts:
        spread[str(p.owner_id)] = spread.get(str(p.owner_id), 0) + 1
    if len(spread) < 2:
        raise RuntimeError(f"6 partitions all on one worker: {spread}")
    if max(spread.values()) - min(spread.values()) > 1:
        raise RuntimeError(f"ownership spread exceeds weight diff 1: {spread}")
    print(f"[e2e] spread PASS: 6 partitions over {len(spread)} workers → {spread}")

    pulled = await asyncio.to_thread(
        _pull_events,
        config.project,
        events_sub,
        {"shard.pulse": EXPECTED_PULSES, "shard.hex": EXPECTED_HEX_PULSES},
        240,
    )
    pulled_pulse, pulled_hex = pulled["shard.pulse"], pulled["shard.hex"]
    if pulled_pulse < EXPECTED_PULSES or pulled_hex < EXPECTED_HEX_PULSES:
        raise RuntimeError(
            f"events short: pulse={pulled_pulse}/{EXPECTED_PULSES} "
            f"hex={pulled_hex}/{EXPECTED_HEX_PULSES}"
        )
    print(f"[e2e] emissions PASS: {pulled_pulse}+{pulled_hex} events on runtime-events")
    return {
        "pulse_deployment": pulse,
        "hex_deployment": hex_dep,
        "owners": owners,
        "tokens": tokens,
    }


async def scenario_failover(
    admin: ContinuousAdmin,
    engine: StateEngine,
    config: DeployConfig,
    **ctx: object,
) -> None:
    deployment = ctx.get("pulse_deployment")
    owners: dict[str, str] = ctx.get("owners")  # type: ignore[assignment]
    tokens: dict[str, int] = ctx.get("tokens")  # type: ignore[assignment]
    hex_dep = ctx.get("hex_deployment")
    if deployment is None or not owners or not tokens or hex_dep is None:
        raise RuntimeError("failover scenario needs continuous results")
    victim = next(iter(set(owners.values())))

    async def stale_victim() -> None:
        async with engine.acquire() as connection:
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
                WHERE owner_id = $1
                """,
                victim,
            )

    print(f"[e2e] suppressing heartbeats for {victim} — waiting for reclaim")

    async def moved():
        await stale_victim()
        for dep in (deployment, hex_dep):
            view = await admin.view(dep.deployment_id)  # type: ignore[union-attr]
            if any(str(p.owner_id) == victim for p in view.partitions):
                return None
        return True

    await _wait(moved, 300, "failover did not reassign partitions")
    print(f"[e2e] failover PASS: partitions left {victim}")

    # split-brain fence: the displaced owner cannot emit
    store = ContinuousStateStore(engine)
    stale_partition_id = next(iter(owners))
    stale_lease = LeaseHandle(
        deployment_id=deployment.deployment_id,  # type: ignore[union-attr]
        partition_id=PartitionId(stale_partition_id),
        owner_id=RuntimeInstanceId(victim),
        fencing_token=tokens[stale_partition_id],
    )
    probe = VersionedEnvelope(
        schema_version=1,
        message_kind="continuous.emit",
        run_id=f"deployment:{deployment.deployment_id}",  # type: ignore[union-attr]
        execution_id=stale_partition_id,
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
        print("[e2e] fence PASS: displaced owner's emission rejected")
    else:
        raise RuntimeError("stale owner emission was accepted — fence broken")


async def scenario_api_iam(**_ctx: object) -> None:
    """Driver identity may call the client API but not the admin API."""
    api_url, admin_url = _env("RUNTIME_API_URL"), _env("RUNTIME_ADMIN_URL")
    if not api_url or not admin_url:
        print("[e2e] api-iam SKIP: RUNTIME_API_URL/RUNTIME_ADMIN_URL unset")
        return

    def _get(url: str) -> int:
        token = _id_token(url)
        # /healthz is intercepted by the run.app edge (Google-served 404) —
        # the app health route on "/" is the reachable check.
        request = urllib.request.Request(f"{url}/", headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return response.status
        except urllib.error.HTTPError as error:
            return error.code

    api_status = await asyncio.to_thread(_get, api_url)
    admin_status = await asyncio.to_thread(_get, admin_url)
    if api_status != 200:
        raise RuntimeError(f"client API unreachable for invoker: {api_status}")
    if admin_status != 403:
        raise RuntimeError(f"admin API returned {admin_status} to a non-invoker — want 403")
    print("[e2e] api-iam PASS: client 200 / admin 403 for the driver identity")


async def scenario_scale(engine: StateEngine, config: DeployConfig, **_ctx: object) -> None:
    mig = _env("RUNTIME_MIG_NAME")
    region = _env("RUNTIME_REGION")
    if not mig or not region:
        print("[e2e] scale SKIP: RUNTIME_MIG_NAME/RUNTIME_REGION unset")
        return
    workers = WorkerRegistry(engine)
    session = _authed_session()
    # The MIG is autoscaled — instanceGroupManagers.resize is rejected (412).
    # Drive the scale through the autoscaler's min replicas instead, which is
    # also the realistic path: the autoscaler performs the actual resize.
    name = f"{mig}-autoscaler"
    autoscaler = (
        f"https://compute.googleapis.com/compute/v1/projects/{config.project}"
        f"/regions/{region}/autoscalers/{name}"
    )
    # update/patch address the collection path, not .../autoscalers/{name}
    collection = autoscaler.rsplit("/", 1)[0]

    def _autoscaler() -> dict:
        response = session.get(autoscaler)
        if response.status_code != 200:
            raise RuntimeError(f"autoscaler get: {response.status_code} {response.text}")
        return dict(response.json())

    def _resize(size: int) -> None:
        resource = _autoscaler()
        policy = dict(resource["autoscalingPolicy"])
        policy["minNumReplicas"] = size
        if policy.get("maxNumReplicas", 0) < size:
            policy["maxNumReplicas"] = size
        resource["autoscalingPolicy"] = policy
        response = session.patch(collection, params={"autoscaler": name}, json=resource)
        if response.status_code != 200:
            raise RuntimeError(f"autoscaler patch: {response.status_code} {response.text}")

    mig_api = (
        f"https://compute.googleapis.com/compute/v1/projects/{config.project}"
        f"/regions/{region}/instanceGroupManagers/{mig}"
    )

    def _managed_instances() -> list[str]:
        response = session.post(f"{mig_api}/listManagedInstances")
        if response.status_code != 200:
            raise RuntimeError(f"listManagedInstances: {response.status_code} {response.text}")
        return [
            item["instance"]
            for item in response.json().get("managedInstances", [])
            if item.get("instanceStatus") == "RUNNING"
        ]

    def _delete_instance(instance: str) -> None:
        # Autoscaler-driven scale-in is governed by stabilization windows
        # (minutes even at 60s) — too slow for an E2E gate. The realistic
        # loss path is instance removal, which deleteInstances performs on
        # autoscaled MIGs (unlike resize, which is rejected).
        response = session.post(f"{mig_api}/deleteInstances", json={"instances": [instance]})
        if response.status_code != 200:
            raise RuntimeError(f"deleteInstances: {response.status_code} {response.text}")

    async def live_ids() -> set[str]:
        return {str(w.instance_id) for w in await workers.list_active()}

    before = await live_ids()
    await asyncio.to_thread(_resize, len(before) + 1)
    added = await _wait(
        lambda: _new_worker(workers, before), 900, "scale-up worker never registered"
    )
    print(f"[e2e] scale-up PASS: new worker {added} registered")

    # Drop the autoscaler floor first, then remove the instance — the MIG
    # replaces a deleted member while the floor still demands it.
    await asyncio.to_thread(_resize, len(before))
    managed = await asyncio.to_thread(_managed_instances)
    victim = next((url for url in managed if url.endswith(f"/{added}")), None)
    if victim is None:
        raise RuntimeError(f"new worker {added} not among managed instances: {managed}")
    await asyncio.to_thread(_delete_instance, victim)
    await _wait(
        lambda: _worker_gone(workers, str(added)),
        300,
        f"scale-down worker {added} never deregistered",
    )
    print(f"[e2e] scale-down PASS: {added} deregistered after instance loss")


async def _new_worker(workers: WorkerRegistry, before: set[str]) -> str | None:
    live = await workers.list_active()
    for worker in live:
        if str(worker.instance_id) not in before:
            return str(worker.instance_id)
    return None


async def _worker_gone(workers: WorkerRegistry, instance_id: str) -> bool | None:
    live = await workers.list_active()
    return True if all(str(w.instance_id) != instance_id for w in live) else None


async def _live_at_least(workers: WorkerRegistry, n: int) -> bool | None:
    return True if len(await workers.list_active()) >= n else None


async def _live_at_most(workers: WorkerRegistry, n: int) -> bool | None:
    return True if len(await workers.list_active()) <= n else None


async def scenario_alerts(config: DeployConfig, **_ctx: object) -> None:
    """Alert policy exists and the dead-letter topic metric records data.

    The dlq scenario may have run long ago, outside any reasonable
    lookback — so publish a probe straight to the dead-letter topic and
    wait for ``send_message_operation_count`` to report it.
    """
    session = _authed_session()
    response = session.get(
        f"https://monitoring.googleapis.com/v3/projects/{config.project}/alertPolicies"
    )
    if response.status_code != 200:
        raise RuntimeError(f"alertPolicies.list: {response.status_code}")
    names = [p.get("displayName", "") for p in response.json().get("alertPolicies", [])]
    if not any("dead-letter" in n for n in names):
        raise RuntimeError(f"dead-letter alert policy missing: {names}")

    def _probe() -> None:
        from google.cloud import pubsub_v1

        publisher = pubsub_v1.PublisherClient()
        publisher.publish(
            publisher.topic_path(config.project, "runtime-dead-letter"),
            b"e2e:alerts-probe",
        ).result(timeout=30)

    await asyncio.to_thread(_probe)

    def _metric_has_data() -> bool:
        start = datetime.now(UTC).timestamp() - 600
        params = {
            "filter": (
                'metric.type="pubsub.googleapis.com/topic/send_message_operation_count" '
                'AND resource.labels.topic_id="runtime-dead-letter"'
            ),
            "interval.startTime": datetime.fromtimestamp(start, UTC).isoformat(),
            "interval.endTime": datetime.now(UTC).isoformat(),
            "view": "HEADERS",
        }
        series = session.get(
            f"https://monitoring.googleapis.com/v3/projects/{config.project}/timeSeries",
            params=params,
        )
        return series.status_code == 200 and bool(series.json().get("timeSeries"))

    async def _poll_metric() -> bool:
        return await asyncio.to_thread(_metric_has_data)

    await _wait(_poll_metric, 300, "dead-letter topic metric never recorded the probe")
    print("[e2e] alerts PASS: dead-letter policy present, metric recording")


# ── entrypoint ───────────────────────────────────────────────────────────

SCENARIOS: list[tuple[str, Callable[..., Awaitable[object]]]] = [
    ("finite", scenario_finite),
    ("faults", scenario_faults),
    ("pin", scenario_pin),
    ("duplicate", scenario_duplicate),
    ("dlq", scenario_dlq),
    ("continuous", scenario_continuous),
    ("api-iam", scenario_api_iam),
    ("scale", scenario_scale),
    ("failover", scenario_failover),
    ("alerts", scenario_alerts),
]


async def main() -> int:
    config = DeployConfig.from_env()
    engine = await StateEngine.connect(config.dsn)
    try:
        workers = WorkerRegistry(engine)
        live = await _wait(lambda: workers.list_active(), 420, "no live worker registered")
        print(f"[e2e] workers live: {[str(w.instance_id) for w in live]}")

        client = RuntimeClient(
            engine,
            application="example-product",
            planner_revision=RevisionId(config.pool_revision),
            execution_revision=RevisionId(config.pool_revision),
        )
        admin = ContinuousAdmin(engine, application="example-stream")

        selected = {
            s.strip() for s in (_env("RUNTIME_E2E_SCENARIOS") or "").split(",") if s.strip()
        }
        ctx: dict[str, object] = {}
        for name, scenario in SCENARIOS:
            if selected and name not in selected:
                print(f"[e2e] {name} SKIP")
                continue
            print(f"[e2e] ── {name}")
            result = await scenario(client=client, admin=admin, engine=engine, config=config, **ctx)
            if isinstance(result, dict):
                ctx.update(result)
        print("[e2e] ALL CHECKS PASSED")
        return 0
    finally:
        await engine.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
