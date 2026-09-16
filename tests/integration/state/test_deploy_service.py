"""Deploy service loops (run_worker / run_control / main) — real Postgres + emulator.

Runs the same loop functions the production worker/control processes run,
with fast tick intervals, and drives finite + continuous workloads through
them end to end: submit → plan tick → dispatch outbox → Pub/Sub wakeup →
claim → execute → result. Also covers continuous reconcile/supervise ticks
and the ``python -m distributed_runtime`` entrypoint plumbing.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import socket
import uuid
from collections.abc import Awaitable, Callable

import pytest

os.environ.setdefault("PUBSUB_EMULATOR_HOST", "localhost:58085")

from distributed_runtime.control.admin import ContinuousAdmin
from distributed_runtime.control.client import RuntimeClient
from distributed_runtime.core import RevisionId, RunStatus
from distributed_runtime.deploy.config import DeployConfig
from distributed_runtime.deploy.service import run_control, run_worker
from distributed_runtime.gcp.pubsub import PubSubConfig, PubSubTransport
from distributed_runtime.registry import RuntimeRegistry
from distributed_runtime.state import StateEngine, migrate
from distributed_runtime.state.outbox import OutboxStore

from .conftest import requires_postgres
from .deploy_fixture_app import build_continuous_app, build_finite_app


def _emulator_reachable() -> bool:
    host, _, port = os.environ["PUBSUB_EMULATOR_HOST"].partition(":")
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
    except OSError:
        return False
    return True


requires_pubsub = pytest.mark.skipif(
    not _emulator_reachable(), reason="Pub/Sub emulator not reachable"
)


def _config(dsn: str) -> DeployConfig:
    return DeployConfig(
        dsn=dsn,
        project="",
        pool_revision="local",
        instance_id=f"instance:svc-{uuid.uuid4().hex[:8]}",
        pubsub_subscription=None,
        dispatch_topic="runtime-unit-dispatch",
        events_topic="runtime-events",
        lease_seconds=60,
        stale_worker_seconds=120,
        tick_seconds=0.05,
        heartbeat_seconds=0.05,
        poll_seconds=0.05,
    )


async def _until(predicate: Callable[[], Awaitable[int]], timeout: float = 15.0) -> None:
    """Poll an async predicate until truthy; fail on timeout."""

    async def wait() -> None:
        while not await predicate():
            await asyncio.sleep(0.05)

    await asyncio.wait_for(wait(), timeout)


def _with_loops(
    config: DeployConfig,
    engine: StateEngine,
    *,
    finite: RuntimeRegistry | None = None,
    continuous: RuntimeRegistry | None = None,
) -> tuple[asyncio.Task[None], asyncio.Task[None]]:
    control = asyncio.create_task(
        run_control(config, engine, finite_registry=finite, continuous_registry=continuous)
    )
    worker = asyncio.create_task(
        run_worker(config, engine, finite_registry=finite, continuous_registry=continuous)
    )
    return control, worker


@requires_postgres
def test_control_and_worker_loops_execute_finite_run(clean_state: str) -> None:
    """Control tick plans pending runs; worker poll tick claims+executes."""

    async def exercise() -> RunStatus:
        engine = await StateEngine.connect(clean_state)
        await migrate(engine)
        registry = build_finite_app().registry
        config = _config(clean_state)
        client = RuntimeClient(
            engine,
            application="example-product",
            planner_revision=RevisionId("rev:1"),
            execution_revision=RevisionId("rev:1"),
        )
        control, worker = _with_loops(config, engine, finite=registry)
        try:
            submitted = await client.submit(
                workload="range.sum", version="1.0.0", input={"start": 0, "end": 10}
            )
            return await client.wait(submitted.run_id, poll_seconds=0.05)
        finally:
            for task in (control, worker):
                task.cancel()
            await asyncio.gather(control, worker, return_exceptions=True)
            await engine.close()

    assert asyncio.run(exercise()) is RunStatus.SUCCEEDED


@requires_postgres
@requires_pubsub
def test_pubsub_dispatch_wakeup_path(clean_state: str) -> None:
    """Outbox rows publish to the dispatch topic; worker stream claims+acks."""

    async def exercise() -> RunStatus:
        subscription = f"svc-{uuid.uuid4().hex[:8]}"
        transport = PubSubTransport(
            PubSubConfig(
                project="ptf-local",
                topic="runtime-unit-dispatch",
                subscription=subscription,
            )
        )
        transport.ensure_topology()
        engine = await StateEngine.connect(clean_state)
        await migrate(engine)
        registry = build_finite_app().registry
        config = dataclasses.replace(
            _config(clean_state),
            project="ptf-local",
            pubsub_subscription=subscription,
        )
        client = RuntimeClient(
            engine,
            application="example-product",
            planner_revision=RevisionId("rev:1"),
            execution_revision=RevisionId("rev:1"),
        )
        control, worker = _with_loops(config, engine, finite=registry)
        try:
            submitted = await client.submit(
                workload="range.sum", version="1.0.0", input={"start": 0, "end": 10}
            )
            return await client.wait(submitted.run_id, poll_seconds=0.05)
        finally:
            for task in (control, worker):
                task.cancel()
            await asyncio.gather(control, worker, return_exceptions=True)
            await engine.close()

    assert asyncio.run(exercise()) is RunStatus.SUCCEEDED


@requires_postgres
def test_worker_supervises_and_control_reconciles_continuous(clean_state: str) -> None:
    """Reconcile tick assigns partitions to the live worker; supervisor emits."""

    async def exercise() -> int:
        engine = await StateEngine.connect(clean_state)
        await migrate(engine)
        registry = build_continuous_app().registry
        config = _config(clean_state)
        admin = ContinuousAdmin(engine, application="example-product")
        control, worker = _with_loops(config, engine, continuous=registry)
        try:
            await admin.deploy(workload="event.stream", version="1.0.0")

            async def emitted() -> int:
                rows = await OutboxStore(engine).pending()
                return len([row for row in rows if row.kind == "emission"])

            await _until(emitted)
            return await emitted()
        finally:
            for task in (control, worker):
                task.cancel()
            await asyncio.gather(control, worker, return_exceptions=True)
            await engine.close()

    assert asyncio.run(exercise()) == 1


@requires_postgres
def test_main_entrypoint_migrate_and_health(
    monkeypatch: pytest.MonkeyPatch, clean_state: str
) -> None:
    """``python -m distributed_runtime migrate`` applies schema and exits 0."""
    import urllib.request

    from distributed_runtime.deploy.main import _serve_health_port, main

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    monkeypatch.setenv("PORT", str(port))
    monkeypatch.setenv("RUNTIME_DSN", clean_state)
    _serve_health_port()
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as response:
        assert response.status == 200
        assert response.read() == b"ok"
    monkeypatch.delenv("PORT")  # main() must not try to rebind the port

    monkeypatch.setattr("sys.argv", ["distributed_runtime", "migrate"])
    assert main() == 0


def test_main_rejects_bad_args(monkeypatch: pytest.MonkeyPatch) -> None:
    from distributed_runtime.deploy.main import main

    monkeypatch.setattr("sys.argv", ["distributed_runtime"])
    assert main() == 2
    monkeypatch.setattr("sys.argv", ["distributed_runtime", "bogus"])
    assert main() == 2


@requires_postgres
def test_run_worker_role_shuts_down_on_sigterm(
    monkeypatch: pytest.MonkeyPatch, clean_state: str
) -> None:
    """_run('worker') starts loops and unwinds cleanly on SIGTERM."""
    import signal
    import threading

    from distributed_runtime.deploy.main import _run

    monkeypatch.setenv("RUNTIME_DSN", clean_state)
    monkeypatch.setenv("RUNTIME_FINITE_APP", "state.deploy_fixture_app:build_finite_app")
    monkeypatch.delenv("RUNTIME_CONTINUOUS_APP", raising=False)
    monkeypatch.delenv("RUNTIME_PUBSUB_SUBSCRIPTION", raising=False)
    monkeypatch.delenv("RUNTIME_PROJECT", raising=False)

    async def _migrate() -> None:
        engine = await StateEngine.connect(clean_state)
        try:
            await migrate(engine)
        finally:
            await engine.close()

    asyncio.run(_migrate())
    timer = threading.Timer(0.5, lambda: os.kill(os.getpid(), signal.SIGTERM))
    timer.start()
    try:
        assert asyncio.run(_run("worker")) == 0
    finally:
        timer.cancel()
