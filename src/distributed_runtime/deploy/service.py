"""Long-running worker and control-plane loops.

Each loop is a ``while True`` with a sleep — the tick-based components own
concurrency discipline, these functions only provide pacing, logging, and
cancellation plumbing. A failure inside one tick is logged and retried on
the next; the process never dies silently on a transient backend error.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import NoReturn

from distributed_runtime.continuous.contracts import discover_partitions
from distributed_runtime.control.dispatcher import OutboxDispatcher
from distributed_runtime.control.planner import PlannerRunner
from distributed_runtime.control.reconciler import Reconciler
from distributed_runtime.core.enums import DeploymentStatus
from distributed_runtime.core.identifiers import (
    RevisionId,
    RuntimeInstanceId,
)
from distributed_runtime.deploy.config import DeployConfig, resolve_instance_id
from distributed_runtime.registry import ContinuousRegistration, RuntimeRegistry
from distributed_runtime.state.continuous import ContinuousStateStore
from distributed_runtime.state.engine import StateEngine
from distributed_runtime.state.finite import FiniteStateStore
from distributed_runtime.state.outbox import OutboxMessage
from distributed_runtime.state.workers import WorkerRegistry
from distributed_runtime.worker.continuous import ContinuousSupervisor
from distributed_runtime.worker.finite import FiniteWorker

logger = logging.getLogger("distributed_runtime.deploy")


async def _forever(name: str, interval: float, tick: Callable[[], Awaitable[object]]) -> NoReturn:
    """Run ``tick`` every ``interval`` seconds; log and survive failures."""
    while True:
        try:
            await tick()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("%s tick failed — retrying in %.1fs", name, interval)
        await asyncio.sleep(interval)


async def run_worker(
    config: DeployConfig,
    engine: StateEngine,
    *,
    finite_registry: RuntimeRegistry | None,
    continuous_registry: RuntimeRegistry | None,
) -> None:
    """Worker process: finite claims + continuous supervision + heartbeat."""
    instance_id = RuntimeInstanceId(resolve_instance_id(config.instance_id))
    workers = WorkerRegistry(engine)
    classes = sorted(
        {
            registration.execution_class
            for registry in (finite_registry, continuous_registry)
            if registry is not None
            for registration in registry.workloads()
        }
    )
    await workers.register(
        instance_id,
        execution_classes=classes,
        revision=RevisionId(config.pool_revision),
    )
    logger.info("worker %s registered (classes=%s)", instance_id, classes)

    tasks: list[asyncio.Task[None]] = []

    async def heartbeat() -> None:
        alive = await workers.heartbeat(instance_id, revision=RevisionId(config.pool_revision))
        if not alive:
            logger.warning("heartbeat rejected — instance %s unknown", instance_id)

    tasks.append(asyncio.create_task(_forever("heartbeat", config.heartbeat_seconds, heartbeat)))

    if finite_registry is not None:
        worker = FiniteWorker(
            registry=finite_registry,
            engine=engine,
            instance_id=instance_id,
        )

        async def poll() -> int:
            handled = await worker.poll_once(limit=10)
            if handled:
                logger.info("poll claimed %d unit(s)", len(handled))
            return len(handled)

        tasks.append(asyncio.create_task(_forever("finite-poll", config.poll_seconds, poll)))
        subscription = config.pubsub_subscription
        if subscription and config.project:
            tasks.append(asyncio.create_task(_consume_dispatches(config, worker, subscription)))

    supervisors: list[ContinuousSupervisor] = []
    if continuous_registry is not None:
        supervisor = ContinuousSupervisor(
            registry=continuous_registry,
            engine=engine,
            instance_id=instance_id,
            lease_seconds=config.lease_seconds,
        )
        supervisors.append(supervisor)
        store = ContinuousStateStore(engine)
        served = {
            str(registration.name)
            for registration in continuous_registry.workloads()
            if isinstance(registration, ContinuousRegistration)
        }

        async def supervise() -> int:
            deployments = await store.list_deployments(
                [DeploymentStatus.ACTIVE, DeploymentStatus.DRAINING]
            )
            running = 0
            for deployment in deployments:
                if str(deployment.workload_id) not in served:
                    continue
                running += await supervisor.tick(deployment.deployment_id)
            return running

        tasks.append(asyncio.create_task(_forever("supervisor", config.tick_seconds, supervise)))

    try:
        await asyncio.gather(*tasks)
    finally:
        for supervisor in supervisors:
            await supervisor.stop_all()


async def _consume_dispatches(
    config: DeployConfig, worker: FiniteWorker, subscription: str
) -> NoReturn:
    """Pub/Sub wakeup stream: claim the named unit, ack after commit."""
    from distributed_runtime.gcp.pubsub import PubSubConfig, PubSubTransport

    transport = PubSubTransport(
        PubSubConfig(
            project=config.project,
            topic=config.dispatch_topic,
            subscription=subscription,
        )
    )
    async for envelope, message in transport.stream():
        try:
            handled = await worker.handle_envelope(envelope)
        except Exception:
            logger.exception("envelope handling failed — nacking")
            message.nack()
            continue
        if handled is not None:
            logger.info(
                "unit %s run=%s outcome=%s",
                handled.unit_key,
                handled.run_id,
                handled.outcome,
            )
        message.ack()
    raise RuntimeError("dispatch stream ended unexpectedly")


async def run_control(
    config: DeployConfig,
    engine: StateEngine,
    *,
    finite_registry: RuntimeRegistry | None,
    continuous_registry: RuntimeRegistry | None,
) -> None:
    """Control plane: planner + outbox dispatcher + continuous reconciler."""
    tasks: list[asyncio.Task[None]] = []
    finite_store = FiniteStateStore(engine)

    if finite_registry is not None:
        planner = PlannerRunner(
            registry=finite_registry,
            engine=engine,
            dispatch_topic=config.dispatch_topic,
            application=str(finite_registry.application),
        )

        async def plan_pending() -> int:
            runs = await finite_store.list_pending_runs()
            for run in runs:
                outcome = await planner.plan(run.run_id)
                logger.info("planned run %s: %d units", outcome.run_id, outcome.unit_count)
            return len(runs)

        tasks.append(asyncio.create_task(_forever("planner", config.tick_seconds, plan_pending)))

    if config.project:
        dispatcher = OutboxDispatcher(
            engine,
            application=(
                str(finite_registry.application) if finite_registry is not None else "runtime"
            ),
            dispatch_topic=config.dispatch_topic,
        )
        send = _pubsub_sender(config)
        tasks.append(
            asyncio.create_task(_forever("dispatcher", 1.0, lambda: dispatcher.cycle(send)))
        )

    if continuous_registry is not None and config.reconciler_mode == "loop":

        async def reconcile_active() -> int:
            return await reconcile_deployments(config, engine, continuous_registry)

        tasks.append(
            asyncio.create_task(_forever("reconciler", config.tick_seconds, reconcile_active))
        )

    if not tasks:
        raise RuntimeError("control role needs at least one app or a project")
    await asyncio.gather(*tasks)


async def reconcile_deployments(
    config: DeployConfig,
    engine: StateEngine,
    continuous_registry: RuntimeRegistry,
) -> int:
    """One convergence pass over every ACTIVE deployment.

    Shared by the control loop (``reconciler_mode=loop``) and the one-shot
    ``reconcile`` role that Cloud Scheduler invokes as a job.
    """
    reconciler = Reconciler(
        engine,
        lease_seconds=config.lease_seconds,
        stale_worker_seconds=config.stale_worker_seconds,
    )
    continuous_store = ContinuousStateStore(engine)
    served = {
        str(registration.name): registration
        for registration in continuous_registry.workloads()
        if isinstance(registration, ContinuousRegistration)
    }
    deployments = await continuous_store.list_deployments([DeploymentStatus.ACTIVE])
    count = 0
    for deployment in deployments:
        registration = served.get(str(deployment.workload_id))
        if registration is None:
            continue
        desired = await discover_partitions(registration.workload)
        result = await reconciler.reconcile(deployment.deployment_id, desired)
        count += 1
        if result.assigned or result.reclaimed or result.dead_workers:
            logger.info(
                "reconcile %s: assigned=%d reclaimed=%d dead=%d",
                deployment.deployment_id,
                result.assigned,
                result.reclaimed,
                result.dead_workers,
            )
    return count


async def reconcile_once(
    config: DeployConfig,
    engine: StateEngine,
    *,
    continuous_registry: RuntimeRegistry | None,
) -> int:
    """One-shot reconcile for the ``reconcile`` job role — runs once, exits."""
    if continuous_registry is None:
        raise RuntimeError("reconcile role needs RUNTIME_CONTINUOUS_APP")
    return await reconcile_deployments(config, engine, continuous_registry)


def _pubsub_sender(
    config: DeployConfig,
) -> Callable[[OutboxMessage], Awaitable[None]]:
    """Route outbox rows to Pub/Sub topics by kind."""
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()

    async def send(message: OutboxMessage) -> None:
        if message.kind == "unit_dispatch":
            topic_path = publisher.topic_path(config.project, message.destination)
        elif message.kind in {"emission", "checkpoint"}:
            topic_path = publisher.topic_path(config.project, config.events_topic)
        else:
            raise ValueError(f"unknown outbox kind: {message.kind}")
        future = publisher.publish(
            topic_path,
            message.envelope.to_json(),
            ordering_key="",
            message_kind=message.envelope.message_kind,
            workload=message.envelope.workload,
            idempotency_key=message.envelope.idempotency_key,
            runtime_pool_revision=message.envelope.runtime_pool_revision,
        )
        await asyncio.to_thread(future.result, 30)

    return send
