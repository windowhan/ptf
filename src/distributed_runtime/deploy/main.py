"""Process entrypoint: ``python -m distributed_runtime <role>``.

Roles:
  migrate  — apply pending schema migrations, then exit
  worker   — finite claims + continuous supervision + heartbeat (MIG)
  control  — planner + outbox dispatcher + reconciler (Cloud Run)

When ``PORT`` is set (Cloud Run injects it) a minimal HTTP responder binds
it so the platform's health check passes — the real work is the loops.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from distributed_runtime.deploy.config import DeployConfig, load_registry
from distributed_runtime.state.engine import StateEngine
from distributed_runtime.state.migrations import migrate

logger = logging.getLogger("distributed_runtime.deploy")


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *_args: object) -> None:
        return


def _serve_health_port() -> None:
    port = os.environ.get("PORT")
    if not port:
        return
    server = ThreadingHTTPServer(("0.0.0.0", int(port)), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("health responder bound on :%s", port)


async def _run(role: str) -> int:
    config = DeployConfig.from_env()
    engine = await StateEngine.connect(config.dsn)
    try:
        if role == "migrate":
            applied = await migrate(engine)
            logger.info("applied %d migration(s)", len(applied))
            return 0

        finite_registry = load_registry(os.environ.get("RUNTIME_FINITE_APP"))
        continuous_registry = load_registry(os.environ.get("RUNTIME_CONTINUOUS_APP"))

        stop = asyncio.Event()
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)

        async def _role() -> None:
            if role == "worker":
                from distributed_runtime.deploy.service import run_worker

                await run_worker(
                    config,
                    engine,
                    finite_registry=finite_registry,
                    continuous_registry=continuous_registry,
                )
            elif role == "control":
                from distributed_runtime.deploy.service import run_control

                await run_control(
                    config,
                    engine,
                    finite_registry=finite_registry,
                    continuous_registry=continuous_registry,
                )
            else:
                raise RuntimeError(f"unknown role: {role}")

        work = asyncio.create_task(_role())
        waiter = asyncio.create_task(stop.wait())
        done, _ = await asyncio.wait({work, waiter}, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if exception := task.exception():
                raise exception
        work.cancel()
        await asyncio.gather(work, return_exceptions=True)
        logger.info("shutdown complete")
        return 0
    finally:
        await engine.close()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if len(sys.argv) != 2 or sys.argv[1] not in {"migrate", "worker", "control"}:
        print("usage: python -m distributed_runtime {migrate|worker|control}")
        return 2
    _serve_health_port()
    return asyncio.run(_run(sys.argv[1]))


if __name__ == "__main__":
    sys.exit(main())
