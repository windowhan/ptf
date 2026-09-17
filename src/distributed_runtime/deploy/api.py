"""JSON HTTP surfaces for the ``api`` (client) and ``admin`` roles.

Two separate Cloud Run services serve these: the client surface submits and
reads finite runs, the admin surface manages continuous deployments.
Authorization is enforced by Cloud Run IAM on each service — the code itself
stays a thin adapter over :class:`RuntimeClient` and :class:`ContinuousAdmin`.

Transport is stdlib ``ThreadingHTTPServer`` so no web framework dependency
is added; handler threads bounce coroutines onto the main event loop with
``asyncio.run_coroutine_threadsafe``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from distributed_runtime.control.admin import ContinuousAdmin
from distributed_runtime.control.client import RuntimeClient
from distributed_runtime.core.envelope import JsonValue
from distributed_runtime.core.errors import (
    InvalidIdentifierError,
    InvariantViolationError,
)
from distributed_runtime.core.identifiers import DeploymentId, RevisionId, RunId
from distributed_runtime.state.continuous import StoredDeployment, StoredPartition
from distributed_runtime.state.finite import StoredRun

logger = logging.getLogger("distributed_runtime.deploy")

JsonObject = Mapping[str, JsonValue]
Route = Callable[[str, tuple[str, ...], JsonObject | None], Coroutine[Any, Any, tuple[int, Any]]]


class ApiError(Exception):
    """HTTP-translated failure: status code plus a JSON error body."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


def _run_view(run: StoredRun) -> dict[str, Any]:
    return {
        "run_id": str(run.run_id),
        "workload_id": str(run.workload_id),
        "workload_name": run.workload_name,
        "workload_version": run.workload_version,
        "planner_revision": str(run.planner_revision),
        "execution_revision": str(run.execution_revision),
        "status": run.status.value,
        "input": dict(run.input),
        "planning_generation": run.planning_generation,
    }


def _deployment_view(
    deployment: StoredDeployment, partitions: tuple[StoredPartition, ...]
) -> dict[str, Any]:
    return {
        "deployment_id": str(deployment.deployment_id),
        "workload_id": str(deployment.workload_id),
        "workload_version": deployment.workload_version,
        "status": deployment.status.value,
        "config": dict(deployment.config),
        "partitions": [
            {
                "partition_id": str(p.partition_id),
                "status": p.status.value,
                "weight": p.weight,
                "owner_id": None if p.owner_id is None else str(p.owner_id),
                "fencing_token": p.fencing_token,
            }
            for p in partitions
        ],
    }


def _require_object(body: JsonObject | None) -> JsonObject:
    if body is None:
        raise ApiError(400, "request body must be a JSON object")
    return body


def _require_string(body: JsonObject, field: str) -> str:
    value = body.get(field)
    if not isinstance(value, str) or not value:
        raise ApiError(400, f"{field} must be a non-empty string")
    return value


def client_routes(client: RuntimeClient) -> Route:
    """Finite run surface: submit, inspect, list results, cancel."""

    async def route(
        method: str, segments: tuple[str, ...], body: JsonObject | None
    ) -> tuple[int, Any]:
        if segments == ("v1", "runs") and method == "POST":
            payload = _require_object(body)
            run_id_raw = payload.get("run_id")
            submitted = await client.submit(
                workload=_require_string(payload, "workload"),
                version=_require_string(payload, "version"),
                input=_input(payload),
                run_id=None if run_id_raw is None else RunId(_as_string(run_id_raw, "run_id")),
            )
            return 201, {"run_id": str(submitted.run_id), "status": submitted.status.value}

        if len(segments) >= 3 and segments[:2] == ("v1", "runs"):
            run_id = RunId(segments[2])
            if len(segments) == 3 and method == "GET":
                run = await client.get(run_id)
                if run is None:
                    raise ApiError(404, f"unknown run: {run_id}")
                return 200, _run_view(run)
            if len(segments) == 4 and segments[3] == "results" and method == "GET":
                return 200, {"results": list(await client.results(run_id))}
            if len(segments) == 4 and segments[3] == "cancel" and method == "POST":
                await client.cancel(run_id)
                return 202, {"run_id": str(run_id), "status": "cancelled"}
        raise ApiError(404, "no such route")

    return route


def _input(payload: JsonObject) -> JsonObject:
    value = payload.get("input")
    if not isinstance(value, Mapping):
        raise ApiError(400, "input must be a JSON object")
    return value


def _as_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ApiError(400, f"{field} must be a non-empty string")
    return value


def admin_routes(admin: ContinuousAdmin) -> Route:
    """Deployment lifecycle surface: deploy, inspect, drain, stop."""

    async def route(
        method: str, segments: tuple[str, ...], body: JsonObject | None
    ) -> tuple[int, Any]:
        if segments == ("v1", "deployments") and method == "POST":
            payload = _require_object(body)
            config = payload.get("config")
            deployment = await admin.deploy(
                workload=_require_string(payload, "workload"),
                version=_require_string(payload, "version"),
                config=None if config is None else _as_mapping(config),
            )
            return 201, _deployment_view(deployment, ())

        if len(segments) >= 3 and segments[:2] == ("v1", "deployments"):
            deployment_id = DeploymentId(segments[2])
            if len(segments) == 3 and method == "GET":
                view = await admin.view(deployment_id)
                return 200, _deployment_view(view.deployment, view.partitions)
            if len(segments) == 4 and segments[3] == "drain" and method == "POST":
                await admin.drain(deployment_id)
                return 202, {"deployment_id": str(deployment_id), "status": "draining"}
            if len(segments) == 4 and segments[3] == "stop" and method == "POST":
                await admin.stop(deployment_id)
                return 202, {"deployment_id": str(deployment_id), "status": "stopped"}
        raise ApiError(404, "no such route")

    return route


def _as_mapping(value: object) -> JsonObject:
    if not isinstance(value, Mapping):
        raise ApiError(400, "config must be a JSON object")
    return value


@dataclass(slots=True)
class ApiServer:
    """Running HTTP server bound to a route table on the main loop."""

    httpd: ThreadingHTTPServer
    port: int

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def serve(loop: asyncio.AbstractEventLoop, route: Route, *, port: int) -> ApiServer:
    """Bind ``port`` and serve ``route`` on a daemon thread."""

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._handle("GET")

        def do_POST(self) -> None:
            self._handle("POST")

        def log_message(self, *_args: object) -> None:
            return

        def _read_body(self) -> JsonObject | None:
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return None
            raw = self.rfile.read(length)
            try:
                decoded = json.loads(raw)
            except ValueError:
                raise ApiError(400, "request body is not valid JSON") from None
            if not isinstance(decoded, dict):
                raise ApiError(400, "request body must be a JSON object")
            return decoded

        def _handle(self, method: str) -> None:
            try:
                path = self.path.split("?", 1)[0]
                if method == "GET" and path in {"/", "/healthz"}:
                    status, payload = 200, {"status": "ok"}
                    self._respond(status, payload)
                    return
                segments = tuple(s for s in path.split("/") if s)
                body = self._read_body()
                future = asyncio.run_coroutine_threadsafe(
                    route(method, segments, body),
                    self.server.loop,  # type: ignore[attr-defined]
                )
                status, payload = future.result(timeout=30)
            except ApiError as exc:
                status, payload = exc.status, {"error": exc.message}
            except LookupError as exc:
                status, payload = 404, {"error": str(exc)}
            except InvariantViolationError as exc:
                status, payload = 409, {"error": str(exc)}
            except (InvalidIdentifierError, ValueError, TypeError) as exc:
                status, payload = 400, {"error": str(exc)}
            except Exception as exc:
                logger.exception("api request failed")
                status, payload = 500, {"error": str(exc)}
            self._respond(status, payload)

        def _respond(self, status: int, payload: Any) -> None:
            data = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    httpd = ThreadingHTTPServer(("0.0.0.0", port), _Handler)
    httpd.loop = loop  # type: ignore[attr-defined]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    bound = int(httpd.server_address[1])
    logger.info("api responder bound on :%d", bound)
    return ApiServer(httpd=httpd, port=bound)


def revisions_from_env() -> tuple[RevisionId, RevisionId]:
    """Revisions the client API stamps on submissions — env or pool default."""
    import os

    pool = os.environ.get("RUNTIME_POOL_REVISION", "local")
    planner = RevisionId(os.environ.get("RUNTIME_PLANNER_REVISION") or pool)
    execution = RevisionId(os.environ.get("RUNTIME_EXECUTION_REVISION") or pool)
    return planner, execution
