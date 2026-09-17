"""UT-API: client/admin JSON surfaces — route logic and real HTTP serving."""

from __future__ import annotations

import asyncio
import json
import threading
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from urllib.error import HTTPError

import pytest

from distributed_runtime.control.admin import DeploymentView
from distributed_runtime.control.client import SubmittedRun
from distributed_runtime.core.enums import (
    DeploymentStatus,
    PartitionStatus,
    RunStatus,
)
from distributed_runtime.core.identifiers import (
    DeploymentId,
    PartitionId,
    RevisionId,
    RunId,
    RuntimeInstanceId,
    WorkloadId,
)
from distributed_runtime.deploy.api import ApiError, admin_routes, client_routes, serve
from distributed_runtime.state.continuous import StoredDeployment, StoredPartition
from distributed_runtime.state.finite import StoredRun


def _stored_run(run_id: str = "run:abc") -> StoredRun:
    return StoredRun(
        run_id=RunId(run_id),
        workload_id=WorkloadId("app.range.sum"),
        workload_name="range.sum",
        workload_version="1.0.0",
        planner_revision=RevisionId("rev:1"),
        execution_revision=RevisionId("rev:1"),
        status=RunStatus.SUCCEEDED,
        input={"start": 0, "end": 5},
        planning_generation=1,
    )


@dataclass
class FakeClient:
    submitted: list[dict[str, Any]]

    async def submit(self, **kwargs: Any) -> SubmittedRun:
        self.submitted.append(kwargs)
        return SubmittedRun(run_id=RunId("run:new"), status=RunStatus.PENDING)

    async def get(self, run_id: RunId) -> StoredRun | None:
        return _stored_run(str(run_id)) if str(run_id) == "run:abc" else None

    async def results(self, run_id: RunId) -> tuple[Any, ...]:
        return ({"subtotal": 10},)

    async def cancel(self, run_id: RunId) -> None:
        return None


@dataclass
class FakeAdmin:
    drained: list[str]

    async def deploy(self, **kwargs: Any) -> StoredDeployment:
        return StoredDeployment(
            deployment_id=DeploymentId("deployment:x"),
            workload_id=WorkloadId(str(kwargs["workload"])),
            workload_version=str(kwargs["version"]),
            status=DeploymentStatus.ACTIVE,
            config=dict(kwargs.get("config") or {}),
        )

    async def drain(self, deployment_id: DeploymentId) -> None:
        self.drained.append(str(deployment_id))

    async def stop(self, deployment_id: DeploymentId) -> None:
        self.drained.append(f"stop:{deployment_id}")

    async def view(self, deployment_id: DeploymentId) -> DeploymentView:
        return DeploymentView(
            deployment=StoredDeployment(
                deployment_id=deployment_id,
                workload_id=WorkloadId("stream.pulse"),
                workload_version="1.0.0",
                status=DeploymentStatus.ACTIVE,
                config={},
            ),
            partitions=(
                StoredPartition(
                    deployment_id=deployment_id,
                    partition_id=PartitionId("shard:0"),
                    status=PartitionStatus.RUNNING,
                    weight=1,
                    payload={},
                    owner_id=RuntimeInstanceId("w-0"),
                    fencing_token=3,
                    lease_expires_at=datetime.now(UTC),
                ),
            ),
        )


def _call(route: Any, method: str, path: str, body: Any = None) -> tuple[int, Any]:
    segments = tuple(s for s in path.split("/") if s)
    return asyncio.run(route(method, segments, body))


def test_client_submit() -> None:
    client = FakeClient(submitted=[])
    status, body = _call(
        client_routes(cast(Any, client)),
        "POST",
        "/v1/runs",
        {"workload": "range.sum", "version": "1.0.0", "input": {"start": 0, "end": 5}},
    )
    assert status == 201
    assert body["run_id"] == "run:new"
    assert client.submitted[0]["workload"] == "range.sum"


def test_client_get_and_results() -> None:
    route = client_routes(cast(Any, FakeClient(submitted=[])))
    status, body = _call(route, "GET", "/v1/runs/run:abc")
    assert status == 200 and body["status"] == "succeeded"
    status, body = _call(route, "GET", "/v1/runs/run:abc/results")
    assert status == 200 and body["results"] == [{"subtotal": 10}]
    status, _ = _call(route, "POST", "/v1/runs/run:abc/cancel")
    assert status == 202


def test_client_unknown_run_404() -> None:
    with pytest.raises(ApiError) as error:
        _call(client_routes(cast(Any, FakeClient(submitted=[]))), "GET", "/v1/runs/run:nope")
    assert error.value.status == 404


def test_client_submit_requires_fields() -> None:
    with pytest.raises(ApiError) as error:
        _call(
            client_routes(cast(Any, FakeClient(submitted=[]))),
            "POST",
            "/v1/runs",
            {"workload": "w"},
        )
    assert error.value.status == 400


def test_admin_deploy_drain_view() -> None:
    admin = FakeAdmin(drained=[])
    route = admin_routes(cast(Any, admin))
    status, body = _call(
        route, "POST", "/v1/deployments", {"workload": "stream.pulse", "version": "1.0.0"}
    )
    assert status == 201 and body["status"] == "active"
    status, body = _call(route, "GET", "/v1/deployments/deployment:x")
    assert status == 200 and body["partitions"][0]["fencing_token"] == 3
    status, _ = _call(route, "POST", "/v1/deployments/deployment:x/drain")
    assert status == 202 and admin.drained == ["deployment:x"]
    status, _ = _call(route, "POST", "/v1/deployments/deployment:x/stop")
    assert status == 202


def test_http_serving_round_trip() -> None:
    """Real socket: handler threads dispatch coroutines onto the main loop."""
    loop = asyncio.new_event_loop()
    ready = threading.Event()

    def _run_loop() -> None:
        asyncio.set_event_loop(loop)
        ready.set()
        loop.run_forever()

    thread = threading.Thread(target=_run_loop, daemon=True)
    thread.start()
    ready.wait(timeout=5)
    server = serve(loop, client_routes(cast(Any, FakeClient(submitted=[]))), port=0)
    try:
        request = urllib.request.Request(
            f"http://127.0.0.1:{server.port}/v1/runs",
            data=json.dumps({"workload": "range.sum", "version": "1.0.0", "input": {}}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            assert response.status == 201
            assert json.loads(response.read())["run_id"] == "run:new"

        with urllib.request.urlopen(
            f"http://127.0.0.1:{server.port}/healthz", timeout=10
        ) as response:
            assert response.status == 200

        with pytest.raises(HTTPError) as error:
            urllib.request.urlopen(
                f"http://127.0.0.1:{server.port}/v1/runs/run:missing", timeout=10
            )
        assert error.value.code == 404
    finally:
        server.close()
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)


def _post(port: int, path: str, raw: bytes | None) -> HTTPError | int:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=raw,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status
    except HTTPError as error:
        return error


def _serve_on_loop(route: Any) -> tuple[Any, Any, threading.Thread]:
    loop = asyncio.new_event_loop()
    ready = threading.Event()

    def _run_loop() -> None:
        asyncio.set_event_loop(loop)
        ready.set()
        loop.run_forever()

    thread = threading.Thread(target=_run_loop, daemon=True)
    thread.start()
    ready.wait(timeout=5)
    return loop, serve(loop, route, port=0), thread


def test_http_error_paths() -> None:
    loop, server, thread = _serve_on_loop(client_routes(cast(Any, FakeClient(submitted=[]))))
    try:
        # malformed JSON body → 400
        error = _post(server.port, "/v1/runs", b"{not json")
        assert isinstance(error, HTTPError) and error.code == 400
        # non-object JSON body → 400
        error = _post(server.port, "/v1/runs", b"[1,2]")
        assert isinstance(error, HTTPError) and error.code == 400
        # invalid run_id format → 400 (identifier validation)
        error = _post(server.port, "/v1/runs/bad%20id/cancel", b"{}")
        assert isinstance(error, HTTPError) and error.code == 400
        # unknown route → 404
        error = _post(server.port, "/v9/nothing", b"{}")
        assert isinstance(error, HTTPError) and error.code == 404
    finally:
        server.close()
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
