"""DeployConfig env parsing and app-spec loading (no GCP required)."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping

import pytest

from distributed_runtime import RuntimeApplication
from distributed_runtime.core.enums import WorkloadMode
from distributed_runtime.core.envelope import JsonValue
from distributed_runtime.deploy.config import (
    DeployConfig,
    load_registry,
    resolve_instance_id,
)
from distributed_runtime.finite import (
    ExecutionContext,
    ExecutionResult,
    ExecutionUnit,
    WorkloadRequest,
)


async def _empty_plan() -> AsyncIterator[ExecutionUnit]:
    return
    yield


class _Planner:
    name = "t.w"
    version = "1.0.0"
    mode = WorkloadMode.FINITE

    def __call__(self, request: WorkloadRequest) -> AsyncIterator[ExecutionUnit]:
        return _empty_plan()


async def _handler(context: ExecutionContext, payload: Mapping[str, JsonValue]) -> ExecutionResult:
    return {"ok": 1}


def _build_app() -> RuntimeApplication:
    app = RuntimeApplication("test-app")
    app.registry.register_finite(
        name="t.w",
        semantic_version="1.0.0",
        execution_class="lightweight",
        planner=_Planner(),
        handler=_handler,
    )
    return app


def test_dsn_override_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNTIME_DSN", "postgresql://u:p@h:1/db")
    monkeypatch.delenv("RUNTIME_DB_PASSWORD", raising=False)
    monkeypatch.delenv("RUNTIME_DB_PASSWORD_SECRET", raising=False)
    config = DeployConfig.from_env()
    assert config.dsn == "postgresql://u:p@h:1/db"
    assert config.pool_revision == "local"
    assert config.lease_seconds == 60


def test_composed_dsn_with_inline_password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUNTIME_DSN", raising=False)
    monkeypatch.setenv("RUNTIME_DB_HOST", "10.0.0.5")
    monkeypatch.setenv("RUNTIME_DB_PASSWORD", "pw")
    config = DeployConfig.from_env()
    assert config.dsn == "postgresql://runtime:pw@10.0.0.5:5432/runtime"


def test_missing_credentials_is_loud(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUNTIME_DSN", raising=False)
    monkeypatch.setenv("RUNTIME_DB_HOST", "h")
    monkeypatch.delenv("RUNTIME_DB_PASSWORD", raising=False)
    monkeypatch.delenv("RUNTIME_DB_PASSWORD_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="RUNTIME_DB_PASSWORD"):
        DeployConfig.from_env()


def test_load_registry_imports_factory() -> None:
    registry = load_registry("tests.unit.test_deploy_config:_build_app")
    assert registry is not None
    assert str(registry.application) == "test-app"
    assert len(registry.workloads()) == 1


def test_load_registry_rejects_bad_spec() -> None:
    with pytest.raises(RuntimeError, match="invalid app spec"):
        load_registry("no-colon-separator")


def test_resolve_instance_id_prefers_env() -> None:
    assert resolve_instance_id("worker-7") == "worker-7"
    assert resolve_instance_id(None)  # hostname or metadata — always non-empty
