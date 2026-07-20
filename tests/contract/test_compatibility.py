"""CT-COMPAT: committed snapshots for compatibility-sensitive contracts."""

from __future__ import annotations

import inspect
import json
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pytest

import distributed_runtime
import distributed_runtime.continuous as continuous
import distributed_runtime.core as core
import distributed_runtime.finite as finite
from distributed_runtime.core import (
    ContinuousPolicy,
    DatabaseCapacity,
    ExecutionClassConfig,
    FinitePolicy,
    RuntimeConfig,
    RuntimePoolConfig,
    UnsupportedVersionError,
    VersionedEnvelope,
    WorkloadMode,
)
from distributed_runtime.registry import RuntimeRegistry

_SNAPSHOT_DIRECTORY = Path(__file__).with_name("snapshots")


def _snapshot(name: str) -> dict[str, Any]:
    value = json.loads((_SNAPSHOT_DIRECTORY / f"{name}.json").read_text())
    assert isinstance(value, dict)
    return value


def _parameters(callable_object: Callable[..., object]) -> list[dict[str, object]]:
    return [
        {
            "name": parameter.name,
            "kind": parameter.kind.name,
            "required": parameter.default is inspect.Parameter.empty,
        }
        for parameter in inspect.signature(callable_object).parameters.values()
    ]


def test_public_api_snapshot() -> None:
    methods = (
        "register_continuous",
        "register_finite",
        "register_sink",
        "sink",
        "workload",
        "workloads",
    )
    actual = {
        "root_exports": distributed_runtime.__all__,
        "core_exports": core.__all__,
        "finite_exports": finite.__all__,
        "continuous_exports": continuous.__all__,
        "registry_methods": {name: _parameters(getattr(RuntimeRegistry, name)) for name in methods},
    }

    assert actual == _snapshot("api")


def test_envelope_snapshot() -> None:
    envelope = VersionedEnvelope(
        schema_version=1,
        message_kind="finite.execute",
        run_id="run:1",
        execution_id="execution:1",
        idempotency_key="execution:1",
        application="example",
        workload="snapshot",
        workload_version="1.0.0",
        handler="snapshot",
        attempt_generation=2,
        execution_class="default",
        runtime_pool_revision="finite:v2",
        published_at="2026-07-20T04:00:00Z",
        payload={
            "nested": {"answer": 42},
            "items": [1, "two", None],
            "enabled": True,
        },
        trace_context={"traceparent": "trace-7"},
    )
    with pytest.raises(UnsupportedVersionError) as caught:
        VersionedEnvelope.from_dict(
            {
                "schema_version": 2,
                "message_kind": "future",
                "payload": {},
            }
        )

    actual = {
        "mapping": envelope.to_dict(),
        "canonical_json": envelope.to_json().decode(),
        "unsupported_version_error": caught.value.as_dict(),
    }
    assert actual == _snapshot("envelope")
    assert VersionedEnvelope.from_json(envelope.to_json()) == envelope


def test_configuration_snapshot() -> None:
    finite_policy = FinitePolicy()
    continuous_policy = ContinuousPolicy()
    finite_pool = RuntimePoolConfig(
        mode=WorkloadMode.FINITE,
        min_replicas=0,
        max_replicas=5,
        connections_per_replica=4,
        target_messages_per_instance=20,
    )
    continuous_pool = RuntimePoolConfig(
        mode=WorkloadMode.CONTINUOUS,
        min_replicas=2,
        max_replicas=4,
        connections_per_replica=5,
        target_cpu_utilization=0.65,
    )
    database = DatabaseCapacity(
        max_connections=100,
        reserved_connections=20,
        fixed_service_connections=6,
    )
    config = RuntimeConfig(
        runtime_pools={
            "finite": finite_pool,
            "continuous": continuous_pool,
        },
        execution_classes={
            "default": ExecutionClassConfig(
                runtime_pool="finite",
                queue="finite-default",
            ),
            "stateful": ExecutionClassConfig(runtime_pool="continuous"),
        },
        database=database,
        finite=finite_policy,
        continuous=continuous_policy,
    )
    capacity = config.connection_capacity()

    actual = {
        "finite_defaults": {
            **asdict(finite_policy),
            "claim_ttl_seconds": finite_policy.claim_ttl_seconds,
        },
        "continuous_defaults": asdict(continuous_policy),
        "runtime_pools": {
            name: {
                **asdict(pool),
                "mode": pool.mode.value,
                "worst_case_connections": pool.worst_case_connections,
            }
            for name, pool in config.runtime_pools.items()
        },
        "execution_classes": {
            name: asdict(execution_class)
            for name, execution_class in config.execution_classes.items()
        },
        "database": {
            **asdict(database),
            "runtime_budget": database.runtime_budget,
        },
        "capacity": {
            **asdict(capacity),
            "total_demand": capacity.total_demand,
            "remaining": capacity.remaining,
            "is_safe": capacity.is_safe,
        },
    }

    assert actual == _snapshot("config")
