"""UT-CONFIG: typed policies and connection-capacity safety."""

from __future__ import annotations

import pytest

from distributed_runtime.core.config import (
    ConfigurationError,
    ConnectionCapacity,
    ContinuousPolicy,
    DatabaseCapacity,
    ExecutionClassConfig,
    FinitePolicy,
    RuntimePoolConfig,
    SecretReference,
)
from distributed_runtime.core.enums import WorkloadMode


def finite_pool(
    *,
    max_replicas: int = 20,
    connections_per_replica: int = 2,
) -> RuntimePoolConfig:
    return RuntimePoolConfig(
        mode=WorkloadMode.FINITE,
        min_replicas=0,
        max_replicas=max_replicas,
        connections_per_replica=connections_per_replica,
        target_messages_per_instance=5,
    )


def continuous_pool(
    *,
    max_replicas: int = 10,
    connections_per_replica: int = 2,
) -> RuntimePoolConfig:
    return RuntimePoolConfig(
        mode=WorkloadMode.CONTINUOUS,
        min_replicas=2,
        max_replicas=max_replicas,
        connections_per_replica=connections_per_replica,
        target_cpu_utilization=0.60,
    )


def test_documented_policy_defaults_are_explicit() -> None:
    finite = FinitePolicy()
    continuous = ContinuousPolicy()

    assert finite.timeout_seconds == 300
    assert finite.max_attempts == 5
    assert finite.claim_ttl_seconds == 420
    assert continuous.heartbeat_seconds == 15
    assert continuous.lease_seconds == 60
    assert continuous.renew_before_expiry_seconds == 30
    assert continuous.drain_seconds == 120
    assert continuous.takeover_seconds == 150


def test_finite_policy_invalid_combinations_raise_at_construction() -> None:
    with pytest.raises(ConfigurationError, match="max_timeout_seconds"):
        FinitePolicy(timeout_seconds=3_601)
    with pytest.raises(ConfigurationError, match="retry_cap_seconds"):
        FinitePolicy(retry_base_seconds=301)


@pytest.mark.parametrize(
    "overrides",
    [
        {"heartbeat_seconds": 31},
        {"renew_before_expiry_seconds": 60},
        {"takeover_seconds": 59},
    ],
)
def test_continuous_policy_rejects_unsafe_timing(
    overrides: dict[str, int],
) -> None:
    with pytest.raises(ConfigurationError):
        ContinuousPolicy(**overrides)


def test_pool_modes_require_their_own_scaling_signal() -> None:
    with pytest.raises(ConfigurationError, match="target_messages"):
        RuntimePoolConfig(
            mode=WorkloadMode.FINITE,
            min_replicas=0,
            max_replicas=1,
            connections_per_replica=1,
        )
    with pytest.raises(ConfigurationError, match="at least two"):
        RuntimePoolConfig(
            mode=WorkloadMode.CONTINUOUS,
            min_replicas=1,
            max_replicas=2,
            connections_per_replica=1,
            target_cpu_utilization=0.6,
        )
    with pytest.raises(ConfigurationError, match="mode"):
        RuntimePoolConfig(
            mode="other",  # type: ignore[arg-type]
            min_replicas=2,
            max_replicas=2,
            connections_per_replica=1,
            target_cpu_utilization=0.6,
        )


@pytest.mark.parametrize("value", [True, 1.0, 1.5])
def test_integer_fields_reject_booleans_and_fractions(value: object) -> None:
    with pytest.raises(ConfigurationError, match="positive integer"):
        FinitePolicy(timeout_seconds=value)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError, match="positive integer"):
        finite_pool(max_replicas=value)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError, match="positive integer"):
        DatabaseCapacity(max_connections=value)  # type: ignore[arg-type]


@pytest.mark.parametrize("version", ["latest", "", "0", "-1", "v1"])
def test_secret_reference_requires_an_explicit_positive_version(version: str) -> None:
    with pytest.raises(ConfigurationError, match="explicit positive version"):
        SecretReference(project_id="runtime-project", secret_id="database", version=version)
    reference = SecretReference("runtime-project", "database", "7")
    assert reference.resource_name.endswith("/versions/7")


def test_execution_class_and_database_capacity_validate_boundaries() -> None:
    with pytest.raises(ConfigurationError, match="runtime_pool"):
        ExecutionClassConfig(runtime_pool="")
    with pytest.raises(ConfigurationError, match="queue"):
        ExecutionClassConfig(runtime_pool="finite", queue=" bad")
    with pytest.raises(ConfigurationError, match="less than"):
        DatabaseCapacity(max_connections=10, reserved_connections=10)
    with pytest.raises(ConfigurationError, match=r"at most 0\.70"):
        DatabaseCapacity(max_connections=10, utilization_limit=0.8)
    database = DatabaseCapacity(max_connections=100, reserved_connections=10)
    assert database.runtime_budget == 62


def test_connection_capacity_reports_safe_and_unsafe_demand() -> None:
    safe = ConnectionCapacity(budget=10, fixed_demand=4, pool_demand=6)
    assert safe.total_demand == 10
    assert safe.remaining == 0
    assert safe.is_safe
    unsafe = ConnectionCapacity(budget=9, fixed_demand=4, pool_demand=6)
    assert unsafe.remaining == -1
    assert not unsafe.is_safe
