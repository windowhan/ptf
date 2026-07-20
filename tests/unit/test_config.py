"""UT-CONFIG: typed policies and connection-capacity safety."""

from __future__ import annotations

import pytest

from distributed_runtime.core.config import (
    ConfigurationError,
    ContinuousPolicy,
    DatabaseCapacity,
    ExecutionClassConfig,
    FinitePolicy,
    RuntimeConfig,
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


def test_execution_class_queue_rules_follow_pool_mode() -> None:
    database = DatabaseCapacity(max_connections=100)
    with pytest.raises(ConfigurationError, match=r"finite.*requires.*queue/subscription"):
        RuntimeConfig(
            runtime_pools={"finite": finite_pool(max_replicas=1)},
            execution_classes={"default": ExecutionClassConfig(runtime_pool="finite")},
            database=database,
        )
    with pytest.raises(
        ConfigurationError,
        match=r"continuous.*must not set.*queue/subscription",
    ):
        RuntimeConfig(
            runtime_pools={"continuous": continuous_pool(max_replicas=2)},
            execution_classes={
                "stream": ExecutionClassConfig(
                    runtime_pool="continuous",
                    queue="finite-only",
                )
            },
            database=database,
        )


@pytest.mark.parametrize("version", ["latest", "", "0", "-1", "v1"])
def test_secret_reference_requires_an_explicit_positive_version(version: str) -> None:
    with pytest.raises(ConfigurationError, match="explicit positive version"):
        SecretReference(project_id="runtime-project", secret_id="database", version=version)

    reference = SecretReference(
        project_id="runtime-project",
        secret_id="database",
        version="7",
    )
    assert reference.resource_name == ("projects/runtime-project/secrets/database/versions/7")


def test_connection_capacity_accepts_demand_at_budget_boundary() -> None:
    config = RuntimeConfig(
        runtime_pools={
            "finite": finite_pool(max_replicas=10, connections_per_replica=2),
            "continuous": continuous_pool(
                max_replicas=5,
                connections_per_replica=2,
            ),
        },
        execution_classes={
            "browser": ExecutionClassConfig(
                runtime_pool="finite",
                queue="runtime-browser",
            ),
            "stream": ExecutionClassConfig(runtime_pool="continuous"),
        },
        database=DatabaseCapacity(
            max_connections=100,
            reserved_connections=10,
            fixed_service_connections=32,
        ),
    )

    capacity = config.connection_capacity()

    assert capacity.budget == 62
    assert capacity.pool_demand == 30
    assert capacity.fixed_demand == 32
    assert capacity.total_demand == 62
    assert capacity.remaining == 0


def test_connection_capacity_fails_closed_above_budget() -> None:
    with pytest.raises(ConfigurationError, match="exceeds safe budget") as raised:
        RuntimeConfig(
            runtime_pools={"finite": finite_pool()},
            execution_classes={},
            database=DatabaseCapacity(
                max_connections=100,
                reserved_connections=10,
                fixed_service_connections=23,
            ),
        )

    assert raised.value.details == {
        "budget": 62,
        "total_demand": 63,
        "over_capacity_by": 1,
    }


def test_connection_capacity_report_for_safe_configuration() -> None:
    config = RuntimeConfig(
        runtime_pools={
            "finite": finite_pool(),
            "continuous": continuous_pool(),
        },
        execution_classes={},
        database=DatabaseCapacity(
            max_connections=120,
            reserved_connections=20,
            fixed_service_connections=5,
        ),
    )

    capacity = config.connection_capacity()

    assert capacity.budget == 70
    assert capacity.pool_demand == 60
    assert capacity.total_demand == 65
    assert capacity.remaining == 5
    assert capacity.is_safe is True


def test_configuration_copies_mappings_and_checks_references() -> None:
    pools = {"finite": finite_pool(max_replicas=1)}
    config = RuntimeConfig(
        runtime_pools=pools,
        execution_classes={
            "default": ExecutionClassConfig(
                runtime_pool="finite",
                queue="runtime-default",
            )
        },
        database=DatabaseCapacity(max_connections=20),
    )
    pools["late"] = finite_pool(max_replicas=1)

    assert set(config.runtime_pools) == {"finite"}
    with pytest.raises(TypeError):
        config.runtime_pools["late"] = finite_pool()  # type: ignore[index]
    with pytest.raises(ConfigurationError, match="unknown pool"):
        RuntimeConfig(
            runtime_pools={"finite": finite_pool(max_replicas=1)},
            execution_classes={"broken": ExecutionClassConfig(runtime_pool="missing")},
            database=DatabaseCapacity(max_connections=20),
        )


@pytest.mark.parametrize("utilization_limit", [0, -0.1, 0.71, 1.0])
def test_database_capacity_enforces_seventy_percent_ceiling(
    utilization_limit: float,
) -> None:
    with pytest.raises(ConfigurationError, match=r"at most 0\.70"):
        DatabaseCapacity(
            max_connections=100,
            utilization_limit=utilization_limit,
        )
