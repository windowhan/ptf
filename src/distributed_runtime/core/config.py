"""Typed runtime configuration and connection-capacity validation."""

from __future__ import annotations

from dataclasses import dataclass
from math import floor

from distributed_runtime.core.enums import WorkloadMode
from distributed_runtime.core.errors import RuntimeContractError


class ConfigurationError(RuntimeContractError):
    """Raised when runtime configuration violates a safety invariant."""

    code = "invalid_configuration"


@dataclass(frozen=True, slots=True)
class SecretReference:
    """One immutable Secret Manager version, never a moving alias."""

    project_id: str
    secret_id: str
    version: str

    def __post_init__(self) -> None:
        _require_name("secret project_id", self.project_id)
        _require_name("secret secret_id", self.secret_id)
        if self.version == "latest" or not self.version.isdecimal() or int(self.version) < 1:
            raise ConfigurationError("secret version must be an explicit positive version")

    @property
    def resource_name(self) -> str:
        """Return the canonical version-scoped resource name."""
        return f"projects/{self.project_id}/secrets/{self.secret_id}/versions/{self.version}"


@dataclass(frozen=True, slots=True)
class FinitePolicy:
    """Defaults and limits for independently retried finite units."""

    timeout_seconds: int = 300
    max_timeout_seconds: int = 3_600
    max_attempts: int = 5
    retry_base_seconds: int = 5
    retry_cap_seconds: int = 300
    claim_grace_seconds: int = 120

    def __post_init__(self) -> None:
        _require_positive("timeout_seconds", self.timeout_seconds)
        _require_positive("max_timeout_seconds", self.max_timeout_seconds)
        _require_positive("max_attempts", self.max_attempts)
        _require_positive("retry_base_seconds", self.retry_base_seconds)
        _require_positive("retry_cap_seconds", self.retry_cap_seconds)
        _require_positive("claim_grace_seconds", self.claim_grace_seconds)
        if self.timeout_seconds > self.max_timeout_seconds:
            raise ConfigurationError("timeout_seconds must not exceed max_timeout_seconds")
        if self.retry_base_seconds > self.retry_cap_seconds:
            raise ConfigurationError("retry_base_seconds must not exceed retry_cap_seconds")

    @property
    def claim_ttl_seconds(self) -> int:
        """Return the lease duration workers use for a finite attempt."""
        return self.timeout_seconds + self.claim_grace_seconds


@dataclass(frozen=True, slots=True)
class ContinuousPolicy:
    """Timing contract for continuous leases, health, and drain."""

    heartbeat_seconds: int = 15
    lease_seconds: int = 60
    renew_before_expiry_seconds: int = 30
    reconcile_seconds: int = 60
    drain_seconds: int = 120
    takeover_seconds: int = 150

    def __post_init__(self) -> None:
        for name, value in (
            ("heartbeat_seconds", self.heartbeat_seconds),
            ("lease_seconds", self.lease_seconds),
            ("renew_before_expiry_seconds", self.renew_before_expiry_seconds),
            ("reconcile_seconds", self.reconcile_seconds),
            ("drain_seconds", self.drain_seconds),
            ("takeover_seconds", self.takeover_seconds),
        ):
            _require_positive(name, value)
        if self.heartbeat_seconds > self.renew_before_expiry_seconds:
            raise ConfigurationError(
                "heartbeat_seconds must not exceed renew_before_expiry_seconds"
            )
        if self.renew_before_expiry_seconds >= self.lease_seconds:
            raise ConfigurationError("renew_before_expiry_seconds must be less than lease_seconds")
        if self.takeover_seconds < self.lease_seconds:
            raise ConfigurationError("takeover_seconds must cover at least one lease")


@dataclass(frozen=True, slots=True)
class RuntimePoolConfig:
    """Autoscaling and database demand for one execution pool."""

    mode: WorkloadMode
    min_replicas: int
    max_replicas: int
    connections_per_replica: int
    target_messages_per_instance: int | None = None
    target_cpu_utilization: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.mode, WorkloadMode):
            raise ConfigurationError("mode must be finite or continuous")
        _require_non_negative("min_replicas", self.min_replicas)
        _require_positive("max_replicas", self.max_replicas)
        _require_positive("connections_per_replica", self.connections_per_replica)
        if self.min_replicas > self.max_replicas:
            raise ConfigurationError("min_replicas must not exceed max_replicas")
        if self.mode is WorkloadMode.FINITE:
            if self.target_messages_per_instance is None:
                raise ConfigurationError("finite pools require target_messages_per_instance")
            _require_positive(
                "target_messages_per_instance",
                self.target_messages_per_instance,
            )
            if self.target_cpu_utilization is not None:
                raise ConfigurationError("finite pools must not set target_cpu_utilization")
        else:
            if self.min_replicas < 2:
                raise ConfigurationError("continuous pools require at least two replicas")
            if self.target_cpu_utilization is None or not (0 < self.target_cpu_utilization < 1):
                raise ConfigurationError(
                    "continuous target_cpu_utilization must be between 0 and 1"
                )
            if self.target_messages_per_instance is not None:
                raise ConfigurationError(
                    "continuous pools must not set target_messages_per_instance"
                )

    @property
    def worst_case_connections(self) -> int:
        """Return demand when this pool reaches maximum replicas."""
        return self.max_replicas * self.connections_per_replica


@dataclass(frozen=True, slots=True)
class ExecutionClassConfig:
    """Route a workload execution class to a configured runtime pool."""

    runtime_pool: str
    queue: str | None = None

    def __post_init__(self) -> None:
        _require_name("runtime_pool", self.runtime_pool)
        if self.queue is not None:
            _require_name("queue", self.queue)


@dataclass(frozen=True, slots=True)
class DatabaseCapacity:
    """Database limit and the fraction available to runtime services."""

    max_connections: int
    reserved_connections: int = 0
    utilization_limit: float = 0.70
    fixed_service_connections: int = 0

    def __post_init__(self) -> None:
        _require_positive("max_connections", self.max_connections)
        _require_non_negative("reserved_connections", self.reserved_connections)
        _require_non_negative(
            "fixed_service_connections",
            self.fixed_service_connections,
        )
        if self.reserved_connections >= self.max_connections:
            raise ConfigurationError("reserved_connections must be less than max_connections")
        if not 0 < self.utilization_limit <= 0.70:
            raise ConfigurationError("utilization_limit must be greater than 0 and at most 0.70")

    @property
    def runtime_budget(self) -> int:
        """Return the safe application budget after reserved connections."""
        usable_connections = self.max_connections - self.reserved_connections
        return floor(usable_connections * self.utilization_limit)


@dataclass(frozen=True, slots=True)
class ConnectionCapacity:
    """Auditable worst-case database connection calculation."""

    budget: int
    fixed_demand: int
    pool_demand: int

    @property
    def total_demand(self) -> int:
        return self.fixed_demand + self.pool_demand

    @property
    def remaining(self) -> int:
        return self.budget - self.total_demand

    @property
    def is_safe(self) -> bool:
        return self.remaining >= 0


def _require_positive(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigurationError(f"{name} must be a positive integer")


def _require_non_negative(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigurationError(f"{name} must be a non-negative integer")


def _require_name(name: str, value: str) -> None:
    if not value or value != value.strip():
        raise ConfigurationError(f"{name} must be a non-empty trimmed string")
