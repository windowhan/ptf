"""Stable string enumerations shared by runtime contracts."""

from enum import StrEnum


class WorkloadMode(StrEnum):
    """The two lifecycle models supported by the runtime."""

    FINITE = "finite"
    CONTINUOUS = "continuous"


class RunStatus(StrEnum):
    """Aggregate lifecycle of one finite workload submission."""

    PENDING = "pending"
    PLANNING = "planning"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ExecutionStatus(StrEnum):
    """Lifecycle of an independently retried finite execution unit."""

    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    RETRY_SCHEDULED = "retry_scheduled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"
    CANCELLED = "cancelled"


class DeploymentStatus(StrEnum):
    """Lifecycle of a continuous workload deployment."""

    PENDING = "pending"
    ACTIVE = "active"
    DRAINING = "draining"
    STOPPED = "stopped"
    FAILED = "failed"


class PartitionStatus(StrEnum):
    """Assignment and processing state of a continuous partition."""

    UNASSIGNED = "unassigned"
    ASSIGNED = "assigned"
    RUNNING = "running"
    BLOCKED_SINK = "blocked_sink"
    FAILED = "failed"
    INACTIVE = "inactive"


class ErrorKind(StrEnum):
    """Machine-readable classification for runtime contract failures."""

    CANCELLED = "cancelled"
    INVALID_IDENTIFIER = "invalid_identifier"
    INVARIANT_VIOLATION = "invariant_violation"
    PLANNING_DRIFT = "planning_drift"
    RETRYABLE_EXECUTION = "retryable_execution"
    PERMANENT_EXECUTION = "permanent_execution"
    RATE_LIMITED = "rate_limited"
    UNSUPPORTED_VERSION = "unsupported_version"
