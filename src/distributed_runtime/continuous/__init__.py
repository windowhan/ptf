"""Contracts for partitioned continuous workloads."""

from distributed_runtime.continuous.contracts import (
    ContinuousWorkload,
    EventSink,
    LeaseHandle,
    Partition,
    PartitionContext,
    PartitionHandler,
    SinkGuarantee,
    SinkRegistry,
    discover_partitions,
)

__all__ = [
    "ContinuousWorkload",
    "EventSink",
    "LeaseHandle",
    "Partition",
    "PartitionContext",
    "PartitionHandler",
    "SinkGuarantee",
    "SinkRegistry",
    "discover_partitions",
]
