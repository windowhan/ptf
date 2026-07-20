"""Continuous partition, lease, context, and sink contracts."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from distributed_runtime.core.enums import WorkloadMode
from distributed_runtime.core.envelope import JsonValue, VersionedEnvelope, freeze_json_mapping
from distributed_runtime.core.identifiers import (
    DeploymentId,
    PartitionId,
    RuntimeInstanceId,
    WorkloadId,
)
from distributed_runtime.core.lifecycle import CancellationSource, CancellationToken, Deadline
from distributed_runtime.core.logging import LogContext

_STABLE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$")


def _immutable_payload(payload: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
    return freeze_json_mapping(payload)


class SinkGuarantee(StrEnum):
    """Durability guarantee advertised by a configured event sink."""

    RUNTIME_FENCED = "runtime_fenced"
    REDUCED_DIRECT = "reduced_direct"


@dataclass(frozen=True, slots=True)
class Partition:
    """One independently owned shard of a continuous workload."""

    partition_id: PartitionId
    payload: Mapping[str, JsonValue]
    weight: int = 1
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if type(self.weight) is not int or self.weight < 1:
            raise ValueError("partition weight must be a positive integer")
        if any(
            not isinstance(key, str) or not key or not isinstance(value, str)
            for key, value in self.metadata.items()
        ):
            raise ValueError("partition metadata must contain non-empty string keys and values")
        object.__setattr__(self, "payload", _immutable_payload(self.payload))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True, slots=True)
class LeaseHandle:
    """Fenced ownership proof supplied to one partition handler."""

    deployment_id: DeploymentId
    partition_id: PartitionId
    owner_id: RuntimeInstanceId
    fencing_token: int

    def __post_init__(self) -> None:
        if type(self.fencing_token) is not int or self.fencing_token < 1:
            raise ValueError("fencing_token must be a positive integer")

    def authorizes(
        self,
        *,
        deployment_id: DeploymentId,
        partition_id: PartitionId,
        owner_id: RuntimeInstanceId,
        fencing_token: int,
    ) -> bool:
        """Return whether a conditional operation carries current ownership."""
        return (
            type(fencing_token) is int
            and self.deployment_id == deployment_id
            and self.partition_id == partition_id
            and self.owner_id == owner_id
            and self.fencing_token == fencing_token
        )


@runtime_checkable
class EventSink(Protocol):
    """Fencing-aware destination for continuous events."""

    guarantee: SinkGuarantee

    async def emit(
        self,
        event: VersionedEnvelope,
        *,
        stable_id: str,
        lease: LeaseHandle,
    ) -> None:
        """Persist one logically stable event under current ownership."""
        ...


@runtime_checkable
class SinkRegistry(Protocol):
    """Resolve explicitly configured sinks by stable name."""

    def get(self, name: str) -> EventSink:
        """Return a configured sink or raise for an unknown name."""
        ...


@dataclass(frozen=True, slots=True)
class PartitionContext:
    """Runtime services and fenced identity supplied to a handler."""

    workload_id: WorkloadId
    partition: Partition
    lease: LeaseHandle
    sinks: SinkRegistry
    metadata: Mapping[str, str] = field(default_factory=dict)
    cancellation: CancellationToken = field(default_factory=lambda: CancellationSource().token)
    log_context: LogContext = field(default_factory=LogContext)
    deadline: Deadline | None = None

    def __post_init__(self) -> None:
        if self.partition.partition_id != self.lease.partition_id:
            raise ValueError("partition and lease identifiers must match")
        if any(
            not isinstance(key, str) or not key or not isinstance(value, str)
            for key, value in self.metadata.items()
        ):
            raise ValueError("context metadata must contain non-empty string keys and values")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    async def emit(
        self,
        sink_name: str,
        event: VersionedEnvelope,
        *,
        stable_id: str,
    ) -> None:
        """Emit through a configured sink while preserving lease fencing."""
        _require_stable_name("sink_name", sink_name)
        _require_stable_name("stable_id", stable_id)
        await self.sinks.get(sink_name).emit(
            event,
            stable_id=stable_id,
            lease=self.lease,
        )


@runtime_checkable
class PartitionHandler(Protocol):
    """Product code that processes one assigned partition."""

    async def run(
        self,
        context: PartitionContext,
        partition: Partition,
    ) -> None:
        """Run until cancellation, failure, or lease loss."""
        ...


@runtime_checkable
class ContinuousWorkload(Protocol):
    """Product-owned discovery and handling contract."""

    name: str
    version: str
    mode: WorkloadMode

    async def discover_partitions(self) -> Sequence[Partition]:
        """Return the currently desired partition set."""
        ...

    async def run_partition(
        self,
        context: PartitionContext,
        partition: Partition,
    ) -> None:
        """Process one partition under the supplied lease."""
        ...


async def discover_partitions(workload: ContinuousWorkload) -> tuple[Partition, ...]:
    """Normalize discovery and reject ambiguous duplicate partition IDs."""
    discovered = tuple(await workload.discover_partitions())
    seen: set[PartitionId] = set()
    for partition in discovered:
        if partition.partition_id in seen:
            raise ValueError(f"duplicate partition_id: {partition.partition_id}")
        seen.add(partition.partition_id)
    return tuple(sorted(discovered, key=lambda partition: partition.partition_id.value))


def _require_stable_name(name: str, value: str) -> None:
    if _STABLE_NAME.fullmatch(value) is None:
        raise ValueError(f"{name} must be a non-empty stable name")
