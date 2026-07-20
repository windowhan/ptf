"""UT-CON-01: partition, lease, context, and sink contracts."""

from __future__ import annotations

from typing import cast

import pytest

from distributed_runtime.continuous import (
    EventSink,
    LeaseHandle,
    Partition,
    PartitionContext,
    SinkGuarantee,
)
from distributed_runtime.core import (
    DeploymentId,
    JsonValue,
    PartitionId,
    RuntimeInstanceId,
    VersionedEnvelope,
    WorkloadId,
)


def make_partition() -> Partition:
    return Partition(
        partition_id=PartitionId("partition:0"),
        payload={"source": "stream-0", "subscriptions": ["A"]},
        weight=2,
        metadata={"region": "internal"},
    )


def make_lease(*, token: int = 7) -> LeaseHandle:
    return LeaseHandle(
        deployment_id=DeploymentId("deployment:1"),
        partition_id=PartitionId("partition:0"),
        owner_id=RuntimeInstanceId("instance:1"),
        fencing_token=token,
    )


def make_event(payload: dict[str, JsonValue]) -> VersionedEnvelope:
    return VersionedEnvelope(
        schema_version=1,
        message_kind="stream.event",
        run_id="deployment:1",
        execution_id="partition:0",
        idempotency_key="partition:0/sequence:1",
        application="example",
        workload="example.stream",
        workload_version="1",
        handler="partition",
        attempt_generation=7,
        execution_class="stream",
        runtime_pool_revision="continuous:v1",
        published_at="2026-07-20T04:00:00Z",
        payload=payload,
    )


def test_partition_is_deeply_immutable() -> None:
    subscriptions: list[JsonValue] = ["A"]
    partition = Partition(
        partition_id=PartitionId("partition:0"),
        payload={"subscriptions": subscriptions},
    )
    subscriptions.append("late")

    assert cast(object, partition.payload["subscriptions"]) == ("A",)
    with pytest.raises(TypeError):
        partition.payload["other"] = True  # type: ignore[index]
    with pytest.raises(ValueError, match="weight"):
        Partition(
            partition_id=partition.partition_id,
            payload={},
            weight=0,
        )
    with pytest.raises(ValueError, match="weight"):
        Partition(
            partition_id=partition.partition_id,
            payload={},
            weight=cast(int, 1.0),
        )


def test_lease_authorization_requires_every_fencing_dimension() -> None:
    lease = make_lease()
    assert lease.authorizes(
        deployment_id=lease.deployment_id,
        partition_id=lease.partition_id,
        owner_id=lease.owner_id,
        fencing_token=lease.fencing_token,
    )
    assert not lease.authorizes(
        deployment_id=lease.deployment_id,
        partition_id=lease.partition_id,
        owner_id=lease.owner_id,
        fencing_token=6,
    )
    assert not lease.authorizes(
        deployment_id=lease.deployment_id,
        partition_id=lease.partition_id,
        owner_id=RuntimeInstanceId("instance:2"),
        fencing_token=lease.fencing_token,
    )
    assert not lease.authorizes(
        deployment_id=lease.deployment_id,
        partition_id=lease.partition_id,
        owner_id=lease.owner_id,
        fencing_token=cast(int, 7.0),
    )
    with pytest.raises(ValueError, match="fencing_token"):
        make_lease(token=cast(int, 7.0))


def test_context_rejects_mismatched_partition_lease() -> None:
    partition = make_partition()
    lease = LeaseHandle(
        deployment_id=DeploymentId("deployment:1"),
        partition_id=PartitionId("partition:other"),
        owner_id=RuntimeInstanceId("instance:1"),
        fencing_token=1,
    )

    with pytest.raises(ValueError, match="must match"):
        PartitionContext(
            workload_id=WorkloadId("stream"),
            partition=partition,
            lease=lease,
            sinks=RecordingRegistry(),
        )


def test_context_rejects_non_string_metadata_keys() -> None:
    with pytest.raises(ValueError, match="context metadata"):
        PartitionContext(
            workload_id=WorkloadId("stream"),
            partition=make_partition(),
            lease=make_lease(),
            sinks=RecordingRegistry(),
            metadata=cast(dict[str, str], {1: "invalid"}),
        )


class RecordingSink:
    def __init__(
        self,
        guarantee: SinkGuarantee = SinkGuarantee.RUNTIME_FENCED,
    ) -> None:
        self.guarantee = guarantee
        self.events: list[tuple[str, VersionedEnvelope, LeaseHandle]] = []

    async def emit(
        self,
        event: VersionedEnvelope,
        *,
        stable_id: str,
        lease: LeaseHandle,
    ) -> None:
        self.events.append((stable_id, event, lease))


class RecordingRegistry:
    def __init__(self) -> None:
        self.sink = RecordingSink()

    def get(self, name: str) -> EventSink:
        if name != "events":
            raise KeyError(name)
        return self.sink
