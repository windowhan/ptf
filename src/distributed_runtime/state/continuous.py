"""Continuous deployment/partition/lease repository.

Lease columns live on the partition row so fencing checks are one atomic
conditional update (state-schema.md §5.2, §6.5-6.7).
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

import asyncpg

from distributed_runtime.continuous import LeaseHandle, Partition
from distributed_runtime.core.enums import DeploymentStatus, PartitionStatus
from distributed_runtime.core.envelope import JsonValue, VersionedEnvelope
from distributed_runtime.core.errors import InvariantViolationError
from distributed_runtime.core.identifiers import (
    DeploymentId,
    PartitionId,
    RuntimeInstanceId,
    WorkloadId,
)
from distributed_runtime.state.engine import StateEngine
from distributed_runtime.state.outbox import insert_outbox


@dataclass(frozen=True, slots=True)
class StoredDeployment:
    deployment_id: DeploymentId
    workload_id: WorkloadId
    workload_version: str
    status: DeploymentStatus
    config: Mapping[str, JsonValue]


@dataclass(frozen=True, slots=True)
class StoredPartition:
    deployment_id: DeploymentId
    partition_id: PartitionId
    status: PartitionStatus
    weight: int
    payload: Mapping[str, JsonValue]
    owner_id: RuntimeInstanceId | None
    fencing_token: int
    lease_expires_at: datetime | None


def _deployment_from_row(row: asyncpg.Record) -> StoredDeployment:
    return StoredDeployment(
        deployment_id=DeploymentId(str(row["deployment_id"])),
        workload_id=WorkloadId(str(row["workload_id"])),
        workload_version=str(row["workload_version"]),
        status=DeploymentStatus(str(row["status"])),
        config=json.loads(str(row["config"])),
    )


def _partition_from_row(row: asyncpg.Record) -> StoredPartition:
    owner = row["owner_id"]
    return StoredPartition(
        deployment_id=DeploymentId(str(row["deployment_id"])),
        partition_id=PartitionId(str(row["partition_id"])),
        status=PartitionStatus(str(row["status"])),
        weight=int(row["weight"]),
        payload=json.loads(str(row["payload"])),
        owner_id=None if owner is None else RuntimeInstanceId(str(owner)),
        fencing_token=int(row["fencing_token"]),
        lease_expires_at=row["lease_expires_at"],
    )


class ContinuousStateStore:
    """Repository for continuous deployment, lease, and emission state."""

    def __init__(self, engine: StateEngine) -> None:
        self._engine = engine

    async def create_deployment(
        self,
        *,
        deployment_id: DeploymentId,
        workload_id: WorkloadId,
        workload_version: str,
        config: Mapping[str, JsonValue] | None = None,
    ) -> StoredDeployment:
        async with self._engine.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                    INSERT INTO runtime_state.continuous_deployments
                        (deployment_id, workload_id, workload_version,
                         status, config)
                    VALUES ($1, $2, $3, 'pending', $4::jsonb)
                    ON CONFLICT (deployment_id) DO NOTHING
                    """,
                str(deployment_id),
                str(workload_id),
                workload_version,
                json.dumps(dict(config or {})),
            )
            row = await connection.fetchrow(
                """
                    SELECT * FROM runtime_state.continuous_deployments
                    WHERE deployment_id = $1
                    """,
                str(deployment_id),
            )
        assert row is not None
        return _deployment_from_row(row)

    async def transition_deployment(
        self, deployment_id: DeploymentId, status: DeploymentStatus
    ) -> None:
        async with self._engine.acquire() as connection:
            changed = await connection.execute(
                """
                UPDATE runtime_state.continuous_deployments
                SET status = $2, updated_at = now()
                WHERE deployment_id = $1
                """,
                str(deployment_id),
                status.value,
            )
        if changed == "UPDATE 0":
            raise LookupError(f"unknown deployment: {deployment_id}")

    async def get_deployment(self, deployment_id: DeploymentId) -> StoredDeployment | None:
        async with self._engine.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT * FROM runtime_state.continuous_deployments
                WHERE deployment_id = $1
                """,
                str(deployment_id),
            )
        return None if row is None else _deployment_from_row(row)

    async def list_deployments(
        self, statuses: Sequence[DeploymentStatus] | None = None
    ) -> tuple[StoredDeployment, ...]:
        """Deployments in the given statuses (all statuses when omitted)."""
        async with self._engine.acquire() as connection:
            if statuses is None:
                rows = await connection.fetch(
                    """
                    SELECT * FROM runtime_state.continuous_deployments
                    ORDER BY deployment_id
                    """
                )
            else:
                rows = await connection.fetch(
                    """
                    SELECT * FROM runtime_state.continuous_deployments
                    WHERE status = ANY($1::text[])
                    ORDER BY deployment_id
                    """,
                    [status.value for status in statuses],
                )
        return tuple(_deployment_from_row(row) for row in rows)

    async def sync_partitions(
        self,
        deployment_id: DeploymentId,
        partitions: Sequence[Partition],
    ) -> tuple[StoredPartition, ...]:
        """Upsert discovered partitions; missing ones become inactive."""
        desired_ids = [str(p.partition_id) for p in partitions]
        async with self._engine.acquire() as connection, connection.transaction():
            await connection.executemany(
                """
                    INSERT INTO runtime_state.continuous_partitions
                        (deployment_id, partition_id, status, weight, payload)
                    VALUES ($1, $2, 'unassigned', $3, $4::jsonb)
                    ON CONFLICT (deployment_id, partition_id) DO UPDATE
                        SET weight = EXCLUDED.weight,
                            payload = EXCLUDED.payload,
                            status = CASE
                                WHEN continuous_partitions.status = 'inactive'
                                THEN 'unassigned'
                                ELSE continuous_partitions.status END,
                            updated_at = now()
                    """,
                [
                    (
                        str(deployment_id),
                        str(p.partition_id),
                        p.weight,
                        json.dumps(dict(p.payload)),
                    )
                    for p in partitions
                ],
            )
            if desired_ids:
                await connection.execute(
                    """
                        UPDATE runtime_state.continuous_partitions
                        SET status = 'inactive', owner_id = NULL,
                            lease_expires_at = NULL, updated_at = now()
                        WHERE deployment_id = $1
                          AND partition_id <> ALL($2::text[])
                          AND status IN ('unassigned', 'assigned', 'running')
                        """,
                    str(deployment_id),
                    desired_ids,
                )
            rows = await connection.fetch(
                """
                    SELECT * FROM runtime_state.continuous_partitions
                    WHERE deployment_id = $1 ORDER BY partition_id
                    """,
                str(deployment_id),
            )
        return tuple(_partition_from_row(row) for row in rows)

    async def partitions(self, deployment_id: DeploymentId) -> tuple[StoredPartition, ...]:
        async with self._engine.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT * FROM runtime_state.continuous_partitions
                WHERE deployment_id = $1 ORDER BY partition_id
                """,
                str(deployment_id),
            )
        return tuple(_partition_from_row(row) for row in rows)

    async def acquire_lease(
        self,
        deployment_id: DeploymentId,
        partition_id: PartitionId,
        owner: RuntimeInstanceId,
        *,
        lease_seconds: int,
    ) -> LeaseHandle | None:
        """Take ownership with a monotonically increasing fencing token.

        Fails (returns None) when another owner holds an unexpired lease.
        """
        async with self._engine.acquire() as connection:
            row = await connection.fetchrow(
                """
                UPDATE runtime_state.continuous_partitions
                SET owner_id = $3,
                    fencing_token = fencing_token + 1,
                    lease_expires_at = now() + make_interval(secs => $4),
                    assigned_at = now(),
                    status = 'assigned',
                    updated_at = now()
                WHERE deployment_id = $1 AND partition_id = $2
                  AND status IN ('unassigned', 'assigned', 'running')
                  AND (lease_expires_at IS NULL
                       OR lease_expires_at < now()
                       OR owner_id = $3)
                RETURNING fencing_token
                """,
                str(deployment_id),
                str(partition_id),
                str(owner),
                lease_seconds,
            )
        if row is None:
            return None
        return LeaseHandle(
            deployment_id=deployment_id,
            partition_id=partition_id,
            owner_id=owner,
            fencing_token=int(row["fencing_token"]),
        )

    async def renew_lease(self, lease: LeaseHandle, *, lease_seconds: int) -> bool:
        """Extend the lease only while the caller's token is current."""
        async with self._engine.acquire() as connection:
            changed = await connection.execute(
                """
                UPDATE runtime_state.continuous_partitions
                SET lease_expires_at = now() + make_interval(secs => $4),
                    updated_at = now()
                WHERE deployment_id = $1 AND partition_id = $2
                  AND owner_id = $3 AND fencing_token = $5
                  AND lease_expires_at > now()
                """,
                str(lease.deployment_id),
                str(lease.partition_id),
                str(lease.owner_id),
                lease_seconds,
                lease.fencing_token,
            )
        return changed == "UPDATE 1"

    async def release_lease(self, lease: LeaseHandle) -> bool:
        """Drop ownership while the caller's fencing token is current."""
        async with self._engine.acquire() as connection:
            changed = await connection.execute(
                """
                UPDATE runtime_state.continuous_partitions
                SET owner_id = NULL, lease_expires_at = NULL,
                    status = 'unassigned', updated_at = now()
                WHERE deployment_id = $1 AND partition_id = $2
                  AND owner_id = $3 AND fencing_token = $4
                """,
                str(lease.deployment_id),
                str(lease.partition_id),
                str(lease.owner_id),
                lease.fencing_token,
            )
        return changed == "UPDATE 1"

    async def release_partition(
        self,
        deployment_id: DeploymentId,
        partition_id: PartitionId,
        *,
        expected_fencing_token: int,
    ) -> bool:
        """Return a partition to unassigned, fenced by its current token."""
        async with self._engine.acquire() as connection:
            changed = await connection.execute(
                """
                UPDATE runtime_state.continuous_partitions
                SET owner_id = NULL, lease_expires_at = NULL,
                    status = 'unassigned', updated_at = now()
                WHERE deployment_id = $1 AND partition_id = $2
                  AND fencing_token = $3
                """,
                str(deployment_id),
                str(partition_id),
                expected_fencing_token,
            )
        return changed == "UPDATE 1"

    async def set_partition_status(
        self,
        lease: LeaseHandle,
        status: PartitionStatus,
    ) -> bool:
        """Fenced status transition — stale owners cannot move state."""
        async with self._engine.acquire() as connection:
            changed = await connection.execute(
                """
                UPDATE runtime_state.continuous_partitions
                SET status = $4, updated_at = now()
                WHERE deployment_id = $1 AND partition_id = $2
                  AND owner_id = $3 AND fencing_token = $5
                """,
                str(lease.deployment_id),
                str(lease.partition_id),
                str(lease.owner_id),
                status.value,
                lease.fencing_token,
            )
        return changed == "UPDATE 1"

    async def emit(
        self,
        lease: LeaseHandle,
        *,
        destination: str,
        stable_id: str,
        envelope: VersionedEnvelope,
    ) -> int:
        """Record one emission: fencing check + outbox row in one transaction."""
        async with self._engine.acquire() as connection, connection.transaction():
            changed = await connection.execute(
                """
                    UPDATE runtime_state.continuous_partitions
                    SET updated_at = now()
                    WHERE deployment_id = $1 AND partition_id = $2
                      AND owner_id = $3 AND fencing_token = $4
                      AND lease_expires_at > now()
                    """,
                str(lease.deployment_id),
                str(lease.partition_id),
                str(lease.owner_id),
                lease.fencing_token,
            )
            if changed != "UPDATE 1":
                raise InvariantViolationError(
                    "emission rejected: stale or expired fencing token",
                    details={
                        "partition_id": str(lease.partition_id),
                        "fencing_token": lease.fencing_token,
                    },
                )
            return await insert_outbox(
                connection,
                kind="emission",
                dedup_key=(f"{lease.deployment_id}:{lease.partition_id}:{stable_id}"),
                destination=destination,
                envelope=envelope,
                fencing_token=lease.fencing_token,
            )

    async def record_checkpoint(
        self,
        lease: LeaseHandle,
        *,
        destination: str,
        checkpoint_key: str,
        envelope: VersionedEnvelope,
    ) -> int:
        """Persist a checkpoint marker through the outbox with fencing."""
        async with self._engine.acquire() as connection, connection.transaction():
            changed = await connection.execute(
                """
                    UPDATE runtime_state.continuous_partitions
                    SET updated_at = now()
                    WHERE deployment_id = $1 AND partition_id = $2
                      AND owner_id = $3 AND fencing_token = $4
                    """,
                str(lease.deployment_id),
                str(lease.partition_id),
                str(lease.owner_id),
                lease.fencing_token,
            )
            if changed != "UPDATE 1":
                raise InvariantViolationError(
                    "checkpoint rejected: stale fencing token",
                    details={"partition_id": str(lease.partition_id)},
                )
            return await insert_outbox(
                connection,
                kind="checkpoint",
                dedup_key=(f"{lease.deployment_id}:{lease.partition_id}:{checkpoint_key}"),
                destination=destination,
                envelope=envelope,
                fencing_token=lease.fencing_token,
            )
