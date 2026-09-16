"""Admin surface for continuous deployments.

Deploy (pending → active), drain (release partitions back to unassigned
after contexts cancel), stop (terminal), and inspect partition state.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass

from distributed_runtime.core.enums import DeploymentStatus
from distributed_runtime.core.envelope import JsonValue
from distributed_runtime.core.identifiers import DeploymentId, WorkloadId
from distributed_runtime.state.continuous import (
    ContinuousStateStore,
    StoredDeployment,
    StoredPartition,
)
from distributed_runtime.state.engine import StateEngine


@dataclass(frozen=True, slots=True)
class DeploymentView:
    deployment: StoredDeployment
    partitions: tuple[StoredPartition, ...]


class ContinuousAdmin:
    """Deployment lifecycle operations."""

    def __init__(self, engine: StateEngine, *, application: str) -> None:
        self._store = ContinuousStateStore(engine)
        self._application = application

    async def deploy(
        self,
        *,
        workload: str,
        version: str,
        config: Mapping[str, JsonValue] | None = None,
        deployment_id: DeploymentId | None = None,
    ) -> StoredDeployment:
        """Create a deployment and activate it."""
        if deployment_id is None:
            deployment_id = DeploymentId(f"deployment:{uuid.uuid4().hex[:16]}")
        deployment = await self._store.create_deployment(
            deployment_id=deployment_id,
            workload_id=WorkloadId(workload),
            workload_version=version,
            config=config,
        )
        await self._store.transition_deployment(deployment_id, DeploymentStatus.ACTIVE)
        return deployment

    async def drain(self, deployment_id: DeploymentId) -> None:
        """Begin draining: partitions release back to unassigned.

        Supervisors observe the status change through their tick; partition
        rows stay owned until each handler exits, then reconcile reassigns
        only when the deployment is ACTIVE again.
        """
        await self._store.transition_deployment(deployment_id, DeploymentStatus.DRAINING)
        # release leases so partitions return to the pool
        partitions = await self._store.partitions(deployment_id)
        for partition in partitions:
            if partition.owner_id is not None:
                await self._store.release_partition(
                    deployment_id,
                    partition.partition_id,
                    expected_fencing_token=partition.fencing_token,
                )

    async def stop(self, deployment_id: DeploymentId) -> None:
        """Terminal stop — drain then mark stopped."""
        await self.drain(deployment_id)
        await self._store.transition_deployment(deployment_id, DeploymentStatus.STOPPED)

    async def view(self, deployment_id: DeploymentId) -> DeploymentView:
        deployment = await self._store.get_deployment(deployment_id)
        if deployment is None:
            raise LookupError(f"unknown deployment: {deployment_id}")
        return DeploymentView(
            deployment=deployment,
            partitions=await self._store.partitions(deployment_id),
        )
