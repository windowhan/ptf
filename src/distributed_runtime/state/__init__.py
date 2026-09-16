"""PostgreSQL state layer: engine, migrations, repositories.

Importing this package requires the ``postgres`` extra (asyncpg). Core
contracts never import this package.
"""

from distributed_runtime.state.continuous import (
    ContinuousStateStore,
    StoredDeployment,
    StoredPartition,
)
from distributed_runtime.state.engine import StateEngine
from distributed_runtime.state.finite import (
    ClaimedUnit,
    FiniteStateStore,
    StoredRun,
    UnitState,
)
from distributed_runtime.state.migrations import (
    Migration,
    applied_versions,
    available_migrations,
    migrate,
)
from distributed_runtime.state.outbox import (
    OutboxMessage,
    OutboxStore,
    insert_outbox,
)
from distributed_runtime.state.workers import WorkerInstance, WorkerRegistry

__all__ = [
    "ClaimedUnit",
    "ContinuousStateStore",
    "FiniteStateStore",
    "Migration",
    "OutboxMessage",
    "OutboxStore",
    "StateEngine",
    "StoredDeployment",
    "StoredPartition",
    "StoredRun",
    "UnitState",
    "WorkerInstance",
    "WorkerRegistry",
    "applied_versions",
    "available_migrations",
    "insert_outbox",
    "migrate",
]
