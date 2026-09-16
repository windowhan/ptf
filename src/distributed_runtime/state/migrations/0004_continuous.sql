-- 0004: continuous deployment/partition/worker tables
-- (docs/design/state-schema.md §5). Lease columns live on the partition row so
-- fencing checks and owner changes are one atomic update.

CREATE TABLE runtime_state.continuous_deployments (
    deployment_id       text PRIMARY KEY,
    workload_id         text NOT NULL,
    workload_version    text NOT NULL,
    status              text NOT NULL DEFAULT 'pending',
    config              jsonb NOT NULL DEFAULT '{}',
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE runtime_state.continuous_partitions (
    deployment_id       text NOT NULL
        REFERENCES runtime_state.continuous_deployments (deployment_id),
    partition_id        text NOT NULL,
    status              text NOT NULL DEFAULT 'unassigned',
    weight              integer NOT NULL DEFAULT 1,
    payload             jsonb NOT NULL DEFAULT '{}',
    owner_id            text,
    fencing_token       bigint NOT NULL DEFAULT 0,
    lease_expires_at    timestamptz,
    assigned_at         timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (deployment_id, partition_id)
);

CREATE INDEX continuous_partitions_owned
    ON runtime_state.continuous_partitions (owner_id)
    WHERE owner_id IS NOT NULL;

CREATE TABLE runtime_state.worker_instances (
    instance_id         text PRIMARY KEY,
    execution_classes   jsonb NOT NULL DEFAULT '[]',
    revision            text NOT NULL DEFAULT '',
    last_heartbeat_at   timestamptz NOT NULL DEFAULT now(),
    status              text NOT NULL DEFAULT 'active',
    created_at          timestamptz NOT NULL DEFAULT now()
);
