-- 0001: finite run and planning tables (docs/design/state-schema.md §3.1-3.2)

CREATE TABLE runtime_state.finite_runs (
    run_id              text PRIMARY KEY,
    workload_id         text NOT NULL,
    workload_name       text NOT NULL,
    workload_version    text NOT NULL,
    planner_revision    text NOT NULL,
    execution_revision  text NOT NULL,
    status              text NOT NULL DEFAULT 'pending',
    input               jsonb NOT NULL,
    planning_generation integer NOT NULL DEFAULT 1,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    cancelled_at        timestamptz
);

CREATE INDEX finite_runs_status ON runtime_state.finite_runs (status)
    WHERE status IN ('pending', 'planning', 'running');

CREATE TABLE runtime_state.finite_plan_units (
    run_id              text NOT NULL REFERENCES runtime_state.finite_runs (run_id),
    planning_generation integer NOT NULL,
    ordinal             integer NOT NULL,
    unit_key            text NOT NULL,
    payload_hash        text NOT NULL,
    unit                jsonb NOT NULL,
    PRIMARY KEY (run_id, planning_generation, ordinal),
    UNIQUE (run_id, planning_generation, unit_key)
);
