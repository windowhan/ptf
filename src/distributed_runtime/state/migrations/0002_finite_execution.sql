-- 0002: finite unit execution and attempt tables (docs/design/state-schema.md §3.3-3.4)

CREATE TABLE runtime_state.finite_units (
    run_id              text NOT NULL REFERENCES runtime_state.finite_runs (run_id),
    unit_key            text NOT NULL,
    status              text NOT NULL DEFAULT 'ready',
    attempt_count       integer NOT NULL DEFAULT 0,
    next_attempt_at     timestamptz,
    claimed_by          text,
    claim_expires_at    timestamptz,
    claim_token         bigint NOT NULL DEFAULT 0,
    result              jsonb,
    last_error          jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, unit_key)
);

-- claim scan: due work first
CREATE INDEX finite_units_claimable ON runtime_state.finite_units
    (next_attempt_at)
    WHERE status IN ('ready', 'retry_scheduled');

CREATE TABLE runtime_state.finite_attempts (
    run_id              text NOT NULL,
    unit_key            text NOT NULL,
    attempt             integer NOT NULL,
    execution_id        text NOT NULL,
    started_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz,
    outcome             text,
    error               jsonb,
    PRIMARY KEY (run_id, unit_key, attempt),
    FOREIGN KEY (run_id, unit_key)
        REFERENCES runtime_state.finite_units (run_id, unit_key)
);
