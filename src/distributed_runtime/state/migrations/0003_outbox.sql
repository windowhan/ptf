-- 0003: shared at-least-once outbox (docs/design/state-schema.md §4)

CREATE TABLE runtime_state.outbox_messages (
    outbox_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind            text NOT NULL,
    dedup_key       text NOT NULL,
    destination     text NOT NULL,
    envelope        jsonb NOT NULL,
    fencing_token   bigint,
    status          text NOT NULL DEFAULT 'pending',
    attempts        integer NOT NULL DEFAULT 0,
    published_at    timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (kind, dedup_key)
);

CREATE INDEX outbox_pending ON runtime_state.outbox_messages (outbox_id)
    WHERE status = 'pending';
