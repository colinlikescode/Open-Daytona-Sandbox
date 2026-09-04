"""Initial schema (state schema version 1)."""

MIGRATION = (
    1,
    "initial",
    """
CREATE TABLE pools (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    data TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE workers (
    id TEXT PRIMARY KEY,
    pool_id TEXT NOT NULL,
    state TEXT NOT NULL,
    provider_cluster TEXT NOT NULL,
    data TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX workers_pool_state ON workers(pool_id, state);

CREATE TABLE sandboxes (
    id TEXT PRIMARY KEY,
    pool_id TEXT NOT NULL,
    worker_id TEXT,
    state TEXT NOT NULL,
    idempotency_key TEXT,
    data TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    expires_at TEXT
);
CREATE INDEX sandboxes_pool_state ON sandboxes(pool_id, state);
CREATE INDEX sandboxes_worker ON sandboxes(worker_id);
CREATE UNIQUE INDEX sandboxes_idempotency ON sandboxes(idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE operations (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    pool_id TEXT,
    worker_id TEXT,
    data TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX operations_status ON operations(status);

CREATE TABLE images (
    worker_id TEXT NOT NULL,
    reference TEXT NOT NULL,
    digest TEXT,
    pulled_at TEXT NOT NULL,
    PRIMARY KEY (worker_id, reference)
);

CREATE TABLE runtime_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE idempotency_keys (
    key TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);
""",
)
