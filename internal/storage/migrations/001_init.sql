CREATE TABLE IF NOT EXISTS event_instances (
    id UUID PRIMARY KEY,
    source_system VARCHAR(64) NOT NULL,
    event_id VARCHAR(160) NOT NULL,
    event_type VARCHAR(200) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    routing_key VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    request_fingerprint CHAR(64) NOT NULL,
    config_version CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (source_system, event_id)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id UUID PRIMARY KEY,
    event_pk UUID NOT NULL REFERENCES event_instances(id) ON DELETE CASCADE,
    route_id VARCHAR(160) NOT NULL,
    route_revision INTEGER NOT NULL,
    external_system_id VARCHAR(160) NOT NULL,
    api_operation_id VARCHAR(160) NOT NULL,
    api_operation_revision INTEGER NOT NULL,
    request_snapshot JSONB NOT NULL,
    status VARCHAR(24) NOT NULL,
    round INTEGER NOT NULL DEFAULT 1,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    next_attempt_at TIMESTAMPTZ,
    lease_owner VARCHAR(160),
    lease_until TIMESTAMPTZ,
    last_error_class VARCHAR(64),
    last_error_summary VARCHAR(512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    UNIQUE (event_pk, route_id),
    CHECK (status IN ('ACCEPTED', 'DELIVERING', 'RETRY_WAIT', 'SUCCEEDED', 'DEAD_LETTER'))
);

CREATE INDEX IF NOT EXISTS idx_deliveries_due
ON deliveries (next_attempt_at, id)
WHERE status IN ('ACCEPTED', 'RETRY_WAIT');

CREATE INDEX IF NOT EXISTS idx_deliveries_expired_lease
ON deliveries (lease_until, id)
WHERE status = 'DELIVERING';

CREATE INDEX IF NOT EXISTS idx_deliveries_dead_letter
ON deliveries (updated_at DESC, id)
WHERE status = 'DEAD_LETTER';

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id BIGSERIAL PRIMARY KEY,
    delivery_id UUID NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    round INTEGER NOT NULL,
    attempt_no INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    duration_ms BIGINT NOT NULL,
    http_status INTEGER,
    result_class VARCHAR(64) NOT NULL,
    error_summary VARCHAR(512),
    UNIQUE (delivery_id, round, attempt_no)
);

CREATE TABLE IF NOT EXISTS delivery_replays (
    id BIGSERIAL PRIMARY KEY,
    delivery_id UUID NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    from_round INTEGER NOT NULL,
    to_round INTEGER NOT NULL,
    operator VARCHAR(160) NOT NULL,
    reason VARCHAR(512) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
