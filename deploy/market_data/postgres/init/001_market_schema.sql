CREATE TABLE market_events (
    event_id BYTEA PRIMARY KEY CHECK (octet_length(event_id) = 32),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    event_type TEXT NOT NULL CHECK (event_type IN (
        'TRADE_TICK', 'QUOTE_SNAPSHOT', 'BOOK_SNAPSHOT',
        'BOOK_DELTA', 'BAR', 'SOURCE_STATUS'
    )),
    source_id TEXT NOT NULL CHECK (source_id ~ '^[a-z0-9][a-z0-9._-]{0,63}$'),
    source_instance_id UUID NOT NULL,
    source_sequence NUMERIC(20, 0) NOT NULL CHECK (
        source_sequence BETWEEN 0 AND 18446744073709551615
    ),
    source_metadata JSONB NOT NULL,
    venue TEXT NOT NULL CHECK (venue ~ '^[A-Z0-9]{2,16}$'),
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z0-9._-]{1,32}$'),
    asset_class TEXT NOT NULL,
    currency TEXT NOT NULL,
    ts_us NUMERIC(20, 0) NOT NULL CHECK (ts_us BETWEEN 0 AND 18446744073709551615),
    recv_us NUMERIC(20, 0) NOT NULL CHECK (
        recv_us BETWEEN 0 AND 18446744073709551615
    ),
    payload_format TEXT NOT NULL,
    payload_json JSONB,
    payload_bytes BYTEA NOT NULL,
    payload_sha256 BYTEA NOT NULL CHECK (octet_length(payload_sha256) = 32),
    evidence_sha256 BYTEA NOT NULL CHECK (octet_length(evidence_sha256) = 32),
    mqtt_topic TEXT NOT NULL,
    qos SMALLINT NOT NULL CHECK (qos BETWEEN 0 AND 2),
    first_received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    delivery_count BIGINT NOT NULL DEFAULT 1 CHECK (delivery_count > 0),
    UNIQUE (source_id, source_instance_id, source_sequence)
);

CREATE INDEX market_events_instrument_time
    ON market_events (venue, symbol, event_type, ts_us, source_id);
CREATE INDEX market_events_source_time
    ON market_events (source_id, source_instance_id, source_sequence);

CREATE TABLE market_duplicate_deliveries (
    duplicate_id BIGSERIAL PRIMARY KEY,
    event_id BYTEA NOT NULL REFERENCES market_events(event_id),
    payload_sha256 BYTEA NOT NULL CHECK (octet_length(payload_sha256) = 32),
    mqtt_topic TEXT NOT NULL,
    qos SMALLINT NOT NULL CHECK (qos BETWEEN 0 AND 2),
    duplicate_flag BOOLEAN NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX market_duplicate_deliveries_event
    ON market_duplicate_deliveries (event_id, received_at);

CREATE TABLE market_conflicts (
    conflict_id BIGSERIAL PRIMARY KEY,
    claimed_event_id BYTEA CHECK (claimed_event_id IS NULL OR octet_length(claimed_event_id) = 32),
    source_id TEXT,
    source_instance_id UUID,
    source_sequence NUMERIC(20, 0) CHECK (
        source_sequence IS NULL OR source_sequence BETWEEN 0 AND 18446744073709551615
    ),
    payload_sha256 BYTEA NOT NULL CHECK (octet_length(payload_sha256) = 32),
    mqtt_topic TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload_bytes BYTEA NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX market_conflicts_received ON market_conflicts (received_at);

CREATE TABLE market_rejections (
    rejection_id BIGSERIAL PRIMARY KEY,
    mqtt_topic TEXT NOT NULL,
    payload_sha256 BYTEA NOT NULL CHECK (octet_length(payload_sha256) = 32),
    reason TEXT NOT NULL,
    payload_bytes BYTEA NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX market_rejections_received ON market_rejections (received_at);

CREATE VIEW market_event_source_summary AS
SELECT venue,
       symbol,
       source_id,
       event_type,
       COUNT(*) AS unique_events,
       SUM(delivery_count - 1) AS duplicate_deliveries,
       MIN(ts_us) AS first_ts_us,
       MAX(ts_us) AS last_ts_us
FROM market_events
GROUP BY venue, symbol, source_id, event_type;

CREATE TABLE market_schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO market_schema_migrations (version) VALUES (1);
