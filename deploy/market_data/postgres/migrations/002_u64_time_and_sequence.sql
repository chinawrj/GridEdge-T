BEGIN;

DROP VIEW market_event_source_summary;

ALTER TABLE market_events
    ALTER COLUMN source_sequence TYPE NUMERIC(20, 0),
    ALTER COLUMN ts_us TYPE NUMERIC(20, 0),
    ALTER COLUMN recv_us TYPE NUMERIC(20, 0);
ALTER TABLE market_events DROP CONSTRAINT market_events_source_sequence_check;
ALTER TABLE market_events DROP CONSTRAINT market_events_ts_us_check;
ALTER TABLE market_events DROP CONSTRAINT IF EXISTS market_events_check;
ALTER TABLE market_events DROP CONSTRAINT IF EXISTS market_events_recv_us_check;
ALTER TABLE market_events ADD CONSTRAINT market_events_source_sequence_u64 CHECK (
    source_sequence BETWEEN 0 AND 18446744073709551615
);
ALTER TABLE market_events ADD CONSTRAINT market_events_ts_us_u64 CHECK (
    ts_us BETWEEN 0 AND 18446744073709551615
);
ALTER TABLE market_events ADD CONSTRAINT market_events_recv_us_u64 CHECK (
    recv_us BETWEEN 0 AND 18446744073709551615
);

ALTER TABLE market_conflicts
    ALTER COLUMN source_sequence TYPE NUMERIC(20, 0);
ALTER TABLE market_conflicts ADD CONSTRAINT market_conflicts_source_sequence_u64 CHECK (
    source_sequence IS NULL OR source_sequence BETWEEN 0 AND 18446744073709551615
);

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

CREATE TABLE IF NOT EXISTS market_schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO market_schema_migrations (version) VALUES (2);

COMMIT;
