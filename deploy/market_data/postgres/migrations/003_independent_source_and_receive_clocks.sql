BEGIN;

ALTER TABLE market_events DROP CONSTRAINT market_events_recv_us_u64;
ALTER TABLE market_events ADD CONSTRAINT market_events_recv_us_u64 CHECK (
    recv_us BETWEEN 0 AND 18446744073709551615
);
INSERT INTO market_schema_migrations (version) VALUES (3);

COMMIT;
