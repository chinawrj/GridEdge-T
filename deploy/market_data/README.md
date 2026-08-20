# GridEdge Market Data Plane

This deployment is deliberately separated from the trading process. It gives
the Synology host three responsibilities only:

1. terminate authenticated MQTT 5 connections;
2. persist every accepted market event in PostgreSQL;
3. detect exact redelivery and conflicting identities without changing the
   first accepted event.

The containers do **not** receive broker-order credentials or access to the
GridEdge trading ledger.

The optional Mac shadow publisher is a separate read-only process. It tails
the already-reviewed Tonghuashun quote JSONL, owns a durable source sequence
and local MQTT outbox, and has no command-line or filesystem path for orders.
It deliberately uses the simulation marker only as a local admission check;
`account_marker` is not market data and is not transmitted in the event.

## Host layout

The installer copies this directory to:

```text
/volume1/Projects/GridEdge-Market/app
```

Persistent state remains outside the application copy:

```text
/volume1/Projects/GridEdge-Market/data/mosquitto
/volume1/Projects/GridEdge-Market/data/postgres
/volume1/Projects/GridEdge-Market/logs/mosquitto
/volume1/Projects/GridEdge-Market/secrets
```

PostgreSQL is reachable only on the private Compose network. MQTT is exposed
as TLS on port `8883`; anonymous access and plain-text port `1883` are disabled.

## Market event v1

The first deployed codec is canonical JSON. The database stores the original
payload as bytes and labels its content type, so a Protobuf codec can be added
without changing event identity or the relational indexing model.

```json
{
  "spec": "gridedge.market",
  "schema_version": 1,
  "event_id": "64 lowercase SHA-256 hex characters",
  "event_type": "TRADE_TICK",
  "source": {
    "source_id": "ths-web-tick",
    "source_instance_id": "0198...",
    "source_type": "WEB_UI",
    "environment": "SIMULATION"
  },
  "instrument": {
    "venue": "XSHE",
    "symbol": "002256",
    "asset_class": "EQUITY",
    "currency": "CNY"
  },
  "source_sequence": 42,
  "ts_us": 1787103000123456,
  "recv_us": 1787103002987654,
  "payload": {
    "price": {"mantissa": 3530, "scale": 3},
    "quantity": 1200,
    "unit": "SHARE",
    "side": "BUY"
  },
  "evidence_sha256": "64 lowercase SHA-256 hex characters"
}
```

`event_id` is the SHA-256 of canonical JSON containing every field above
except `event_id` and `recv_us`. Excluding `recv_us` makes a network redelivery
of the same source fact idempotent. The publisher must durably persist
`source_sequence` across restart.

All wire timestamps and source sequences are unsigned 64-bit integers. Time
is Unix microseconds: compact enough for high-rate tick traffic while retaining
sub-millisecond source ordering. PostgreSQL uses exact `NUMERIC(20,0)` columns
so the full u64 domain is preserved instead of silently narrowing it to i64.
`ts_us` is the source/exchange observation time and `recv_us` is collector
arrival time. They are stored independently because source clocks can be
slightly ahead; clock skew is an auditable quality signal, not a reason to
rewrite the original timestamp.

Supported event types are `TRADE_TICK`, `QUOTE_SNAPSHOT`, `BOOK_SNAPSHOT`,
`BOOK_DELTA`, `BAR`, and `SOURCE_STATUS`. A three-second scrape of an actual
time-and-sales table is a `TRADE_TICK`; a three-second read of only the latest
price is a `QUOTE_SNAPSHOT`.

## Multi-source identity and deduplication

The authoritative uniqueness constraints are:

```text
event_id
(source_id, source_instance_id, source_sequence)
```

Neither constraint contains only the instrument. Therefore the same stock and
timestamp can be stored independently from `ths-web-tick`, `ths-sim-quote`, or
a future licensed vendor. Exact redelivery increments `delivery_count` and is
written to `market_duplicate_deliveries`. A reused event ID or source sequence
with different content is preserved in `market_conflicts` and never overwrites
the accepted event.

## Topics

```text
gridedge/market/v1/{venue}/{symbol}/trade
gridedge/market/v1/{venue}/{symbol}/quote
gridedge/market/v1/{venue}/{symbol}/book
gridedge/market/v1/{venue}/{symbol}/bar/{interval}
gridedge/market/v1/{venue}/{symbol}/status
```

All market events use MQTT 5 QoS 1 and are non-retained. Only status/watermark
messages may be retained by a future publisher.

## Tick and order-book evolution

The three-second interval describes how often a web collector inspects the
time-and-sales page; it is not the event type. Every newly observed row is one
`TRADE_TICK` and carries the source's trade time, price, quantity, aggressor
side when available, and stable row/trade identity in `payload`. A collector
may publish several ticks from one inspection. Reading only the current price
produces `QUOTE_SNAPSHOT` instead and must never invent quantity.

Future depth collectors use `BOOK_SNAPSHOT` for a complete ordered set of bid
and ask levels and `BOOK_DELTA` for one source-sequenced change. Source sequence
and source instance make gaps and restarts observable; a later snapshot is the
only legal way to repair a detected delta gap. The JSONB/raw-byte payload can
hold volume, trade IDs, order counts and multiple depth levels without merging
facts from different `source_id` values.

## Installation and verification

From the repository root:

```sh
./deploy/market_data/install_synology.sh
./deploy/market_data/install_mac_shadow.sh
```

The first command is idempotent: it preserves the PostgreSQL and Mosquitto
data directories, applies forward SQL migrations, and copies only the CA
certificate plus publisher credential back to the Mac. The CA private key is
never mounted into a running service container. The second command installs an isolated Python
environment and the read-only LaunchAgent `com.gridedge.market-shadow`; it
starts at 09:25 on weekdays and tails the existing `002256-quotes.jsonl`.

Automated local contracts are:

```sh
python3 -m unittest \
  deploy/market_data/ingestor/test_market_ingestor.py \
  deploy/market_data/publisher/test_shadow_publisher.py
```

Deployment certification additionally publishes two sources for the same
instrument over real TLS/MQTT 5, repeats one exact payload, reuses one source
sequence with changed content, and sends one message without the required
content type. The expected database deltas are respectively unique events,
duplicate delivery, conflict, and rejection. After all three containers are
restarted, replaying the exact original bytes may only increment duplicate
delivery; it must never create another unique event.
