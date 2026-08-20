# Event model

Every event contains `event_id`, `event_type`, `schema_version`, `run_id`, `cycle_id`, `symbol`, exchange `event_time`, journal `recorded_at`, monotonic `sequence_number`, correlation/causation IDs, stable `idempotency_key`, JSON payload, and `config_version`.

SQLite uniquely constrains `(run_id, idempotency_key)` and `(run_id, sequence_number)`. An exact duplicate is counted but has no business effect. Reusing a key with a different type, cycle, symbol, time or normalized payload is rejected as a conflict. Related event groups are prevalidated and inserted in one `IMMEDIATE` transaction. Database triggers reject update and delete operations.

Schema versions are selected per event type. `MARKET_DATA_RECEIVED` v1 is upcast-compatible with historical ledgers; v2 separates receipt from completion. A committed golden v1 fixture is rebuilt in tests. Unknown future versions fail closed before mutating the projection. Decimal values remain JSON strings.

The durable bar workflow is `MARKET_DATA_RECEIVED → decisions/orders/fills → MARKET_BAR_DECISIONS_COMMITTED → MARKET_BAR_PROCESSED`. Only the processed event advances `last_price` and the replay cursor. Recovery can therefore resume any partially completed bar without dropping or duplicating its work.

Rights use explicit `GRID_RIGHT_*` events for grant, platform residual, algorithm defer, platform block, reserve, partial exercise, exercise, release and expiry. Current `GATE_DECISION_MADE` schema 4 stores decision contract v4's canonical 20-bar market evidence, rational resource multiplier and exact `C/P/A/B0/X/E/D/B1/I/R` partition, so its SHA-256 input hash can be independently recomputed. Historical schema 3 remains replay-only with the whole-unit v3 `C/P/A/E/D` contract and its original context hash. Terminal right dispositions add the approval fields: `GRID_RIGHT_BLOCKED` and `GRID_RIGHT_RESERVED` use schema 4 for new writes and recompute BUY approval with one cumulative minimum commission. Their historical schema 3 remains replay-only and retains the former two-minimum approval boundary. Current order intents use schema 7, must be whole `standard_quantity` units and carry a typed `GRID_RIGHT` or `INITIAL_DEPLOYMENT_V1` origin. An initial-deployment intent is valid only immediately after its unique `INITIAL_DEPLOYMENT_EVALUATED` fact and must bind the still-unprocessed `MARKET_DATA_RECEIVED` event ID and canonical bar SHA-256. BUY reservation in schemas 6 and 7 is worst-case order notional plus one cumulative minimum commission. Historical order-intent schemas 1–6 remain replayable with their exact recorded contract; schema 5 specifically retains the former two-minimum reservation and cannot be silently reinterpreted as a current write. Decision schema 2 likewise remains replay-only and keeps its original board-lot quantities and legacy context hash. `CONFIG_SNAPSHOTTED` includes a canonical content hash that excludes only the operational database path.

`REPLAY_INITIALIZED` binds a run to dataset ID, SHA-256, bar count and time range. It has no direct accounting effect.

`PLATFORM_UPGRADE_AUTHORIZED` and `PLATFORM_UPGRADE_ACTIVATED` form an append-only platform identity
transition without replacing `ALGORITHM_REGISTERED`. Authorization binds the current effective platform SHA,
an unused target SHA, unchanged algorithm/config identities, journal head, certification evidence and explicit
operator reason. Activation must be the immediately following fact and binds the authorization event, a full-log
business-state digest and an independently reconciled Paper digest. At most one authorization may be pending;
forks, historical-target downgrades, duplicate activation, intervening business events and non-platform manifest
changes fail closed. Replayers derive the effective algorithm manifest by applying this chain in order; historical
bootstrap remains immutable.
