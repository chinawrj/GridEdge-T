# Architecture

GridEdge-T is a Rust research platform whose authoritative model is an append-only event ledger. The same automation aggregate serves batch replay, single-step replay and simulated paper execution. `StrategyState` is only a rebuildable projection; cash and positions never change from a strategy decision or order intent, only from a validated unique fill.

The current core decision boundary is `DecisionRequestV3 → DecisionResponseV3`. A request contains one stable `GridRight`, its full point-in-time `GateContext`, a deterministic seed and the immutable quantity partition `C = A + P`, where `P = C mod standard_quantity` and only `A` is offered to the algorithm. A response has typed exact quantities: `EXERCISE { exercise_quantity, defer_quantity }` or `DEFER { defer_quantity }`, with `E + D = A`. The platform then records one disposition fact with `I = E - B` and `R = D + B`. It validates identity, hashes, versions and that every active quantity `A/E/D/B/I/R` is a whole algorithm unit before it can create an intent. Earlier decision schemas are replay-only; their board-lot quantities are historical facts, not current fractional units.

The application is split into four explicit layers:

1. `ledger` is the sole application write boundary. It pre-projects the entire event or batch, runs cross-aggregate invariants, rejects incomplete right/tranche sets and illegal state transitions before append, and then performs expected-sequence CAS against `journal`. `journal` publicly exposes read ports only; raw append and snapshot functions are crate-private.
2. `rights` is the grid-right/gating coordinator between the mechanical grid and the ledger. The algorithm sees a simple aggregate share capacity, while the authoritative inventory is a set of source tranches. A current buy tranche is one directed crossing's fixed `standard_quantity`; a sell tranche is one concrete lot/quantity slice. Every tranche conserves `minted = available + reserved + consumed + revoked + expired`. Carry changes ownership without copying a balance, fill allocation is `DEEPEST_FIRST_V1`, and a one-grid reversal revokes only the unconsumed marginal tranche. Budget-denominated buy tranches remain readable solely as a versioned legacy ledger format.
3. `profit` is a mechanical sell-side boundary before the decision algorithm. It proves every concrete lot slice against its remaining buy cost, worst allowed fill price, commission, minimum commission and sell tax. A losing or unknown-cost slice remains available and is labelled below break-even; it is never exposed as exercisable quantity.
4. `service` owns the durable workflow and delegates broker effects to `execution`. The grid-algorithm boundary is touch → right → exact algorithm decision → pre-order risk → ledger → order intent. Execution, fill and reconciliation continue as a separate state machine; strategies never write account state directly.

Derived arithmetic is a pre-write boundary. Grid generation validates every bounded node before migration/bootstrap, including Decimal multiplication/division, positive tick-rounded price and a finite configured level-count budget. Position and quantity limits use checked subtraction/addition rather than potentially wrapping sums. The ledger projection and independent Paper gateway both validate a complete fill transition before committing any journal, account or report row; a numeric overflow is a normal domain error with zero side effects.

Same-side OHLC round trips are path-ambiguous too. If one bar can both reverse an active tranche and reach a deeper level, the conservative policy records the ambiguity and makes no rights-inventory mutation.

Two algorithm adapters exist:

- `ProcessAlgorithm` is the production boundary for deterministic stateless algorithms. A Rust algorithm executable performs a manifest handshake and receives JSON requests in an isolated process group with wall/CPU/output limits. The platform measures and journals the executable SHA-256, exact argument vector and controlled-environment hash; recovery rejects a changed artifact or invocation.
- `GateAlgorithmAdapter` is an explicitly experimental in-process compatibility adapter for the built-in gates. Panic and timeout are contained at the call boundary, but a permanently hung in-process thread cannot be forcefully reclaimed.

`supports_checkpoint=true` is deliberately rejected in this version. A stateful algorithm will only be admitted after a versioned checkpoint/restore protocol can bind checkpoint hash and ledger sequence. This prevents a manifest from claiming recovery semantics that the platform cannot yet prove.

SQLite uses WAL plus `synchronous=FULL`. Every append carries the aggregate's expected last sequence, so a stale second writer is fenced by compare-and-swap. Event batches are projected on a clone before commit. Idempotency keys accept byte-equivalent business events only; the same key with different contents is a hard conflict.

The Paper Broker has its own persistent account and order ledger. Stable intent IDs make submit idempotent. Recovery queries that independent ledger to resolve external-side-effect windows, and reconciliation compares two independently persisted views.

The local web control plane fixes the expected loopback Host at startup, requires an exact same-origin header and an unpredictable server token for every mutation, and uses SameSite cookies plus restrictive response headers. A clean reconciliation is bound to its broker-source hash and ledger sequence; any later state change invalidates operator resume.

The repository intentionally has no live-broker adapter. Adding one is a separate safety-reviewed project phase.
