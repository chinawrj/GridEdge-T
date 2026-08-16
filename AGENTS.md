# GridEdge-T contributor guide

## Structure

- `src/domain.rs`: money-safe domain types and account/order state machines.
- `src/grid.rs`: geometric grid calculation and level state machine.
- `src/event.rs`, `src/journal.rs`, `src/ledger.rs`: versioned projections, SQLite persistence, and the sole prevalidated application writer.
- `src/rights.rs`: tranche-backed grid-right capacity and gating coordination between mechanical levels and ledger writes.
- `src/decision.rs`, `src/gate.rs`, `src/risk.rs`, `src/profit.rs`: versioned algorithm contract, compatibility gates, pre-trade controls and the mechanical no-loss proof.
- `src/bin/gridedge_algorithm_probe.rs`: reference Rust subprocess-algorithm protocol implementation.
- `src/data.rs`, `src/execution.rs`, `src/service.rs`: replay feed, independent paper execution, and durable workflow orchestration.
- `src/main.rs`: CLI only; domain code must not depend on it.
- `configs/`, `docs/`, `tests/fixtures/`: configuration, decisions and deterministic data.

## Commands

```sh
cargo build
cargo run -- validate-config --config configs/default.yaml
cargo run -- replay --config configs/default.yaml --data tests/fixtures/sample.csv
cargo run -- web --config configs/default.yaml --data tests/fixtures/sample.csv
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

## Non-negotiable rules

- Read and preserve the product semantics in `GOAL.md`. If an implementation or UI interpretation conflicts with it, `GOAL.md` wins and the executable cases must be updated first.
- Financial amounts and prices use `rust_decimal::Decimal` or explicit integer minor units, never binary floating point.
- Strategies create `OrderIntent`; they never mutate cash, positions or lots.
- Only idempotent fill events change cash and positions.
- Every external event needs a stable idempotency key. Never use future market data.
- State-machine changes require tests. Event-schema changes must remain replay-compatible.
- Preserve the append-only journal. Derived state must be rebuildable from it.
- Preserve tranche conservation: minted must always equal available + reserved + consumed + revoked + expired; transfer changes ownership only.
- Never sell a loss-making lot slice. One explicit lot+tranche+quantity allocation must drive reservation, order, fill, cost allocation and tranche consumption; every realized slice PnL must be non-negative after all fees.
- Mathematical-rights semantics require approval from the fixed mathematics and quantitative-review seats before release certification.
- This project is research/backtest/paper-trading software only. Never add live-broker execution without an explicit new project phase and safety review.
- Before completion run format, Clippy, all tests and an end-to-end replay.
