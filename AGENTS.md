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

## Operational objective

- GridEdge-T is an unattended research, backtest and paper-trading system supervised by AI. Safety and trading-session availability are both product requirements; a system that is safely stopped because of an ordinary recoverable fault is not operationally successful.
- Operational rules in this file govern orchestration and recovery. They do not weaken the strategy, accounting, mathematical-rights or no-loss semantics in `GOAL.md`.
- Existing user authorization to start and control Chrome, the reviewed market-data page, the reviewed paper-trading app and the paper worker is durable until the user revokes it. Do not repeatedly ask for the same permission during patrols. This permission never extends to a real-money account or to bypassing an operating-system security boundary.

## Trading-session availability

- Use `Asia/Shanghai` and the reviewed A-share trading calendar. On every trading day, finish dependency and identity preflight by 09:25. From 09:30-11:30 and 13:00-15:00, keep the reviewed market collector, MQTT/DB path, paper-account adapter and strategy worker operational. By 09:35, the current session must have produced and processed a completed five-minute bar when the market-data source reports normal trading.
- During a trading session, a stopped worker, missing Chrome/tab/extension, missing PostgreSQL application ACK, or market/status watermark more than 60 seconds stale is a P0 availability incident. It must not be described as "the strategy did not trigger" or "there was no trading opportunity." If there was no executable bar, state that the strategy was not evaluated and identify the upstream cause.
- Recover ordinary process and dependency faults automatically when durable authorization exists: restart Chrome and the reviewed page, reload the reviewed extension, reconnect MQTT/DB, recreate the trusted worker session and repeat read-only preflight. Never wait for a routine patrol or user reminder before attempting recovery.
- Do not bypass a locked screen, OS permission boundary or account-identity check. A locked computer is a real external blocker; preserve evidence, keep order submission stopped and notify the user once with the exact required action.
- Fail closed only for safety-critical evidence: wrong or non-paper account identity, real-account evidence, unknown/open remote contracts, unresolved `AMBIGUOUS` state, ledger/outbox integrity failure, installed/effective artifact identity mismatch, future/gapped/reordered market data, or Paper/remote state inconsistency. Stop order submission, preserve the journal/outbox/evidence and diagnose immediately.
- A missing process, closed browser, disconnected extension, transient UI snapshot, stale connection or failed launcher is recoverable unless it produces one of the safety-critical conditions above. A failed LaunchAgent is not sufficient reason to remain stopped when the reviewed trusted-session runner is available.
- An Android emulator that fails to boot or renders a corrupted/garbled screen is a recoverable startup fault. Perform a bounded cold boot of the reviewed AVD: stop the emulator cleanly, preserve its user data, move or invalidate the Quick Boot snapshot, restart with snapshot loading/saving disabled and a reviewed software renderer, then repeat the read-only identity/orders/fills/cancellable preflight. Never use `-wipe-data` automatically.
- Market-hours recovery takes priority over refactoring, replay experiments and new feature work. Perform development in isolated fixtures or shadow state so it cannot displace the production paper worker.
- Startup and reconnect backlogs are always `READ_ONLY`: rebuild bars and state but never synthesize or catch up historical orders. Return to `RUNNING` only after a fresh, contiguous current-session receipt, an application-level committed DB ACK and terminal paper-account reconciliation.
- Never duplicate the initial position, repeat an uncertain click, or create a test order during a production patrol. Order submission requires one durable intent and idempotent reconciliation through a terminal state.

## AI patrol contract

- Every patrol owns the full loop: detect, diagnose, add an independent regression test for a reproducible defect, obtain the required review, fix, run gates, build/sign, activate through the same-run upgrade path, and restore the paper worker. Merely reporting that the worker is stopped is not a completed patrol.
- Each trading-session report must include: local time; collector/provider version; source sequence and watermark; PostgreSQL committed-ACK status; latest completed bar; current and observed-minimum cash; position and sellable quantity; open/unknown/ambiguous orders; ledger head and outbox cursor; installed/effective artifact identity; worker mode; and the recovery action actually taken.
- Carry durable permissions and the latest signed identities in the automation baseline. Patrol context loss, a new heartbeat or a new trading day must not erase prior authorization or revert to obsolete artifacts.
- Keep forensic evidence free of credentials and personal account details. Record hashes or masked identity markers instead of secrets.
- A quiet patrol is allowed only when the complete health contract is satisfied and no user action is required. Any missed readiness deadline, unprocessed current bar or blocked recovery must be reported immediately.

## Operational definition of done

- Passing unit tests or completing an adapter implementation is not enough. During market hours the exact signed installed artifact must equal the ledger's effective artifact, the reviewed paper account must reconcile, the market source must be fresh and contiguous, PostgreSQL must issue a committed application ACK, and the worker must process current bars without an unattended blocker.
- After 09:35 on a normal trading session, absence of a current completed bar is an operational failure even when no order would have been generated. The system must prove that the strategy evaluated the bar before attributing zero orders to market conditions.
- Any new market-data or execution-adapter path requires at least two consecutive full replay/recovery runs with no defect that would stop unattended paper trading, followed by a real read-only preflight. Every production incident requires a dedicated automated regression and test-expert review before release.
- Normal post-close shutdown is healthy only after market-data watermarks, completed bars, orders, account state, ledger and outbox have been reconciled and reported.
