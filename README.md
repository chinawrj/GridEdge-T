# GridEdge-T

GridEdge-T is a Rust platform for one-symbol A-share grid-strategy research. Its core abstraction is the auditable right granted at each grid crossing: an algorithm chooses exact `Exercise(q)` and `Defer(q)` quantities, while platform risk controls can independently block an intended order. Append-only accounting, deterministic replay and crash recovery take priority over prediction sophistication.

The mechanical unit is a fixed share quantity: `1份 = standard_quantity 股`. Market price changes settlement cash, fees and P&L, but never changes how many shares one authorization unit contains. Historical budget-denominated ledgers remain replayable and are not valid formats for new writes.

> **Research/backtest/paper trading only.** This repository has no real-broker login or live-order implementation and must not be used to execute real trades. It does not provide investment advice.

## Quick start

Install a current Rust toolchain, then:

```sh
cargo build
cargo run -- init-db --config configs/default.yaml
cargo run -- validate-config --config configs/default.yaml
cargo run -- validate-data --config configs/default.yaml --data tests/fixtures/sample.csv
cargo run -- fetch-klines --symbol 002256.SZ --start 2023-08-14 --end 2026-08-14 --frequency 5 --adjustment none
cargo run -- generate-grid --anchor 10
cargo run -- replay --config configs/default.yaml --data tests/fixtures/sample.csv
cargo run -- status --config configs/default.yaml
cargo run -- ledger --config configs/default.yaml
cargo run -- orders --config configs/default.yaml
cargo run -- positions --config configs/default.yaml
cargo run -- reconcile --config configs/default.yaml
cargo run -- resume --config configs/default.yaml --run-id RUN_ID --reason "broker discrepancy investigated"
cargo run -- rebuild-state --config configs/default.yaml
cargo run -- compare-baseline --config configs/default.yaml
GRIDEDGE_API_TOKEN=local-token cargo run -- web --config configs/default.yaml --data tests/fixtures/sample.csv --port 8790
GRIDEDGE_API_TOKEN=local-token .venv/bin/gridedge-web
```

Every reporting command supports `--json`. Pass `--run-id ID` to address a particular run. `run-paper` consumes the same validated CSV source as `replay` in this MVP and exercises the identical automation service and Paper Broker.

The local dashboard opens at `http://127.0.0.1:8787/`. It is a reactive Python/NiceGUI frontend backed by an authenticated Rust JSON API on port 8790. The browser never writes SQLite: every command crosses the Rust single-ledger-writer boundary. State and chart data update dynamically without full-page refresh. A step replay can be closed and resumed because its cursor is derived from journaled market events rather than browser memory. Both layers bind to the local computer only and contain no live-broker action.

## 兆新股份三年数据集

The repository includes a validated BaoStock snapshot for `002256.SZ`:

- Standard replay data: `data/processed/002256.SZ_5m_raw_20230814_20260814.csv`
- Source-native retention: `data/raw/baostock_002256.SZ_5m_raw_20230814_20260814.csv`
- Machine-readable quality report: `data/reports/002256.SZ_5m_raw_20230814_20260814.quality.json`
- Fixed-quantity replay configuration: `configs/zhaoxin_5m_quantity_v10.yaml`

It contains 34,944 unadjusted 5-minute bars over 728 observed trading days. Use unadjusted prices for execution replay. For return-based research, fetch an additional forward-adjusted snapshot with `--adjustment forward`; never mix adjusted and unadjusted prices in one run.

```sh
cargo run -- validate-data --config configs/zhaoxin_5m_quantity_v10.yaml \
  --data data/processed/002256.SZ_5m_raw_20230814_20260814.csv
cargo run -- replay --config configs/zhaoxin_5m_quantity_v10.yaml \
  --data data/processed/002256.SZ_5m_raw_20230814_20260814.csv
cargo run -- web --config configs/zhaoxin_5m_quantity_v10.yaml \
  --data data/processed/002256.SZ_5m_raw_20230814_20260814.csv
```

The dashboard lets users switch among compatible CSV files in the same data directory and inspect 1-day, 5-day, 20-day, or full-range charts. Choose **创建单步回放** to start at bar zero, then either use **下一根 K 线** or automatic playback at 0.25, 0.5, 1, or 2 seconds per bar. The Rust service advances and snapshots one bar at a time while the reactive page requests updated data without full-page navigation; pausing or restarting resumes from the durable event-ledger cursor. Only consumed bars are rendered, so future prices are not exposed. K-lines, grid state, orders, lots, and audit events appear together on one overall-results page without tab switching. **运行至结束** remains available when a user wants to finish immediately.

## What is implemented

- Symmetric geometric levels `anchor × 1.02^k`, ±4 trading levels and ±5 boundaries.
- Fixed-share-quantity buy/sell rights with grant, defer, risk block, reserve, partial/full exercise, release and cycle expiry.
- Typed quantity contract with exact `exercise_quantity + defer_quantity = available_quantity`, complete input snapshot/hash, deterministic seed and strict response validation.
- Killable Rust subprocess-algorithm protocol with executable/argument/environment identity binding, process-tree cleanup and resource limits, plus experimental built-in gate adapters. Stateful checkpoint claims are rejected until durable checkpoint/restore is implemented.
- SQLite WAL/FULL durability, append-only versioned events, expected-sequence writer fencing, strict idempotency conflicts, atomic prevalidated batches and immutable checksummed snapshots.
- Crash-resumable bar and order workflows, independent persistent Paper Broker ledger, frozen resources, fill invariants and deterministic execution.
- Decimal accounting, board-lot rounding, per-order commission, sell tax, adverse slippage, allocated-cost P&L and A-share T+1 controls.
- Conservative no-lookahead bar handling, explicit SAFE/READ_ONLY modes, independent reconciliation and audited operator resume.
- Modern reactive Python dashboard with a typed Rust API, market/right decision markers, run selector, order/lot views and automatic or manual single-step replay. The Rust core retains authenticated commands, fixed-host/origin controls and the only ledger writer.
- Versioned fixed-quantity opportunity catalog with 24 curated crossing paths, exact `Exercise/Defer`, decision-time tranche dispositions, order-intent sets, forbidden events, duplicate-input and per-bar-restart expansion, plus an independent randomized layer-stack oracle.

## Validation

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
.venv/bin/python -m pytest -q webapp/tests
```

PR 会自动执行相同的 Rust 门禁、隔离 wheel 安装后的 BFF 测试、短回放 E2E，以及
从 checkout 外目录启动完整平台的 smoke test。详见
[`docs/testing_ci.md`](docs/testing_ci.md)。Rust 工具链由 `rust-toolchain.toml` 固定，
Python 依赖由 `webapp/uv.lock` 冻结；更新依赖时必须同步提交相应 lockfile。

Design decisions and limits are documented in [`docs/`](docs/). The simulation retains OHLC intrabar ambiguity and deliberately excludes live connectivity, multi-symbol portfolios and predictive model training.
