# GridEdge-T fixed-quantity certification v8

- Run: `certification-3y-quantity-v8-0815`
- Symbol/data: `002256.SZ`, 5-minute bars, 2023-08-14 through 2026-08-14
- Mechanical unit: `standard_quantity = 6000` shares (`lot_size = 100`)
- Source dataset SHA-256: `f68e60309c7af91a3805f7144f6b7fec07fab4274bd5eb6102c051151ce19e7b`
- Frozen platform SHA-256: `7ae373b1f546d92b32acf72ff694f8dcc03cc94c7ea810c764e9f33551c4a61b`

## Verification

- SQLite integrity: `ok`
- Journal: 110,522 events, sequence 1…110,522, no gaps or duplicate sequence numbers
- Market workflow: 34,944 unique bars; received/decisions-committed/processed each exactly once and strictly ordered
- Opportunity log: every touch resolves to one grant or explicit skip; every grant has exactly one decision; violations `0`
- Decision partition: `exercise_quantity + defer_quantity = available_quantity`, non-negative and board-lot aligned; violations `0`
- Current event schemas: rights/tranches/decisions v2, order intents v4, fills v3
- New legacy-budget events: `0`
- BUY marginal mints: each exactly 6,000 shares; violations `0`
- Orders: 37, all final `FILLED`; all intent quantities are integral fixed-share units
- Lots: 31, all closed; negative realized lot PnL violations `0`
- Tranches: 54; double-entry conservation violations `0`
- Final account: cash `323802.12`, fees `497.88`, position/sellable `20000/20000`, realized grid PnL `23802.12`
- Independent Paper account equals strategy projection exactly; reconciliation matched with no differences
- Full-log and latest-snapshot rebuild: sequence 110,522, matched
- Historical rebuild compatibility: v3, v5, v6 and quantity-v7 all snapshot/full matched
- Strict gates: format and Clippy clean; 90 Rust tests and 7 reactive-web tests passed
- Executable opportunity contract: 24 named grid paths plus restart, duplicate-input, metamorphic and randomized layer-stack expansion

This artifact certifies research/backtest/Paper behavior only. It is not approved for live brokerage.
