# GridEdge-T fixed-quantity certification v9

- Run: `certification-3y-quantity-v9-0815`
- Symbol/data: `002256.SZ`, 5-minute bars, 2023-08-14 through 2026-08-14
- Mechanical unit: `standard_quantity = 6000` shares (`lot_size = 100`)
- Source dataset SHA-256: `f68e60309c7af91a3805f7144f6b7fec07fab4274bd5eb6102c051151ce19e7b`
- Frozen platform SHA-256: `c560f102ccc67107e2a033f46afa463f809bfbabd07a2255f5e429a61ab9f194`

## Verification

- SQLite integrity: `ok`
- Journal: 110,523 events, sequence 1…110,523, no gaps, duplicate sequence numbers, event IDs or idempotency keys
- Market workflow: 34,944 unique bars; received/decisions-committed/processed each exactly once and strictly ordered
- Opportunity log: 1,414 touches; every touch resolves by strict XOR to one grant or one explicit skip; violations `0`
- Grants: 39; each has exactly one typed decision, a hash-anchored canonical capacity, a real market crossing and an atomic touch; violations `0`
- Decision partition: `exercise_quantity + defer_quantity = available_quantity`, non-negative and board-lot aligned; violations `0`
- Current event schemas: configuration/rights/tranches/decisions v2, order intents v4, fills v3
- New legacy-budget events: `0`
- Order intents: 37; all quantities are positive integral multiples of 6,000 shares
- Orders: 37, all final `FILLED`; execution results are audited separately and are not grid-algorithm case oracles
- Lots: 30, all closed; negative realized lot PnL violations `0`
- Tranches: 51; quantity and budget double-entry conservation violations `0`
- Final account: cash `323802.12`, fees `497.88`, position/sellable `20000/20000`, realized grid PnL `23802.12`
- Independent Paper account equals strategy projection exactly; reconciliation matched with no differences; open orders `0`
- Full-log and latest-snapshot rebuild: sequence 110,523, matched
- Historical rebuild compatibility: v3, v5, v6, quantity-v7 and quantity-v8 all snapshot/full matched
- Strict gates: format and Clippy clean; 92 Rust tests and 7 reactive-web tests passed
- Executable opportunity contract: 24 named grid paths plus restart, duplicate-input, metamorphic and randomized layer-stack expansion
- Adversarial ledger cases reject missing/forged config hash, phantom market touch, contradictory grant+skip, missing decision, ghost carry, forged SELL partitions/price, unbacked tranche reservation and schema downgrade without changing state or sequence

This artifact certifies research/backtest/Paper behavior only. It is not approved for live brokerage.
