# GridEdge-T fixed-quantity certification v10

- Run: `certification-3y-quantity-v10-0815`
- Symbol/data: `002256.SZ`, 5-minute bars, 2023-08-14 through 2026-08-14
- Mechanical unit: `standard_quantity = 6000` shares (`lot_size = 100`)
- Source dataset SHA-256: `f68e60309c7af91a3805f7144f6b7fec07fab4274bd5eb6102c051151ce19e7b`
- Frozen platform SHA-256: `68ab4ee32f4663aecc2b6296b79bf95717b3f3dd096157c8656ad5c94539d980`

## Verification

- SQLite integrity: `ok`
- Journal: 110,590 events, sequence 1…110,590, no gaps, duplicate sequence numbers, event IDs or idempotency keys
- Market workflow: 34,944 unique bars; received/decisions-committed/processed each exactly once and strictly ordered
- Opportunity log: 1,427 physical opportunities; every touch resolves by strict XOR to one grant or one explicit skip; duplicate physical opportunities and resolution violations `0`
- Opening gaps: canonical directional crossing is shared by GridEngine, Service and Ledger; the 2023-12-01 09:35 downward gap records both crossed levels `-2,-3`
- Grants: 40; each has exactly one typed decision, a hash-anchored canonical capacity, a real market crossing and an atomic touch; violations `0`
- Decision partition: `exercise_quantity + defer_quantity = available_quantity`, non-negative and board-lot aligned; violations `0`
- Current event schemas: market/configuration/rights/tranches/decisions v2, order intents v4, fills v3
- New legacy-budget events and budget tranches: `0`
- Order intents/orders: 38; all quantities are positive integral multiples of 6,000 shares; all orders final `FILLED`
- Lots: 31, all closed; negative realized lot PnL violations `0`; exact lot PnL sum equals state realized PnL `23502.44`
- Tranches: 53; quantity and budget double-entry conservation violations `0`
- Final account: cash `323502.44`, fees `497.56`, position/sellable `20000/20000`, realized grid PnL `23502.44`
- Independent Paper account equals strategy projection exactly; reconciliation matched with no differences; open orders `0`; broker snapshot SHA-256 `54e82b9d2c501d77191a927ace0947ba18dfc9567b7989dc1ad3fb25aedf9552`
- Frozen-binary full-log and latest-snapshot rebuild: sequence 110,590, matched
- Historical rebuild compatibility: v3, v5, v6, quantity-v7, quantity-v8 and quantity-v9 all snapshot/full matched
- Strict gates: format and Clippy clean; 101 Rust tests and 7 reactive-web tests passed
- Executable opportunity contract includes 24 named grid paths plus restart, duplicate-input, metamorphic, randomized layer-stack, SAFE/READ_ONLY reversal and opening-gap expansion
- Adversarial ledger cases reject missing/forged configuration, future/duplicate market stages, phantom or retroactive touch, omitted rearm, incomplete cycle topology, ghost carry, forged SELL partitions/price, unbacked reservation and schema downgrade without changing state or sequence

This artifact certifies research/backtest/Paper behavior only. It is not approved for live brokerage.
