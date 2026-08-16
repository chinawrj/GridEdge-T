# Platform implementation status

## Complete in this phase

- Pure Rust crate and `gridedge` binary; no Python implementation.
- Decimal geometric grid, trade/boundary levels, crossing ambiguity policy, hysteresis and rearming.
- First-class buy/sell `GridRight` state machine with typed exercise/defer decisions, proportional exercise, deeper-level-only carry-forward, explicit T+1/risk-blocked capacity, conservation checks, reservation, release and expiry.
- Separate ledger writer, rights/gating coordinator and execution workflow boundaries; illegal event plans are rejected before append.
- Full decision-context audit, boundary validation, deterministic seed, platform-measured process identity, isolated process group, deadline and resource/output caps.
- Versioned append-only SQLite journal with `FULL` durability, prevalidated atomic batches, expected-sequence fencing, strict idempotency conflicts and immutable snapshots.
- Crash-resumable bar workflow and persistent independent Paper Broker ledger; deterministic rejection, partial fill and slippage outcomes.
- Frozen cash/shares, fill aggregation invariants, per-order commission, T+1 at three boundaries, allocated-cost P&L and explicit operator resume.
- Snapshot fallback, configuration content binding, restored feature windows, v1 compatibility fixture and unknown-schema fail-closed behavior.
- Validated minute CSV replay, replay/system clocks, pause/resume/stop feed interface, one automation service for replay and paper execution.
- All requested CLI command names, terminal/JSON reporting, deterministic synthetic scenario and automated coverage of core acceptance semantics.
- Loopback-only Rust web console with fixed-host/origin/CSRF controls for graphical inspection and audited replay, rebuild, reconciliation and operator resume.

## Deliberate limitations

- No real broker adapter, credentials, network orders or GUI automation.
- Minute OHLCV cannot reveal the true intrabar path; conservative handling remains the default.
- The Paper Broker fills immediately after optional simulated latency and does not model an exchange queue/order book.
- Sell capacity is intentionally lot/share based rather than a synthetic currency allowance.
- Base-position unrealized P&L is unavailable because no base cost basis is configured; reported unrealized P&L covers strategy lots.
- Physical database repair, multi-symbol portfolio scheduling, exchange queue simulation and live execution remain outside scope.
- The built-in in-process gate adapter is experimental; production algorithms should use `ProcessAlgorithm`.
- Stateful process algorithms are not yet admitted: `supports_checkpoint=true` is rejected until a durable versioned checkpoint/restore protocol is implemented.
