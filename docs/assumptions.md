# Assumptions and limitations

- Research, backtest and simulated paper trading only; no real broker login or order path exists.
- One configured A-share symbol and one active grid cycle per run.
- The symmetric grid is `anchor × (1 + ratio)^k`; negative levels divide by the same factor. Levels ±1…±4 trade and ±5 are observation/risk boundaries.
- The first bar initializes crossing state. Intrabar paths are unknown. Ambiguous opposing touches default to conservative skipping; alternatives are configurable.
- Replay accepts regular A-share continuous-auction minutes (09:30–11:30 and 13:00–15:00, Asia/Shanghai expressed as naive local timestamps). It rejects non-increasing or duplicate timestamps.
- Paper outcomes are deterministic hashes of seed and intent ID. Fills use touched price plus configured adverse slippage. Minimum commission is aggregated per order, not per partial fill.
- Current buy and sell rights are fixed-share-quantity capacity. One unit is exactly `standard_quantity` shares; algorithms return exact exercised and deferred quantities whose sum equals the granted capacity. Currency is settlement metadata only. Pre-quantity budget rights are a read-only historical replay format.
- Snapshots are accelerators, not authoritative data. A damaged latest snapshot falls back to an older valid one in read-only mode; journal corruption stops recovery.
- The production algorithm boundary is a killable local process protocol. OS-level CPU/memory sandboxing and remote algorithm scheduling are not implemented.
- The simple rule gate is transparent demonstration logic, not a claim of predictive performance.
