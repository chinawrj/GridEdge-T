# Recovery and reconciliation

On startup, SQLite integrity is checked and snapshots are scanned newest to oldest. A checksum-valid snapshot must match the run and cannot be ahead of the journal. A corrupt newest snapshot falls back to the latest valid predecessor and replays its tail; if every snapshot is corrupt, recovery rebuilds from the full log. Either fallback appends exactly one durable `READ_ONLY` transition. Repeating recovery remains reusable and does not append a second fallback transition, although each successful recovery may append its own `RECOVERY_COMPLETED`. Snapshots are immutable and stale writers cannot save one.

A schema-1 history without `ALGORITHM_REGISTERED` remains available to status and full-log rebuild for audit compatibility only. It cannot be opened as `GridAutomationService`, continued by replay, or resumed by an operator; all such attempts fail before appending an event. Historical readability is not authority to execute an unaudited algorithm.

Recovery verifies the canonical configuration hash and refuses strategy/configuration drift. The recent-close feature window is rebuilt from processed-bar events. Paper rejection, partial-fill and slippage outcomes are pure functions of seed plus stable intent ID, so restarting does not reset a random cursor.

Stable client intent IDs and the independent broker ledger close external side-effect windows. A created intent is submitted; a submitted intent is idempotently queried or submitted; accepted/partially-filled orders replay missing broker reports. Unresolved external state moves the run to `READ_ONLY` instead of guessing.

Single-step progress is the count of `MARKET_BAR_PROCESSED`, never merely received bars. The input dataset is re-hashed against `REPLAY_INITIALIZED` before continuing. The dashboard receives only the processed prefix, so future bars are not rendered.

Fault-injection certification covers receipt, intent commit, submission, broker side effect, acceptance, each fill, decision commit and bar completion. Every recovery path must match the uninterrupted event payloads, rights, orders, lots, cash and positions.

A discrepancy produces `RECONCILIATION_COMPLETED` and keeps the run `SAFE`; a lifecycle stop event cannot overwrite that safety mode. Returning to `RUNNING` requires a clean independent reconciliation, no unfinished orders, no ledger change after that reconciliation, and an explicit operator reason recorded by `SERVICE_MODE_CHANGED`. The reconciliation audit binds the broker source, broker snapshot SHA-256 and checked ledger sequence. Recovery never silently promotes a stopped or safe run.
