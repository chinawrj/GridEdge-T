# 2026-09-01 orchestrator-exit afternoon availability incident

## Outcome

The paper-trading day is operationally unsuccessful. The formal worker had no
events or completed bars from 13:00 through 15:00, so the strategy was not
evaluated during the entire afternoon session. The only two
`GRID_LEVEL_TOUCHED` events occurred at 10:05 and 10:35 while the formal path
was unstable/read-only; neither produced an order intent. No order, submission
or fill occurred during the day.

The terminal Paper facts remain cash `103418.530`, position/sellable `27500`,
and zero open, unknown or unresolved ambiguous orders. Ledger/outbox are
synchronized at `1307/1307` after the post-reboot recovery record. This
reconciliation proves that no unintended money action occurred; it does not
make the missing afternoon service successful.

## Timeline (Asia/Shanghai)

- 09:00–11:28: the installed 0.6.30 path repeatedly transitioned between
  `RUNNING` and `READ_ONLY` because of source-observation catch-up/timeouts.
- 10:05 and 10:35: two grid levels were touched. Both occurred without order
  eligibility and created no `ORDER_INTENT`.
- 11:25: the final completed formal bar was OHLC
  `3.41/3.42/3.41/3.42`, volume `110300`, amount `376681`; it completed the
  received, decisions-committed and processed stages.
- 11:28: the formal market raw stream stopped at source sequence `19919` and
  the trusted worker stopped producing events. Ledger/outbox reached
  `1306/1306`.
- 12:30: the last pre-afternoon automation heartbeat ran.
- 12:47:24: macOS recorded the Codex/ChatGPT application as a voluntary exit.
  The Codex app-server stopped with it. There was no scheduler process left to
  dispatch the 13:00, 13:30, 14:00, 14:30 or 15:00 heartbeats.
- 13:00–15:00: no formal bar, strategy evaluation, worker recovery or isolated
  0.6.44 live E2E occurred.
- 16:00:58: the host rebooted. This reboot happened after the market closed and
  therefore does not explain the afternoon outage.
- 16:03:02: startup appended `RECOVERY_COMPLETED` sequence `1307`, with no
  unfinished orders, and synchronized the outbox. Subsequent startup attempts
  failed on broker reachability, Android landing-page readiness and the
  existing three-attempt Android runner circuit breaker.
- 16:05: the next automation heartbeat finally arrived and detected the
  outage. AC power was connected and the battery was charging.

## Root cause

The operating model incorrectly made the Codex desktop process and its
30-minute heartbeat scheduler the only owner of time-critical restoration.
The 12:30 patrol ended with future work deferred to 13:00, but the application
then exited voluntarily at 12:47. Because no app-independent trusted-session
guard had been left running, neither the worker nor the planned isolated E2E
was started for the afternoon session.

The old 0.6.30 source-observation defect explains why the worker needed
recovery. It does not explain why recovery was absent for two hours; that is an
orchestration design failure.

## Required prevention and remaining evidence

- A time-bound action later in the same trading day must not be left solely to
  a future heartbeat. Before ending the preceding patrol, keep the patrol alive
  or start a reviewed app-independent trusted-session guard that persists when
  the Codex GUI exits.
- The guard may only start the exact installed paper runner. Every restart
  enters `READ_ONLY`, preserves the Android circuit breaker and all identity,
  market, ledger and reconciliation gates, and never synthesizes historical
  orders.
- The AI heartbeat remains the diagnostic and repair supervisor, but is no
  longer a runtime dependency for keeping or restarting the trusted worker.
- Candidate 0.6.44 remains unpromoted. Its two fresh physical-isolation live
  READ_ONLY E2E rounds must start automatically in the earliest reviewed
  market window. Post-close evidence cannot satisfy that gate.
- The Android circuit-breaker evidence must be preserved. The reviewed
  emulator may be recovered with a bounded data-preserving cold boot, followed
  by the full read-only identity/orders/fills/cancellable preflight; the
  circuit breaker must not be silently deleted or bypassed.

## Post-close mitigation status

- The scheduler investigation is conclusive: Codex app-server logs stop at
  12:47 and resume only after the 16:00 reboot; macOS reports the application
  exit as voluntary rather than a crash or forced termination.
- An independent regression was added first and failed because no
  app-independent guard existed. `deploy/run_ths_trusted_session_guard.sh` was
  then implemented. It is intended to run inside the trusted tmux worker
  session, survives the Codex GUI, adopts an already-running single worker,
  rejects multiple workers, launches only the exact installed runner, and
  never resets `android-runner-failures`.
- Shell syntax, the dedicated guard regression, all 48 `ths_live` integration
  tests, `cargo fmt`, strict Clippy and all Rust targets/features passed.
- The host's reviewed Android AVD was recovered with a data-preserving cold
  boot using no snapshot load/save and the software renderer. The installed
  binary then completed the full identity/orders/fills/cancellable read-only
  preflight and reproduced execution identity
  `d43357f7cf3ebb1e581f8a2cc6f3b319f9a8b6a5dfae31bb20a582029300ff7c`.
  The failed-attempt record `2026-09-01 3` remains preserved; it was not reset.
- At 16:29, AC power was connected and charging, the broker port was reachable,
  ledger and outbox integrity were `ok`, and head/cursor were `1307/1307`.
- The guard source is not a production release until the required independent
  test review and normal deployment step are complete. Candidate collector
  0.6.44 likewise remains blocked on the two fresh live physical-isolation
  rounds; neither condition may be waived because the market is closed.
