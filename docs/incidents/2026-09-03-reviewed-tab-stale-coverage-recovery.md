# 2026-09-03 reviewed-tab stale coverage recovery

## Impact

- The reviewed Eastmoney page stopped advancing trade coverage during the
  afternoon session even though status observations continued.
- The Android daily circuit breaker was already open (`2026-09-03 3`), so no
  formal paper-order worker could be started. The day remains a P0 availability
  failure and no order was submitted.

## Timeline (Asia/Shanghai)

- 13:02 — PostgreSQL showed a same-day discontinuity boundary followed by a
  continuous afternoon stream. Exact replay excluded the partial 13:00 bucket
  and produced the complete 13:05 bucket.
- 13:12 — the reviewed page's latest trade stopped advancing while the source
  observation lane continued, proving that a fresh status observation alone
  did not restore executable trade coverage.
- 13:25 — the reviewed page was restored. The duplicate collector tab was
  removed, the single retained exact-URL tab was marked for handoff, and
  PostgreSQL resumed current trade coverage.
- 13:27 — database continuity was verified from sequence 25143 onward with no
  gaps, future timestamps, or duplicate deliveries. Conflict/rejection totals
  remained at their historical 1/5 baseline.
- 14:32 — a third production reproduction was detected: the visible table had
  stopped at 14:21:24 while status observations were still being committed.
  The patrol reloaded the sole reviewed tab immediately; its latest row advanced
  to 14:33:03 within 15 seconds, and PostgreSQL resumed trade ticks through
  sequence 26904 without a gap, future timestamp, duplicate delivery, conflict,
  or rejection increase.
- 15:02 — close reconciliation found another freeze. Status observations ran
  through 14:59:53, but committed trade coverage stopped at 14:55:15. Therefore
  the 14:55-15:00 bucket is unavailable and must not be synthesized or called
  processed. A post-close read-only reload reached an Eastmoney slider
  challenge; it was not bypassed and no post-close DOM data was ingested.
- 15:06 — the final trustworthy complete bucket is 14:50-14:55 with OHLC
  3.32/3.32/3.30/3.31, volume 1,859,900 and amount 6,153,804. PostgreSQL ended
  at sequence 27520 with the afternoon range continuous and delivery/future
  anomaly counts zero; global conflict/rejection totals remained 1/5. Android
  orders, fills and cancellable lists were empty. Ledger/outbox remained
  1469/1469 and the daily breaker bytes remained unchanged.
- 13:49:53–14:00:25 — production 0.6.50 again emitted no
  `SOURCE_OBSERVED_CURRENT` for 632.209 seconds while trade ticks continued.
  Observation resumed without a sequence gap, but the interval is a second
  direct production reproduction of the unavailable recovery path and exceeds
  the 60-second hard boundary by more than ten times.

## Root cause

`createReviewedTabRecovery` existed but was not reachable from the production
heartbeat path. The heartbeat returned after publishing a status-only source
observation; stale trade coverage was only classified by the regular capture
lane. Consequently the background recovery coordinator never received the
exact `STALE_TRADE_COVERAGE` reason and could not issue its bounded reload.

## Candidate repair

Extension 0.6.51 makes the heartbeat path publish the exact status-only
observation first, then classify trade coverage from that same atomic capture.
Only the exact stale-coverage result requests one reviewed-tab reload through a
session-persisted cooldown. Initialization, delivery failures, and unrelated
errors cannot trigger a reload. A regression first demonstrated that the old
production path was unreachable; the corrected tests cover stale, fresh,
non-ok delivery, thrown errors, cooldown coalescing, and production wiring.

The final extension suite is 189/189. Deployment/ingestor/publisher tests,
local-E2E manager tests, `cargo fmt --check`, strict Clippy, and the full Rust
test suite pass. Independent review first rejected a broad reload classifier,
then approved the corrected exact `STALE_TRADE_COVERAGE` allowlist. The
heartbeat still commits status-only evidence before it requests recovery;
initialization, delivery failure, exception and TOCTOU paths cannot reload.

## Release state

0.6.51 is built and the registered unpacked directory contains the reviewed
bytes, but the running Chrome instance has not yet been authorized to load the
updated local code. No `chrome://extensions` workaround is required: after
explicit confirmation, a normal controlled reload/restart may load the exact
registered directory, followed by popup runtime-identity verification. The
sole production tab still presents Eastmoney's slider challenge, which must be
completed by the user and must not be automated or bypassed. A 2026-09-04
read-only inspection proved that the underlying latest-first table was current
and that the loaded 0.6.50 runtime continued to commit fresh, contiguous source
coverage. This supports the current worker but is not evidence that 0.6.51 is
loaded.

The two required fresh 900-second READ_ONLY physical-isolation rounds completed
PASS with separate UUID nonce roots, loopback-only broker and PostgreSQL,
formal-topic ACL denial, the frozen candidate bundle, and zero Android, order,
strategy or money actions. R1 committed 415 messages and R2 committed 522;
each produced 176 current source observations, two bars and six complete
processing-chain events. R2 also survived a deliberate MV3 service-worker
replacement in 2.109420 seconds. Both exact roots were torn down and formal
PostgreSQL leak counts were zero. The early 07:37 prior-session attempt remains
excluded from acceptance evidence.
