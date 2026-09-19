# 2026-09-03 complete-session overlap recovery incident

## Impact

- Detected at 11:02 Asia/Shanghai while the Android daily circuit breaker was
  already open from the separate startup-race incident.
- The formal Eastmoney collector stopped advancing after 10:43:48. The source
  observation was about 1,047 seconds stale, so no executable market-data path
  existed. No paper order, fill, Android write or money action occurred.
- This is a P0 availability incident. It is not a no-signal market outcome.

## Timeline

- 11:02 — detected stale formal PostgreSQL source/observation state; guard,
  artifact identities, ledger/outbox and Android circuit breaker were unchanged.
- 11:03 — first recovery action: reloaded the reviewed 0.6.48 extension and
  refreshed the exact reviewed 002256 time-sales page.
- 11:04 — popup exposed the exact persistent error: `a complete session cannot
  be replaced by a partial resume boundary`.
- 11:05 — diagnosed a same-day recovery loop: live-overlap loss always requested
  a partial resume boundary even when durable state already held complete-session
  evidence; initialization then returned to the same failing path.
- 11:07 — independent red tests proved both the wrong route and missing strict
  complete-history bridge enforcement.
- 11:10 — implemented a dedicated same-day complete-history bridge with fresh
  durable-state routing and strict old-watermark overlap/new-row checks.
- 11:11 — independent review rejected the first patch because bridge conflicts
  could still mutate conflict stores; added a whole-store atomicity regression
  and moved rejection before every write transaction.
- 11:13 — focused regressions passed and independent review approved 0.6.49 for
  build and real READ_ONLY recovery only.
- 11:14 — Chrome actually loaded 0.6.49 and the reviewed page was refreshed.
- 11:15–11:21 — real recovery failed the 45-second boundary: the rolling
  nine-page history never produced stable, globally unambiguous identity and
  stopped at page 6. PostgreSQL remained at source sequence 25142.
- 11:22 — review rejected pagination deduplication because Eastmoney exposes no
  global stable trade identifier; two economically identical trades at a page
  boundary cannot be distinguished from one duplicated row.
- 11:24 — red tests proved the old same-day state machine could not represent a
  safe discontinuity and that an initial implementation lacked conflict
  atomicity and lost-response idempotency.
- 11:36 — 0.6.50 implemented and passed independent review for build and real
  READ_ONLY recovery. It records an explicit same-day discontinuity instead of
  claiming to reconstruct ambiguous history.
- 11:47 — independent final review returned PASS for build/sign, same-run
  staged installation and real READ_ONLY recovery only. It explicitly did not
  authorize RUNNING while the Android breaker remained open.
- 11:48 — the frozen signed worker SHA-256
  `c4f3654e98b1fa39129d5ccb77f4a025b2efc0063b331c78c97079c553146383`
  was authorized against journal head 1466.
- 11:49 — atomic staged publication installed the exact signed binary while
  proving launchd absent and quiescing the old trusted guard. Platform
  activation, outbox staging and the read-only Android execution-identity
  upgrade completed at head/cursor 1469/1469. The new identity SHA-256 is
  `6b9186aa4c0e942a1efc296109af183f489a5cc45ccd94a400b9af10517fa39d`.
- 11:51 — the new installed guard published ready PID 15189 with the reviewed
  guard SHA. The breaker bytes remained exactly `2026-09-03 3\n`, so no formal
  worker or money action was started.
- 11:52 — Computer Use reloaded the unpacked collector and the Chrome details
  page visibly confirmed version 0.6.50. The exact reviewed Eastmoney URL was
  rebuilt and marked for cross-patrol persistence. Lunch-time capture remained
  non-executable; real V2/committed-ACK and next-complete-bucket evidence must
  come from the first valid afternoon rows and will not be simulated.

## Root cause and permanent contract

The durable collector correctly forbids replacing same-day complete-session
evidence with a weaker partial resume boundary. The page recovery coordinator
did not distinguish that durable state from a partial-session state, so its
otherwise safe fallback could never succeed after a sufficiently long page gap.

For a same-day complete session, runtime overlap recovery no longer crawls or
deduplicates rolling history. It captures one reviewed current page and emits an
auditable `SAME_DAY_DISCONTINUITY_BOUNDARY_V2` only when every row is unseen and
strictly newer than the prior durable watermark. Any overlap means continuity
has recovered and the normal live path must be used. Any conflict, old row,
wrong session, wrong predecessor or expired 12-second delivery deadline rejects
before every durable write.

The V2 boundary is idempotent only while durable state remains exactly at the
same capture, prior watermark and boundary watermark. Boundary-page ticks are
sequenced before the status but the Rust bar builder clears them when it applies
the discontinuity. The containing partial five-minute bucket can never be
released. Execution can become eligible only after a later, entirely local full
bucket, current source observation, exact PostgreSQL committed ACK and terminal
paper-account reconciliation. Heartbeats remain status-only and cannot advance
trade coverage.

Production recovery is complete only after Chrome actually loads 0.6.50 and the
formal PostgreSQL stream accepts the V2 boundary within the 45-second cold-start
budget, then advances contiguously through a new full bucket with exact committed
ACKs and zero new conflicts/rejections. The open Android circuit breaker remains
authoritative and is never reset by this repair; therefore 2026-09-03 remains an
operationally failed trading day even if market capture is repaired after lunch.
