# 2026-09-02 pre-open Android circuit-breaker incident

## Timeline (Asia/Shanghai)

- 09:00:52 — first formal startup reached ledger recovery but failed before a
  usable Android simulation landing page was established.
- 09:01:49 — second formal startup failed; the runner recorded another daily
  Android startup failure.
- 09:02:49 — third formal startup failed and opened the daily circuit breaker
  (`2026-09-02 3`). No order intent, submit, cancel, fill, or cash movement was
  produced.
- 09:03:04 — patrol detected that the formal worker/tmux was absent. First
  recovery action: restored the reviewed Eastmoney page and began Android cold
  boot recovery.
- 09:04:05 — started the reviewed `THSP_API_32` AVD with Quick Boot disabled,
  snapshot load/save disabled, and the reviewed software renderer. User data
  was preserved.
- 09:06 — Android UI showed `模拟炒股`, masked account `**0208`, the top
  `撤单` tab, and no cancellable orders.
- 09:07 — the complete read-only execution-identity/orders/fills/cancellable
  preflight reproduced identity
  `d43357f7cf3ebb1e581f8a2cc6f3b319f9a8b6a5dfae31bb20a582029300ff7c`.
- 09:08:04 — started the app-independent trusted-session guard in
  `tmux -L gridedge_codex` session `ths_worker`. It preserves the open daily
  circuit breaker and therefore does not start a money-enabled worker.
- 09:09 — Computer Use proved that Chrome had loaded unpacked collector
  version `0.6.44` from `build/gridedge-web-market-extension`, not the stale
  `0.6.30` operational baseline. Order submission remains stopped while the
  required physical-isolation live E2E evidence is collected.
- 09:10 — prepared a fresh loopback-only E2E stack and shadow subscriber under
  nonce `e2e-0629-68385b0b-f4db-48b6-bd3f-0d651f4c18f3`; formal host/topic
  tripwires are absent and `money_actions_enabled=false`.
- 09:30 — the first E2E shadow expired before its browser was started because
  the two 900-second phases were scheduled in the wrong order. The zero-event
  result is retained as orchestration-failure evidence and is not a collector
  quality result.
- 09:34 — both the formal database and a fresh isolated E2E database still had
  no current-session events. The visible Eastmoney page itself was live.
- 09:36 — direct isolated service-worker evidence identified the reproducible
  root cause: a page-one snapshot contained valid current rows plus 09:15–09:24
  call-auction display rows, and the resume-boundary path atomically rejected
  the entire capture as outside the reviewed sale-time window.
- 09:37 — reloading the formal reviewed page restored database delivery, but
  the loaded 0.6.44 collector had already committed the unfiltered auction
  display prefix. Formal sequences 19920–19953 are 34 unreviewed rows from
  09:15:00–09:24:57. Sequence 19954 at 09:25:00 is the first eligible current-day
  anchor. The formal worker remained stopped, so no contaminated row reached a
  bar, order intent, Android write, fill, cash, position, ledger, or outbox.
- 09:39 — an independent provider regression first failed on a mixed
  09:24:57/09:25:00/09:30:03 snapshot. The adapter was then changed to exclude
  non-sale-time display rows before durable row identity is formed; the durable
  layer retains its atomic poison rejection as defense in depth.
- 09:42 — candidate 0.6.45 began a fresh physically isolated READ_ONLY E2E.
  Its first PostgreSQL exact-COMMITTED event arrived at 09:42:43; by 09:43:01
  the database had sequences 1–149 continuous, delivery count 1, 144 trade
  rows, 5 status rows, zero conflicts, and no pending browser outbox events.
- 09:57 — candidate 0.6.45 R1 completed PASS (900 seconds, two complete bars,
  start-to-first observation 37.407 seconds, maximum source gap 29.997 seconds,
  maximum committed-ACK gap 30.064 seconds, and zero money actions). Its fresh
  R2 then exposed another cold-start defect: the exact no-reviewed-sale-rows
  initialization error did not enter the bounded automatic recovery path. R1
  is retained but the release does not have two consecutive passes.
- 10:00 — added an independent failing recovery regression, fixed only the
  exact reviewed no-sale-rows initialization error, rebuilt candidate 0.6.46,
  and completed extension 176/176, deployment tests, format, strict Clippy,
  full Rust tests, and source/build identity checks.
- 10:04 — candidate 0.6.46 R1 was healthy in its fresh loopback-only stack.
  An app-independent `e2e_0646_chain` tmux session now owns R1 completion,
  evidence archival, fresh R2 startup, isolated MV3 worker cold restart, final
  validation, and teardown without waiting for another heartbeat.
- 10:05 — detected that the trusted-session guard tmux had disappeared after
  its last 09:59 circuit-breaker observation. Recreated the app-independent
  `ths_worker` guard session immediately; it observes `failures=3` and still
  cannot launch or bypass the formal money-enabled worker.
- 10:30 — the app-independent 0.6.46 validation chain completed two fresh,
  physically isolated 900-second rounds and tore the stack down cleanly. R1:
  start-to-first 24.110 seconds, maximum source/committed gaps 32.672/32.613
  seconds, 169 observations and two bars. R2: start-to-first 19.599 seconds,
  maximum source/committed gaps 24.000/24.008 seconds, 182 observations and two
  bars. Both rounds had `money_actions_enabled=false`, market-path-only shadow
  processing and six bar triplet events. R2 also closed and recreated the
  isolated MV3 service worker. The loopback MQTT and PostgreSQL ports were no
  longer listening after teardown. This is candidate evidence, not production
  approval or restoration.
- 11:01 — a read-only PostgreSQL export/rebuild rehearsal from reviewed anchor
  19954 through 21297 verified 1,344 exact-committed events and replayed 18
  completed bars. The first export attempt correctly stopped before mutation
  because its verifier treated the bytea hash column as text; the corrected
  rehearsal used `encode(payload_sha256, 'hex')`. Formal raw and MQTT were not
  changed by either rehearsal.
- 11:06 — the trusted-session guard disappeared for the second consecutive
  top-of-hour boundary. The independent red regression proved the cause:
  under `set -e`, `expr "00" + 0` returns status 1 even though zero is valid,
  terminating the guard at 10:00 and 11:00. The guard now strips the optional
  leading zero without a subprocess. The targeted regression, all 48 live
  tests, format, strict Clippy and the full repository suite passed. The fixed
  guard was restored at 11:07 and continues to preserve the open breaker.
- 11:05 — an app-independent `ths_postclose_recovery` tmux task was established
  for 15:05. It waits in 30-second bounds and will act only if the session date,
  breaker count, replay-binary hash, absent formal worker and a stable database
  maximum all match. It then performs the already-rehearsed exact export,
  validation and replay, backs up formal raw, atomically replaces it, and clears
  only the DB-covered persistent MQTT session. Any failed gate writes evidence
  and stops before the next side effect.
- 13:01 — the afternoon window opened with formal PostgreSQL still at the
  11:28:06 sequence 21500 watermark. The same patrol reloaded and re-handoffed
  the exact reviewed Eastmoney page. Within 15 seconds the database advanced
  to 21527, and the next bounded observation reached 21537 with the complete
  21501–21537 prefix. This restored capture only; the daily Android breaker
  still forbids the formal worker and order submission.
- 13:31 — the old formal 0.6.44 collector was stale for 602 seconds at sequence
  21678. Reloading only the page did not recover it. The same patrol used
  Computer Use on the exact GridEdge extension details page to reload the
  existing 0.6.44 background worker, then refreshed and re-handoffed the
  reviewed page. PostgreSQL advanced to 21758 and its committed age returned
  to 21.971 seconds. This is another production-availability failure of the
  old artifact and mitigation only; it does not approve 0.6.46 or the formal
  order worker.
- 14:01 — inspection proved that the 13:31 extension reload had in fact loaded
  candidate 0.6.46 from the mutable build directory. The formal database was
  already stale for 207 seconds. A manual extension/page reload restored it,
  but the extension popup then reported the exact failure `Eastmoney
  latest-first cycle did not produce a reviewed rowset effect` with 37 pending
  events. The two isolated 0.6.46 passes therefore do not establish production
  viability and promotion is invalidated.
- 14:32 — the same formal 0.6.46 path again exceeded the 60-second hard
  availability limit: sequence 22609 was 70.510 seconds old. The same patrol
  reloaded the exact extension and reviewed page, restoring PostgreSQL to
  sequence 22685 with a 0.724-second committed age.
- 14:36 — an independent exact-allowlist regression first failed for the
  rowset-effect error. Candidate 0.6.47 now maps only that exact initialization
  error into the existing bounded, durable-cooldown reviewed-tab reload path;
  all 176 extension tests pass and the build was regenerated. It was loaded
  only to keep the money-disabled formal collector observable while the Android
  breaker remains open. Initial live evidence advanced sequences 22769 through
  22776 with committed ages 0.363–5.919 seconds. This is mitigation evidence,
  not release approval or permission to start the formal order worker.
- 14:59 — continuous formal READ_ONLY observation invalidated 0.6.47 as a
  completed repair: after healthy operation through 14:58, database age grew
  to 68.784 and then 99.207 seconds at sequence 23266. The collector recovered
  by 15:00:14 and advanced to sequence 23293, but the >60-second interval is a
  hard availability failure. The exact allowlist change is therefore
  insufficient on its own and 0.6.47 must not be promoted. The post-recovery
  popup showed `DATABASE_COMMIT_ACK`, pending zero, conflicts zero; this does
  not erase the measured outage.
- 15:00 — database inspection showed that this was not a quiet-market period:
  sequences after 23266 included delayed 14:54:48–14:57:00 trades, followed by
  a 15:00:04 batch and final `LIVE_CONTIGUOUS` sequence 23308. The regular live
  scan's stale-trade recovery was holding the reviewed UI mutation lane and
  starving both trade ingestion and the independent status heartbeat.
- 15:02 — an independent contract regression first failed because a stale
  latest trade still entered `refreshStaleFirstPage`. Candidate 0.6.48 removes
  that live-path UI mutation: it returns `STALE_TRADE_COVERAGE`, keeps the
  status-only heartbeat observable, and relies on the worker's independent
  trade-coverage gate to remain READ_ONLY. A later DOM mutation re-enters the
  regular ingestion lane. Extension tests pass 177/177 and the build contains
  0.6.48. It remains an unreviewed, unpromoted candidate and has not been loaded
  into the formal Chrome profile.
- 15:05 — the first guarded post-close reconstruction attempt stopped before
  replacing formal raw because the pinned replay binary rejected the current
  source-observation completion watermark. Its failure evidence was preserved;
  no raw or MQTT state changed. Rebuilding the current replay binary produced
  SHA `222a1a278dcd4879a4d515cee8616a89e836b1865d033e6d6c4687df311aa8aa`
  and successfully replayed the exact database candidate: 3,355 events, 48
  bars, covered through 15:00, with final bar OHLC 3.35/3.36/3.35/3.35,
  volume 1,163,900 and amount 3,903,560.00.
- 15:08 — the guarded reconstruction completed PASS from the reviewed anchor
  19954 through final sequence 23308. It atomically replaced formal raw after
  backup, then cleared only the database-covered persistent MQTT session. The
  resulting raw SHA is
  `da8bdbce456761694a5dca43ff4beeadfacf002e88934095ebc15340c6fec377`;
  the PASS manifest and all failure evidence remain under the runtime evidence
  directory.
- 15:24 — candidate 0.6.48 was frozen for the next valid market-window E2E at
  `/tmp/gridedge-market-e2e-bundle-e2e-0629-33f3eb5a-8f3b-4499-8da9-985ffd29fb20`.
  Its manifest SHA is
  `01a75e5ad6002d052d452024efd61e6b8b1ca10831753e5480885e0bbff927d8`,
  source snapshot SHA is
  `867c2a6f0bf5fd300a237e18220cc8dec107ade78eb76de9451ae0d5fa88716c`,
  extension tree SHA is
  `edd97893c94dabb757c018a69f8645d5bee5ec984e6d7bb6658d4d9b4050eac1`,
  and candidate release SHA is
  `9ecace099fa65f01347a69c026cafa79d1474f5a1dde327f792f00227268989a`.
  The frozen bundle is evidence preparation only and is not production approval.
- 15:27 — independent test-expert review PASS permits only fresh physical-
  isolation READ_ONLY E2E. The reviewer confirmed that the exact stale-trade
  branch no longer mutates UI, source heartbeat remains status-only, unseen
  heartbeat trades are atomically rejected before coverage changes, and Rust
  still requires independent current trade coverage before RUNNING. Independent
  checks passed JS 143/143, `ths_live` 48/48, `web_market` 32/32 and
  `git diff --check`. Production remains blocked on two consecutive live E2E
  rounds that cover a quiet/stale table followed by a real new mutation, atomic
  unseen-trade heartbeat rejection and recovery, background throttling, high
  tick, and MV3 cold start.
- 15:34 — prepared (but did not start) fresh isolated R1 root
  `/tmp/gridedge-market-e2e-e2e-0629-5f8f585f-c234-44dc-96e9-188f7c62d751`.
  Its generated contract has `money_actions_enabled=false`, loopback-only MQTT
  and PostgreSQL endpoints, nonce-scoped topics/state/profile, no formal host,
  and no formal topic in its ACL. The stack remains stopped until the next
  reviewed market window.

## Current safety and availability status

The terminal paper account and formal execution identity are correct, and the
ledger/outbox are intact and synchronized at 1310/1310. Formal raw has now been
reconstructed from the reviewed 19954 anchor through sequence 23308 and the
database-covered persistent MQTT backlog has been cleared. The Chrome profile
still runs failed mitigation 0.6.47; the mutable build contains candidate
0.6.48 and must not be reloaded into production before review and two fresh
physical-isolation READ_ONLY E2E rounds. The strategy worker is **not RUNNING**
because the daily Android circuit breaker is open. This is a P0 availability
failure, not evidence that the grid strategy saw no opportunity.

The circuit-breaker state is not reset or bypassed. Market capture, isolated
collector validation, diagnosis, regression work, and release preparation
continue while money actions remain fail-closed.
