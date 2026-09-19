# 2026-09-04 pre-open local raw gap and install handoff

Status: P0 availability breach restored; post-close defects repaired and
deployed; order submission remained fail closed.

## Timeline (Asia/Shanghai)

- 09:00:13 — the app-independent old guard started the old signed worker in
  `READ_ONLY` first. Recovery reached journal/outbox 1470/1470.
- 09:25:09 — the worker exited on `market source sequence is not contiguous`.
  Detection was at 09:25:15; this is the P0 detection time. The Android breaker
  legitimately recorded its first 2026-09-04 preflight failure.
- 09:26:09 — the guard performed the first automatic recovery action. The
  worker failed on the same local gap at 09:26:11 and the breaker became exact
  bytes `2026-09-04 2\n`.
- 09:26:51 — the old guard was isolated before a third restart could exhaust
  the breaker. This was the mitigation choice: preserve fail-closed state while
  rebuilding only the missing committed market prefix.
- 09:28–09:35 — read-only PostgreSQL inspection proved source instance
  `8101d65c-bdba-4de3-83e0-8983506f159e` continuous from sequence 1 through
  27553 with no gap. The formal local raw log ended at 23308 and then jumped to
  27521. Root cause was therefore a local durable-raw prefix gap, not a source
  database gap.
- 09:36 — the exact PostgreSQL-committed payload bytes for 27521..27768 were
  exported and verified by event ID, payload SHA, source identity, sequence,
  timestamp and topic. The successful candidate contained 248 events (96 trade,
  152 status), SHA-256
  `0b68501e8f7404709a3fa7e339732106cf5563232cabb5ac1db050c8a9de9e95`.
  It replayed as `COMPLETE_SESSION` and released the genuine 09:35 bar. The old
  raw was preserved at
  `/Users/rjwang/Library/Application Support/GridEdge-T/runtime/002256-opening-v1-market-mqtt.jsonl.pre-live-gap-recovery-20260904-0936`.
- 09:37:45 — the old worker restarted from the repaired raw. It recovered
  through the current 09:35 bar, reconciled the terminal Paper account, and
  entered `RUNNING` at 09:38:17. This exceeded the five-minute recovery budget;
  the incident remains a P0 availability-budget breach.
- 09:45–10:02 — two fresh, physically isolated 900-second READ_ONLY E2E rounds
  passed with fresh UUID roots, formal-topic denial and zero money actions.
- 10:03:42 — the frozen validator appended platform-upgrade authorization 1495
  for signed worker `149ca55f...` and certification `059b8550...`.
- 10:04–10:07 — two install attempts stopped before publication because the
  quiescence probe falsely matched the persistent tmux server's historical
  `new-session ... run_ths_trusted_session_guard.sh` argv. Exact process
  inspection proved no actual guard or worker and the breaker remained byte
  identical.
- 10:08 — the process expression was anchored to an executing shell plus the
  exact formal guard path. A new regression and a real-root assert-only check
  passed. Publication then installed the exact signed worker and reviewed
  runner/guard/plist with launchd still absent.
- 10:09:24 — platform activation appended sequence 1496; recovery appended
  1497. At 10:09:48 execution identity revision 14 bound the installed
  platform, runner, plist and guard.
- 10:10:03 — new guard PID 62172 published its exact SHA ready handshake and
  uniquely started the new worker. The worker processed 10:05, reconciled the
  account, entered `RUNNING` at 10:10:32, then processed the completed 10:10
  bar through sequence 1508 at 10:10:36. Outbox cursor was also 1508.
- 10:15:21 — the installed worker processed the next current bar through
  sequence 1512; a fresh matching reconciliation returned the service to
  `RUNNING` at 1514, with outbox cursor 1514. This confirms the restored worker
  remained operational beyond its first post-install bar.
- 10:25:19 — the worker processed another current bar through the complete
  1526/1527/1528 chain with outbox cursor 1528. The resource-aware algorithm
  deferred the full opportunity and created no order intent.
- 15:05 — the reviewed worker completed the session and exited normally after
  processing the 14:55 bar through the complete 1781/1782/1783 chain. Ledger
  and outbox both ended at 1783; the guard and worker were absent post-close,
  as designed.
- 15:23 — post-close audit found that the old runner's successful-exit path had
  overwritten the legitimate breaker bytes from `2026-09-04 2\n` to
  `2026-09-04 0\n`. The first recovery action was a behavior-level red test
  reproducing the overwrite. Root cause was the unconditional successful-exit
  write in `run_ths_android_sim.sh`, not an Android or order failure.
- 15:27 — Chrome's existing unpacked extension was reloaded in place. Popup
  runtime identity then reported manifest 0.6.51 and reviewed build
  `collector-0.6.51-stale-trade-coverage-only-v1`; the exact reviewed Eastmoney
  URL remained open. This performed no CAPTCHA bypass and no market or order
  action.
- 15:28–15:37 — the runner stopped rewriting the breaker on successful worker
  exit. A new regression proves successful exit preserves the exact file bytes.
  Full validation also exposed a post-15:05 clock race in the guard's stale-PID
  test; the test now runs against an isolated temporary root and a fixed 09:00
  clock. Five consecutive focused runs, the full Rust suite, extension suite,
  Python suites, config validation, deterministic replay and release build all
  passed. Independent review approved the runner-only mitigation.
- 15:39 — the exact signed worker remained unchanged while the five installed
  files were republished atomically. The installed runner is now
  `6f0df0290db7cc84a1c0b5977817c346008f42be81e37070fd2306338d32662c`.
  The legitimate breaker state was restored to the pre-defect exact bytes
  `2026-09-04 2\n` (SHA-256 `7902facaf7bf...`) and stayed byte-identical through
  publication. Append-only execution binding revision 15 changed only the
  runner identity and bound identity
  `1be375fb951fd03a25f5f98100d43a746bea5f1ff7619f16ae73fba3b483bd53`.
  Launchd remained absent and no formal worker was started after close.

## Safety and accounting outcome

No live or test order was created, no historical order was synthesized, and no
market datum was fabricated. The defective old runner did overwrite the
breaker after normal close; the audit restored the exact legitimate pre-defect
bytes without clearing a failure or enlarging the budget, and the deployed
runner now preserves those bytes on successful exit. The Paper snapshot
remained cash 103418.530 available, zero frozen, fees 28.97;
position and sellable quantity 27500, today bought and frozen sell zero. There
were no open terminal orders. The sole historical raw cancellation ambiguity
is fully covered by the immutable FILLED fact for all 27500 shares, leaving zero
unresolved remote ambiguity.

## Corrective contract

- PostgreSQL committed history is the authoritative repair source for a local
  raw-log gap. Payload bytes and all envelope identities must match exactly;
  replay must pass before atomic replacement, and the prior raw file is kept.
- Market failures after the atomic Android preflight handshake do not consume
  the Android-only breaker. Preflight failures still do.
- Deployment quiescence recognizes only a real formal worker or an executing
  shell whose script argv is the exact installed guard. Merely retaining that
  path in an unrelated tmux server argv is not a live guard.
- Publication, activation, append-only identity binding and new-guard startup
  remain ordered. The trigger-free LaunchAgent label stays absent.
- Successful worker exit must not write the Android breaker. Its exact bytes
  are changed only by the reviewed date rollover or a genuine atomic-preflight
  failure; a regression now enforces this boundary.
- Production extension 0.6.51 is loaded in the existing authorized Chrome
  profile and the exact reviewed URL remains open. The next reviewed live
  window must still prove latest-first state, any user-presence challenge,
  first current observation plus PostgreSQL committed ACK within 45 seconds,
  and the complete first post-open five-minute bar by 09:35.

## Issue disposition

| Issue | Impact and root cause | Mitigation | Completion evidence |
| --- | --- | --- | --- |
| Local raw sequence gap | P0; formal raw missed a PostgreSQL-committed prefix | Exact committed payload reconstruction, replay, atomic raw replacement | Worker restored at 09:38:17 and processed current bars through close |
| Collector runtime lagged disk | 0.6.50 remained active while 0.6.51 was registered on disk | Normal reload of the existing unpacked extension | Popup reports exact 0.6.51 runtime/build identity; live 45-second proof remains window-only |
| Successful exit cleared breaker | Safety-management defect in the runner's success path | Remove the write, add byte-preservation regression, atomically republish and rebind | Independent PASS; runner `6f0df029...`; exact breaker bytes preserved |
| Guard test depended on wall clock | Post-15:05 test race, not a production runtime fault | Isolated test root and fixed 09:00 clock | Five focused passes and full all-target/all-feature suite PASS |

The trading day remains operationally unsuccessful because restoration missed
the five-minute P0 budget. Post-close repair does not retroactively erase that
availability breach.
