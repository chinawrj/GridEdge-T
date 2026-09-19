# 2026-09-07 follow-through on the last closed reviewed session

Status: OPEN. The supervisor/raw-repair scoped release is installed. Collector
0.6.53 passed R2 and independent scoped review, but production activation,
authoritative final-bucket completion, and the afternoon CAPTCHA outage remain
open.
All times are Asia/Shanghai. No production test order or fabricated market event
is authorized by this packet.

## Scope established from evidence

The last closed reviewed session is **2026-09-04**, identified from the incident
packet, actual journal 1781/1782/1783 for 14:55, matching outbox cursor 1783, and
the guard's normal 15:05:12 worker exit. This is not a guess based on yesterday.
The September 4 five-minute recovery budget was missed and remains an operational
failure even though the worker subsequently ran through close.

The September 7 09:00 patrol was also reviewed because its unresolved prevention
work predates this task. It manually recovered the emulator, opened the exact
reviewed source page, and unloaded the otherwise trigger-free launchd label.
At 09:27 the source observation was over 60 seconds stale. Reloading the existing
0.6.51 extension restored source sequence 33052 onward. At 09:35 a genuine bar
completed the received/committed/processed chain and the worker was RUNNING.
The exact transport failure behind that historical stall was not recorded; the
newly reproduced MQTT deadlocks below are a confirmed fault class, not proof
that they alone caused that particular stall.

## Current formal facts

At the 10:35 inspection the latest completed bar was 10:35, the journal and outbox
were 1845/1845, and the worker was RUNNING. Paper available cash was 103418.530,
frozen cash zero, fees 28.97; position/sellable 27500, today-bought/frozen-sell zero.
There were zero Paper open orders. The single historical cancel AMBIGUOUS remains
covered by the durable FILLED fact (27500 shares, 11 fill rows); it is not a new
unresolved cancellation and must never be clicked again.

Installed worker SHA 149ca55f6ae239695f22e132d56b6cfcfd14514f230b4fe349e3fc9bb50e8422
matches the effective platform. Execution binding revision 15 is unchanged.
Runner 6f0df0290db7cc84a1c0b5977817c346008f42be81e37070fd2306338d32662c,
guard b5efcd83220bb981b6b2df57054adbe766c8b7c38aa66bcb8c11689d6378c536 and
breaker SHA 7902facaf7bf29d46856397acd9378959bb41074c30033f79c16e198a95c0344
match the reviewed baseline. No breaker write has been performed in this task.
The reviewed Chrome URL remains open and latest-first is checked. A CAPTCHA is
visible; this task has not interacted with it. Genuine underlying source receipts
continue to advance independently.

At 10:40 PostgreSQL read-only inspection proved the source prefix continuous
1..34784, with 34784 distinct sequences, and exact committed event 34784 hash
36be1cab63714902674f1616ded7128a9fd680a5ead98fbff49f86ce6073e6fc.
The market ingestor, broker and PostgreSQL containers were all healthy. Historical
conflict/rejection totals were 1/5; those totals are not claimed to be new incidents.

## Reproducible defects and repair status

| Defect | Reproduction and minimum correction | Status |
| --- | --- | --- |
| Missing PUBACK holds global publishing lane forever even after exact COMMITTED | Independent fake transport never invokes PUBACK callback. Original code awaits publish before receipt and hangs. One deadline now covers both promises; missing either leaves PENDING and releases listeners. | Independent review PASS; final collector 198 tests PASS, R1 PASS, R2/activation pending |
| Missing SUBACK leaves connection setup pending | Fake subscribe never invokes callback. Add bounded SUBACK deadline and retain normal QoS/identity checks. | Independent review PASS; final collector 198 tests PASS, R1 PASS, R2/activation pending |
| Next-day startup still depends on Codex patrol | Existing guard is date-bound and has no independent next-day bootstrap. | Corrected after independent RED; scoped PASS and unique long-lived owner installed |
| Guard only defers missing dependencies | Chrome/Android missing can wait indefinitely without an AI turn. | Reviewed process/AVD/guard recovery installed; existing healthy worker preserved |
| Chrome process alive but tab/extension absent | Initial candidate erroneously reports OBSERVE. | Collector exact-page watchdog independently reviewed; real R2/production activation pending; disabled extension not externally recovered |
| Normal ADB offline boot misclassified as wrong identity | Independent reviewer injected emulator-5554 offline. | Independent bounded cold-boot and phase-resume tests PASS; installed |
| Supervisor manifest read/parse TOCTOU and unrestricted starter | Independent reviewer substituted manifest contents between two reads. | Single-fd manifest, exact allowlist, frozen copy checks and durable rollback independently PASS; installed |

## Verification completed so far

- Baseline fmt and strict all-target/all-feature Clippy PASS.
- Full Rust suite PASS after copying the historical certification fixtures into
  the isolated worktree. The initial missing-fixture failure is retained in logs.
- Baseline collector 189/189 PASS; final 0.6.53 collector 198/198 PASS.
- Deployment/ingestor/publisher/local-E2E Python suites 47/47 PASS.
- Two isolated default-fixture strategy replays completed with their own ledgers.
- The exact frozen September 4 raw bytes replayed twice: 5766 events, 47 genuine
  five-minute bars, covered-through 1788505020000000, equal CSV SHA
  40f9ae1860325a2ce113d5cb6ece3999adacc8f141b48cc2124d9201977ed281.
  The 15:00 bar is not invented; the last executable bar is 14:55.

Working evidence is under /tmp/gridedge-20260907-closure-evidence. Safe evidence
is also archived under the installation runtime/e2e-audits/20260907-recovery-closure.
Final collector R1 passed a full 900 seconds; independent PID 90006 owns R2 in the
13:00 window, and a one-time 13:16 task wakeup continues review/activation.
No collector 0.6.53 production release is claimed before those gates pass.

The scoped-reviewed supervisor/raw gap repair is now installed. Its current
manifest is 4b1b9ee226bd97a5704c4f6238836e63f7f689a49b5572c778dc5b5b84df6ae9,
script 8df496ae16a57d43ef42f609ac2a2cfd747ac65314c5cdae9772ea96ac6addcd,
and unique tmux owner PID 22058 has the tmux server as its parent. Independent
post-install checks observed advancing heartbeats while original worker PID 50053,
core/runner/guard/plist/breaker identities and binding revision 15 stayed unchanged.
The last SQL correction recognizes durable CANCELLED as terminal, without admitting
non-SUBMITTED or unresolved ambiguity. All 36 ops tests pass, including independent per-stage bar-mode fixtures. Full Rust gates pass
with 483 tests, zero failures and the two existing hardware-money smoke tests
ignored. Real Android read-only identity/orders/fills/cancellable checks passed;
orders/fills/cancellable rows were zero. No money-action flag was used.

At lunch ledger/outbox were 1902/1902. Source sequence 35808 was committed but only
covered the last genuine 11:29:48 trade. The latest processed completed bar was
11:25; no legal 11:30 completion existed. The source-native final-segment proof
remains an OPEN production availability defect, separately documented in
2026-09-07-missing-segment-completion.md. A fresh HTTP clock, sidecar installation,
or passing unrelated tests does not close it. The overall Goal remains active.

Mode audit correction: 11:25 was processed in READ_ONLY recovery, followed by RUNNING reconciliation. The morning contained 19 received-in-RUNNING and 4 received-in-READ_ONLY bars (10:35, 10:50, 11:15, 11:25). Current RUNNING plus a three-stage chain is not proof of normal strategy evaluation for that bar. No historical strategy/order evaluation is replayed to fill this gap.

At 12:55 the independent supervisor entered PREFLIGHT with one reviewed guard,
worker and emulator, Chrome present, MQTT reachable, identity valid, screen
unlocked and AC at 100%. Four real Android read-only preflight commands passed;
orders, fills and cancellable rows were zero. The exact reviewed browser page
remained latest-first. Its CAPTCHA iframe was not interacted with.

Afternoon source receipt resumed at 13:00:04.793 with LIVE_CONTIGUOUS sequence
35811; fresh source observation followed at 13:00:10.003. The core briefly entered
READ_ONLY for the lunch observation timeout, reconciled at 13:00:18, and resumed
RUNNING. The genuine 13:05 bar completed stages 1906/1907/1908 entirely in RUNNING,
so its normal strategy processing is evidenced. Head/cursor reached 1908/1908;
cash 103418.530, position/sellable 27500 and no unresolved/open orders were
unchanged. This afternoon recovery does not close the missing morning end bucket.

Final collector R2 started at 13:00:09 in its isolated namespace. At 13:02:09 its
single source page was closed by the frozen acceptance owner. By 13:02:30 the
collector had opened a new target for the exact reviewed URL, while formal
source receipts and committed acknowledgements continued. Full duration and
gap/bar acceptance remained pending at this checkpoint; page recreation alone
does not constitute a pass.

Final candidate outcome: independent scoped PASS for both full 900-second rounds.
R1 has two bars; R2 has one. The unchanged executable contract requires a nonzero
bar count in each round. R2's maximum committed receipt gap was 22.006824 seconds;
its full elapsed time was 900.163 seconds. Exact final metrics and review limits
are recorded in docs/reviews/2026-09-07-collector-0653-final-review.md.

Production activation remains pending user action. Browser URL security policy
refused Chrome extension-management access and explicitly forbids alternate
surfaces, CDP or commands as a workaround. No bypass was attempted and the
registered build remains 0.6.51 (tree 4881a04687535be542b99906a9314e4d5d057b10c932496e49c9592011f92a6b).
Reviewed 0.6.53 source is synchronized, and the exact frozen candidate plus manual
instructions are in runtime/e2e-audits/20260907-recovery-closure/collector-0.6.53-manual-release.
Manual activation still requires actual runtime and fresh source/ACK/current-bar
verification under existing recovery gates. The Goal is active, not complete.

## Afternoon outage and normal close

At 13:56:05 journal sequence 1957 moved the worker to `READ_ONLY` for
`EASTMONEY_SOURCE_OBSERVATION_TIMEOUT`. Source sequence 36653 was the last formal
committed contiguous receipt and covered trades only through 13:54:54. The 14:00
patrol refreshed and then recreated the single exact reviewed page, but a slider
CAPTCHA required user presence and the registered 0.6.51 extension did not resume
delivery. Automation did not interact with the CAPTCHA or use an alternate
extension-management surface.

No later executable bar was received. The 13:50 bar at journal 1949/1950/1951
was processed entirely in `READ_ONLY`, so the strategy was not evaluated. The
worker exited normally at 15:05:06 and was not restarted after close. Final
head/outbox were 1957/1957; cash, position and order state were unchanged. This
day is an operational failure. Full evidence and the next-session completion
criteria are recorded in `docs/incidents/2026-09-07-afternoon-captcha-outage.md`.
