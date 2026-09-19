# 2026-09-03 LaunchAgent/guard startup race

Status: P0 availability breach; order submission remained fail closed.

## Timeline (Asia/Shanghai)

- 09:02 — pre-open patrol detected no usable formal worker, transient MQTT
  `No route to host`, and an unavailable Android emulator.
- 09:03 — the legacy loaded LaunchAgent retried the worker before the reviewed
  trusted-session guard owned startup. Three failed attempts exhausted the
  daily Android circuit breaker.
- 09:04 — the emulator was recovered with a data-preserving bounded cold boot
  (`-no-snapshot-load`, `-no-snapshot-save`, reviewed software renderer).
- 09:06 — MQTT routing and TLS port reachability recovered without weakening
  any market-data gate.
- 09:07 — Android identity, orders, fills and top-tab cancellable probes passed;
  the trusted guard was established but correctly refused to bypass breaker=3.
- 09:09 — root cause isolated to two competing startup owners: the legacy
  calendar/KeepAlive LaunchAgent and the trusted tmux guard.
- 09:12 — independent regression first failed against the trigger-bearing
  LaunchAgent template.
- 09:18 — first patch was rejected by independent review because launchctl
  errors were swallowed, staged publication was not atomic with unload, and
  the trusted guard SHA was not bound into execution identity.
- 09:24 — second review rejected the remaining old-guard-inode handoff race and
  incomplete staged-script/rendered-plist validation.
- 09:30 — the staged publisher gained one coordination/maintenance critical
  section covering launchd unload, old-owner quiescence, final process absence
  and all five atomic file replacements. Breaker preservation was strengthened
  from parsed fields to exact full-file bytes, including final newline.
- 09:32 — independent review rejected inherited tmux-server environment and an
  early guard-ready handshake. The starter was changed to tmux 3.4 per-session
  `-e` values and one literal guard command; the guard SHA/PID handshake is now
  published atomically only after all one-time gates.
- 09:35 — two real macOS regressions passed: an existing tmux server preserved
  all six exact environment values without shell evaluation, while a wrong
  reviewed date produced no ready record and caused starter cleanup. A second
  regression proved recovery from SIGKILL-stale `pid` and `ready` records.
- 09:38 — independent final review returned PASS. Full Rust format, strict
  Clippy, all-target/all-feature tests, 177 extension tests and 23 deployment
  tests completed successfully.
- 09:40 — release binaries and a new immutable 0.6.48 candidate bundle were
  built. Fresh physically isolated READ_ONLY E2E R1 started on loopback-only
  MQTT/PostgreSQL with money actions disabled; the formal collector page and
  Android account remained independently available.
- 09:57 — 0.6.48 R1 completed PASS after 900 seconds: 499 exact committed
  events with sequence 1..499 and delivery 1, 178 source observations, two
  complete bars/six processing events, 15.001-second maximum source gap and
  15.040-second maximum committed gap. Formal-topic, conflict, rejection and
  money-action counts were zero; teardown removed all isolated listeners.
- 09:58 — fresh R2 started from empty nonce state. After initial committed
  flow, the candidate MV3 service worker was deliberately closed; a different
  service-worker target appeared within 20 seconds and sequence delivery
  continued.
- 10:13 — R2 completed PASS after 900 seconds: 512 exact committed events with
  sequence 1..512 and delivery 1, 190 source observations, two complete
  bars/six processing events, 8.003-second maximum source gap and 8.066-second
  maximum committed gap. All negative tripwires remained zero and teardown was
  clean. Both rounds are archived under the installed runtime `e2e-audits`.
- 10:15 — independent production review returned PASS for signing, same-run
  authorization/install/activation, a real 0.6.48 Chrome reload and guard-only
  establishment. It explicitly prohibited breaker reset and formal worker
  startup on 2026-09-03.
- 10:18 — signed artifact SHA-256
  `32abfc4c9e26ba2fdd67c488c238a0c51c1df8b0dee3126e2a9c8e245529276b`
  was authorized against journal head 1463. The first atomic install attempt
  stopped before publication because an orphaned old project-path guard had
  not exited within the handoff bound.
- 10:19 — the known old guard PID was terminated and atomic publication
  succeeded. Platform activation and recovery appended through head 1466;
  outbox cursor was staged to 1466. Execution-identity revision 12 bound the
  new binary, runner, trigger-free plist and guard identities.
- 10:20 — installed guard PID 78672 published the exact expected ready SHA.
  Launchd remained absent, breaker bytes remained exactly `2026-09-03 3\n`,
  and no formal worker existed. Chrome visibly confirmed and reloaded 0.6.48;
  the reviewed Eastmoney URL was refreshed and retained across patrols.
- 10:22 — PostgreSQL showed formal source sequence 24698, provider
  `eastmoney-time-sales-dom-v6`, current V3 observation, exact committed flow
  and unchanged historical conflict/rejection counts 1/5. This proves
  collector recovery but does not permit bypassing the exhausted breaker.

## Root cause and safety outcome

The LaunchAgent remained a runtime owner after the project introduced an
app-independent trusted-session guard. Both could start the same formal runner.
The runner's daily breaker prevented duplicate money actions, but ordinary
startup failures consumed the entire daily retry budget before dependencies
were restored. No order intent, Android submit or cancel action occurred.

The breaker is not reset or bypassed. Therefore 2026-09-03 cannot be reported
as operationally successful even if the deployment defect is repaired later in
the session.

## Corrective contract

- The LaunchAgent is a trigger-free identity artifact; its launchd label must
  be proven absent before publication.
- Candidate binary, config, runner, guard and rendered plist are all validated
  in adjacent staging files before the old owner is stopped.
- The old trusted tmux owner is quiesced before publication, with proof that no
  guard/worker remains and that the circuit-breaker bytes did not change.
- Launchd unload failures or ambiguous state cause zero file publication.
- The exact guard SHA is part of the append-only remote execution identity.
- Activation and identity binding precede establishment of the new unique
  trusted guard; first worker startup remains READ_ONLY.

## 2026-09-04 recurrence hardening

The next pre-open audit found that the runner still charged every non-zero
worker exit to the Android daily breaker. Thus a failure after the Android
identity/orders/fills/cancellable preflight—such as MQTT TLS or subscription
failure—could consume the Android-only retry budget. Tests were made red before
the repair.

The worker now publishes one atomic per-launch handshake immediately after the
read-only Android preflight and before service recovery or market connection.
The runner creates a random same-directory path with `mktemp`, removes the
placeholder, launches the worker in the background, and records the actual
`$!` PID. Its exit trap accepts only a regular non-symlink file whose complete
bytes are exactly `GRIDEDGE_ANDROID_PREFLIGHT_OK_V1 <actual-pid>\n`. Missing,
forged-PID, extra-token and extra-line markers all charge the breaker; later
market failures after the valid handshake do not. Every exit removes the
marker. Independent review and the dynamic matrix passed.

The installed guard also gains an app-independent dependency gate before it
may invoke the money-enabled runner: exactly one ready `emulator-5554`, exact
AVD `THSP_API_32`, one running Google Chrome process, and reachable MQTT TCP
8883. Ordinary dependency absence defers startup without changing a single
breaker byte. Seven isolated dependency cases prove the worker spy is never
called and the breaker is unchanged.

A separate same-run publication defect was found before installation:
`install_ths_sim.sh` depended on an unfrozen worktree
`target/release/gridedge` validator. The regression first reproduced failure
from a missing validator. Stage now builds worker and validator in one locked
release invocation, freezes both by content SHA, and install verifies the
validator as a regular non-symlink executable before creating any publication
target. A tampered validator produces zero target creation and zero publish.
Independent pre-install review passed after this correction.

At 06:50 the old installed guard was established in the app-independent tmux
session for the 2026-09-04 date. At 07:28 a disappeared emulator was recovered
with a data-preserving cold boot using disabled snapshot load/save and the
reviewed software renderer. Identity, orders, fills and cancellable probes then
passed with empty terminal lists. Launchd remained absent and the exact prior
breaker bytes `2026-09-03 3\n` remained unchanged. Final installation and
append-only identity evidence are recorded in the 0.6.51 release review.
