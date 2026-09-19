# 2026-09-07 afternoon source outage and close state

Status: **OPEN external/user-presence blocker and P0 availability failure**.
All times are Asia/Shanghai. No CAPTCHA, order, breaker or historical market
event was changed while investigating this incident.

## Impact, cause, mitigation and completion criteria

| Item | Finding |
| --- | --- |
| Impact | The formal source stopped advancing after source sequence 36653. Its last committed `LIVE_CONTIGUOUS` coverage was 13:54:54. The worker entered `READ_ONLY` at 13:56:05 and never regained executable market evidence before close. The last processed bar was 13:50 at journal 1949/1950/1951 and all three stages were `READ_ONLY`, so the strategy was not evaluated. |
| Root cause boundary | The exact reviewed Eastmoney page was still present, but a slider CAPTCHA was visible and the registered 0.6.51 extension no longer delivered source observations after the page was refreshed/recreated. The CAPTCHA is a required user-presence boundary and must not be solved or bypassed by automation. The already-reviewed 0.6.53 candidate cannot help until Chrome actually reloads it. |
| Current mitigation | The core failed closed in `READ_ONLY`; no historical order was caught up. The exact page was refreshed, recreated once, and marked for handoff. The page now shows the genuine close rows behind the CAPTCHA, including 15:00:00, but none of those rows has been admitted to the formal source or ledger. Worker and guard exited normally at 15:05; the app-independent supervisor remains alive for the next preflight. |
| Completion criteria | The user completes the visible CAPTCHA and normally reloads the already-reviewed 0.6.53 extension. A later valid market window must then prove the actual runtime/build identity, a fresh V3 observation, exact PostgreSQL `COMMITTED` ACK, contiguous current coverage, terminal Android/Paper reconciliation, and one current completed bar whose three stages are all `RUNNING`. No post-close capture can substitute for this live evidence. |

The separate final-bucket evidence limitation remains open: a visually present
15:00 row is not part of the formal append-only source until the reviewed
collector captures and commits a complete, identity-bound history. Wall-clock
close, HTTPS time and the current DOM must not be used to synthesize a bar.

At 15:37 a fresh read-only DOM observation also showed a `15:23:45` row above
the 15:00 close row. Its source meaning is not part of the reviewed A-share sale
window. An exact adapter probe using those visible times confirmed that the
existing parser excludes 15:23:45 before market identity is formed while
retaining 14:57:00 and 15:00:00; the post-close row therefore did not enter raw,
PostgreSQL or the ledger. This does not certify session completeness: the
remaining reviewed rows still require a complete paginated capture and exact
application commit.

## Incident timeline

- 13:29:15 — the worker recovered from a short source-coverage catch-up and
  returned to `RUNNING`.
- 13:33:40 — source observation timeout forced `READ_ONLY`; the worker continued
  to audit bars without trading eligibility.
- 13:50:24 — terminal reconciliation briefly restored `RUNNING`; at 13:50:34
  stale trade coverage returned it to `READ_ONLY` before the 13:50 bar stages.
- 13:54:54 — source sequence 36653 carried the last formal contiguous trade
  coverage. The exact committed receipt was retained.
- 13:56:05 — journal sequence 1957 recorded
  `EASTMONEY_SOURCE_OBSERVATION_TIMEOUT`; no later formal market event arrived.
- 14:00:15 — the scheduled patrol detected source age above 300 seconds and
  classified the stopped trading path as P0. The five-minute recovery budget
  had already begun at the 13:56 journal transition.
- 14:00–14:03 — first recovery action: reload the exact reviewed page. The table
  rendered, but the extension did not resume. The patrol then recreated exactly
  one reviewed URL and retained it for handoff; the site presented a slider
  CAPTCHA. No alternate browser surface, CDP or command workaround was used.
- 15:00 — the system was still `READ_ONLY`, head/outbox cursor 1957/1957. The
  incident consumed the remainder of the session and the trading day is an
  operational failure.
- 15:05:06 — the worker exited successfully at the normal close boundary. The
  guard did not restart it. At 15:22 the supervisor reported `CLOSED`, zero
  worker/guard, one booted reviewed emulator, Chrome present, MQTT reachable,
  unchanged breaker bytes and no maintenance state.
- 15:44 — the frozen-candidate activation README was found to contradict the
  incident gate by saying not to operate the CAPTCHA. The instruction was
  corrected to require the user personally to complete it while explicitly
  prohibiting automation from operating or bypassing it. Only the README and
  its recorded evidence hash changed; the reviewed 0.6.53 tree stayed frozen at
  `719726ca656d8e5ed8ab50dbd905226748ebc6c8151c04dc3d3210cf932afd0f`.
  `manual-release-instructions-audit.json` records the correction boundary and
  both frozen/formal tree identities.
- 15:48 — the active heartbeat baseline was corrected to use the authoritative
  workspace `/Users/rjwang/Documents/ChatGPT/GridEdge-T`, inspect the actually
  loaded collector identity rather than requiring stale 0.6.51, and apply the
  45-second first-observation/ACK gate to the user-activated production 0.6.53.
  The schedule, target task and all fail-closed gates were preserved. The later
  rollback finding was also added: a failed 0.6.53 production validation must
  stay `READ_ONLY` and direct the user to the persistent 0.6.51 GUI rollback.
- 15:50 — the manual activation procedure's only 0.6.51 rollback reference was
  a volatile `/tmp` directory. The exact still-formal 0.6.51 tree was copied to
  the persistent runtime release-backup area and verified as `4881a046...`.
  The README now gives a fail-closed, user-GUI rollback procedure; the formal
  Chrome build remains untouched.
- 15:56 — next-session scheduling was checked against the installed supervisor.
  Its source equals the reviewed `8df496ae...` artifact and maps 2026-09-08
  09:00/09:25 to `PREFLIGHT`, 09:30 to `TRADING`, and 12:55 to `PREFLIGHT`;
  the exact independent operating-contract regression passes. AC idle sleep is
  disabled and an app-independent PID-1-owned `caffeinate -d -i -m` assertion
  is live. Because the product permits only one heartbeat per task, the active
  09:00 patrol now stays alive with bounded checks until 09:25 whenever any
  preflight gate is missing, then immediately reports a readiness P0 without
  waiting for the 09:30 recurrence. `next-session-readiness-audit.json` records
  this point-in-time evidence.

No deployment or rollback was performed during the outage. The formal Chrome
registration remains collector 0.6.51, tree
`4881a04687535be542b99906a9314e4d5d057b10c932496e49c9592011f92a6b`.
Collector 0.6.53 tree
`719726ca656d8e5ed8ab50dbd905226748ebc6c8151c04dc3d3210cf932afd0f`
has independent scoped approval after 198 tests and two isolated 900-second
rounds, but is not a production activation.

## Post-close reconciliation

- Journal head and outbox cursor: 1957/1957.
- Last processed bar: 13:50, stages 1949/1950/1951, all `READ_ONLY`, strategy
  evaluated false.
- Paper cash: 103418.530 available, 0 frozen, fees 28.97.
- Position: 27500 total and sellable, 0 bought today, 0 frozen sell.
- Paper open orders: zero. Android read-only identity, orders, fills and
  cancellable probes passed post-close; all three remote tables were empty.
- The sole historical staged row remains `SUBMITTED` with a raw ambiguous cancel
  state, but its immutable `FILLED` fact covers all 27500 shares. Formal
  unresolved/open/unknown order count is zero and it must never be clicked again.
- Installed/effective core SHA is
  `149ca55f6ae239695f22e132d56b6cfcfd14514f230b4fe349e3fc9bb50e8422`;
  execution binding revision 15 and binding SHA
  `1be375fb951fd03a25f5f98100d43a746bea5f1ff7619f16ae73fba3b483bd53`
  are unchanged. The trigger-free launchd label is absent.
- Breaker bytes remain unchanged with SHA
  `7902facaf7bf29d46856397acd9378959bb41074c30033f79c16e198a95c0344`.

Post-close repository validation passed: format, strict all-target/all-feature
Clippy, 483 Rust tests with zero failures and the two explicit money-action
hardware smokes ignored, 198 extension tests, 83 Python deployment/local-E2E
tests, and the 36 operational tests included in that Python run. A fresh,
isolated sample replay ended with 233 events, zero duplicates, STOPPED mode,
no open orders and realized grid PnL 5717.02. The release build completed, but
no unsigned core candidate replaced the signed installed worker.

## Next reviewed session

The long-lived `ths_session_supervisor` tmux owner remains app-independent and
will enter `PREFLIGHT` at 09:00 on the next reviewed trading day, establish only
the exact signed guard/worker, and preserve `READ_ONLY` until the normal market
and terminal gates pass. The active heartbeat must check the CAPTCHA and loaded
collector identity immediately, notify once if user presence is still required,
and must not report healthy unless a current complete bar is processed entirely
in `RUNNING`.

### 2026-09-08 early-preopen observation (not restoration)

The operations task's 08:02 read-only UI check found the exact reviewed tab
present, latest-first enabled and no visible CAPTCHA; the tab was retained for
handoff. The delivery task read that completed result at08:12 rather than
concurrently operating Chrome. Runtime collector0.6.53 activation is still
unproven and belongs to the09:00 formal preflight. At08:13 the independent
supervisor remained CLOSED before its scheduled09:00 PREFLIGHT, with
head/cursor1957/1957. This removes the last observed visible CAPTCHA condition,
not the incident's activation, committed-ACK, current-bar or terminal-account
restoration requirements. The incident and separate completion defect remainOPEN.
