# 2026-09-07 operational supervisor review

Status: scoped review PASS; installed and independently observed running. Asia/Shanghai. The overall task remains OPEN for collector R2/activation and authoritative segment completion.

Independent reviewer: parent task `01a00079-b837-7750-9e60-1dbe3f803685`.
The implementation task is `01a079b7-9292-7753-93ae-8b32104039e2`.
The reviewer authored only independent temporary-fixture tests in
`deploy/ops/test_supervisor_independent.py` and
`deploy/ops/test_market_raw_independent.py`; no formal fault injection or order
was made for review.

## Rejected iterations and corrections

The first dependency supervisor was rejected because it inferred source health
from Chrome presence, treated ordinary offline boot as an unknown device,
had no bounded cold-boot continuation, and read a manifest twice without a
closed path allowlist. The manifest now uses one no-follow file descriptor for
hashing and parsing, freezes the exact installed files, and restricts every
executable/configuration path. Actual market health binds the local raw event,
its source sequence and exact payload SHA to a committed PostgreSQL row.

Independent cold-boot tests then found that the new emulator inherited the old
expired boot timer and that offline ADB could consume the one daily recovery
attempt without terminating its unique owner. The correction stores atomic
RESERVED/STOPPED/LAUNCHING/LAUNCHED phases, verifies the qemu birth time and
complete executable/AVD identity, permits only SIGTERM of the unique reviewed
owner with no worker, confirms exit before spawning, and assigns a new 120-second
boot window. A resumed LAUNCHING phase adopts a matching new process; it never
blindly repeats a launch. There is no SIGKILL, data wipe or breaker write.
The independent cold-boot subsystem review passed, with 17 combined supervisor
cases passing at that point. This is not whole-release certification.

## Committed raw gap recovery

The reviewer confirmed the September 4 missing local prefix had no executable
automatic repair path in the previous release. The new separate module requires
strict PostgreSQL interval continuity and validates canonical event identity,
source, instance, instrument, topic, payload SHA, sequence, QoS and non-future
times. It inserts only missing records. Every existing local line, including a
previously seen exact duplicate at an older sequence, retains its original bytes
and position. An unknown reordered event or conflicting retransmission blocks.

Independent publication review initially rejected unbound semantic audit data,
missing backup-directory fsync before replace, and acceptance of a backup
symlink. Publication now reconstructs exact per-day candidate hashes, checks the
complete replay set and successful exit identities, binds the replay executable
to the approved installation manifest, refuses symlink evidence, persists the
original file and directory entry before replacement, and rechecks quiescence
and original bytes at the atomic replacement fence.

Integration owns coordination then maintenance, never displaces a running
worker, terminates only a uniquely identified stopped-worker guard, and waits
for its exit before the read-only PostgreSQL export and replay. Any failure
before publication leaves the formal raw unchanged. Repaired startup remains
READ_ONLY and delegates account/market/intent safety to the unchanged signed
worker; no historical order is submitted.

Two isolated full September 4 recovery runs removed the same 25 source
sequences (including their retransmissions), filled only those committed gaps,
retained original evidence and passed signed semantic replay. Both produced
candidate SHA `dab198af3fd61f4d75c1607d8ddf429f595c007ba50a846a1cce7bea8dca0c4c`
and replay audit SHA `d9a7ad242ceac33c6c388cd5a952c0542f13c5afe48c62158d773c3fbf142821`.
The read-only formal raw gap preflight returned an empty interval list.

## Installed owner and remaining task gates

The final reviewed 1141 stage index was
`e85f63397bc9a89335057f7caba4a59a544f869a3609d4b3e6832226ea3f0c0c`.
Additive publication and installed `--once` preflight passed at 11:40.
Installed supervisor SHA is
`217c8e3fe62b9a56e2ecbeba0114f9a9b545fb75fd61d86f332d0f63450ff73b`,
manifest SHA is
`360290788a2ebe16e667f27995b3b0fe91d1547a631954d79cfbecbeaa9b000f`,
and immutable launcher SHA is
`821c8bddbeaf5180bda2414a5c9dcfb647446cb34c717a78e03bb4cfceffcd05`.
The single `tmux -L gridedge_codex` session `ths_session_supervisor` runs
PID 15709 with the tmux server as its parent. Independent inspection observed
the same PID's heartbeat advance after 16 seconds. This owner survives Codex
exit and handles the next reviewed 09:00/12:55 windows without an AI turn.

The original worker PID 50053, its 09:05:46 start time, signed core SHA,
runner/guard/plist identities, ledger/outbox and binding revision 15 were
preserved. The breaker SHA stayed
`7902facaf7bf29d46856397acd9378959bb41074c30033f79c16e198a95c0344`.
AC and battery evidence is now recorded through read-only `pmset`.
The frozen collector's heartbeat owns source-page presence and bounded ACK
recovery. The supervisor does not claim an external recovery path for a disabled
extension; nor does dependency supervision certify a completed trading bar.

All 33 independent/local ops cases and the full Rust fmt/strict Clippy/test gates
passed. Four real Android read-only commands (identity/orders/fills/cancellable)
returned success; cancellable contracts were empty. No money-action flag was
used in this preflight. Collector R2 and formal activation remain pending.
The 11:25–11:30 bucket has no authoritative source completion: the latest genuine
trade coverage is 11:29:48, so that bucket was not evaluated. It remains an open
production availability failure. Neither this publication nor a fresh HTTP clock
closes that separate evidence gap. Mathematical-rights and order semantics were
not changed.


## Confirmed cancellation follow-up

Independent RED found SUBMITTED+CANCELLED without FILLED was incorrectly
unresolved. The minimum SQL correction recognizes durable cancellation, while
non-SUBMITTED/CANCELLING/AMBIGUOUS without exact FILLED remains blocked.
FILLED still requires exact intent, contract and request identity. Ops 35/35 PASS.

The reviewed upgrade stopped only sidecar 15709. Current sidecar PID 17952,
PPID 48047 (tmux), was independently observed with advancing heartbeats.
Stage index: 58ff4bf1c4169a7195c9bfeb20f8ea29fa3281f2b8951e264cbd5e51ce096f10.
Supervisor: c4e322f8415955bcf5deaa31d3226557b48123e41700446079172bed0db9f385.
Manifest: a9ffa7958b0b447182cee14a7ce3f8180bdaa0856e82f013a73eb704f2047773.
Launcher: 9d5dacddd5f14bad9ad8fbfba8a714a2c300a8b2b632b95e675d5e3c1fac140f.
Worker PID 50053, its 09:05:46 start time, core/runner/guard/breaker hashes,
ledger/outbox 1902/1902 and binding revision 15 were unchanged.
Real read-only Android orders/fills/cancellable counts were all zero.
The final-bucket source-completion defect remains OPEN.

## Per-bar mode follow-up, 12:41

Independent fixtures first failed against the old report (missing per-bar mode
fields). The report now records the service mode at each journal stage, keeping
the existing sequence/type chain. All RUNNING stages report evaluation eligibility
true, all READ_ONLY stages false, and mixed or unknown stages null. Restoring
RUNNING after recovery cannot retroactively change a bar's result. This is a
read-only projection change; strategy, order, core and journal semantics did not
change. Independent scoped review and all 36 ops tests passed.

The formal 11:25 bar has READ_ONLY at stages 1895/1896/1897 and therefore reports
latest_bar_strategy_evaluated=false despite the current RUNNING state. This field
does not prove that an order opportunity existed or an order was submitted.

Current installed supervisor SHA:
8df496ae16a57d43ef42f609ac2a2cfd747ac65314c5cdae9772ea96ac6addcd.
Manifest: 4b1b9ee226bd97a5704c4f6238836e63f7f689a49b5572c778dc5b5b84df6ae9.
Launcher: a5d25ed6402db57efa13973c0e764bb45369f9d733bfdfcc0f70e801067ac35d.
Stage index: 6f801821e6d10aa3c9cb9187c97a65cbbc68741be9495f8ad630d481f38e46ec.
The unique tmux owner is PID 22058, PPID 48047; independent heartbeats advanced
at 12:40:46 and 12:41:18. Original worker PID 50053 and its birth time, core,
runner, guard, plist, breaker, binding revision 15 and head/cursor 1902/1902
were preserved. Installation receipt and subsequent live heartbeat are archived.

A concurrent duplicate publication attempt failed before file copying because
the immutable installation backup already existed. Its process-stop assertions
also failed before sending a signal. The designated publisher completed the
upgrade and removed transient non-tmux supervisor instances; only the verified
tmux owner remains. Future continuation must verify that owner before activation.
