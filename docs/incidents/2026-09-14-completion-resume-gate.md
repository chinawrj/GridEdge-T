# 2026-09-14 completion/resume gate repair — not deployed

## Observed production outcome

September 14 remains an operational failure. The journal contains 45 completed bar triplets, 41 received in READ_ONLY and four in RUNNING. The 09:35 and 09:55 grid touches were skipped with SERVICE_MODE_READONLY. The 11:20 touch was genuinely deferred by the algorithm (score 0.3113888244713754834418772558, threshold 0.60). There were no new order intents or fills. Bars are END-labelled (`market_bar.timestamp = self.end`): independent baseline SQL audit found three missing expected endings, 11:30, 14:55 and 15:00. Earlier wording calling 14:55 the final bar was incorrect. At 19:00, both SQLite integrity checks passed and head/cursor remained 2448/2448.

## Independently reproduced defect and limited fix

The old completion closure replaced a completion's own freshness and continuity with source-observation evidence. A fresh observation could therefore admit stale advancing trade coverage or mask a coverage gap. Empty released bars did not block this false recovery.

The production closure now calls `completion_gate_evidence`, independently checks completion freshness, predecessor continuity and current session, and ANDs these with observation evidence when present. No-observation compatibility retains completion checks. Observation cannot substitute for trade coverage. Bar processing, terminal reconciliation, then resume ordering is unchanged.

Independent tests in `tests/ths_live_completion_gate_regression.rs` reproduced four failures before the fix, including real builder zero-bar receipts. All twelve independent cases pass after the fix (18 tests including imported production unit tests). The independent reviewer approved this limited fix. It does not solve quiet-tape completeness, missing terminal source evidence, or establish that all 130 mode transitions today had this cause.

## Validation and frozen candidate

- `cargo fmt --check`: PASS.
- Strict all-target/all-feature Clippy: PASS.
- `cargo test --all-targets --all-features`: PASS, process exit 0.
- Isolated fixture replay: PASS, run 79346df2-34f9-4122-bdac-f9a702c61f53, 233 events, no duplicate events or open orders. No formal broker or Android money action.
- Production source SHA: 7d8bb07d5f82c36e1f4c5c22252675c315e89346ba2a8f35940273428171ac19.
- Independent test SHA: 9f1ad07e3e1e8b6964aa64a9c5ea567c6e313b9b825ec0bc44efe5827cf36335.
- Signed worker candidate: 0af7f40ab66a5a08565164b04c4858ba04c52cf35c854bae07fb970c9a471c8a.
- Frozen validator: 87000e77e15821997695cf23f611acdba6593814c9b0e7f83f1d64bc8db1fd7a.
- Red/green and pre-fix source evidence: installation runtime `e2e-audits/20260914-completion-gate/`.

## Deployment is not complete

Installed/effective core remains 149ca55f6ae239695f22e132d56b6cfcfd14514f230b4fe349e3fc9bb50e8422. Existing supervisor manifest d6386e00060b7c7e0120aa6453441b592385ecc64acbbc430ae08bd7c4e43387 binds that old core. Independent release review found no existing atomic reviewed core-plus-supervisor-anchor migration. Chaining the core installer and supervisor-only publisher would create mixed identity and BLOCKED_IDENTITY. No installation, authorization, activation, or trust-anchor rewrite was performed.

The next release work must introduce an explicit old-anchor-to-new-core migration with frozen companions, one exclusive owner across quiescence and publication, append-only authorization/activation/binding, failure injection and idempotent roll-forward. Existing supervisor-only trust rules must not be relaxed. Do not rehash dirty development companions into approval or roll back journal facts by restoring a database snapshot.

## Evening preflight evidence and remaining boundaries

The live app-independent supervisor verified all twelve installed files and its next-day 09:00/09:25 preflight, 09:30/09:35 trading and lunch windows. The scheduler's actual persisted next run was 2026-09-15 08:01:11 Asia/Shanghai. These are scheduling/dependency evidence, not tomorrow's trading acceptance.

The frozen Android read-only probe passed identity, orders, fills and cancellable checks with empty lists. This is not a newly certified cash/fees/holdings reconciliation. No money actions were enabled. Computer Use subsequently refused the current browser URL; no alternate browser-control path or security-boundary bypass was attempted. Full live source activation and current-bar RUNNING acceptance remain unproven.

## Evening continuation through 20:05

The core/manifest/launcher migration now has independently tested frozen staging, real filesystem durability and SQLite-fact recovery layers. Native read-only process identity uses kernel argv rather than a lossy process-list string; a separately hashed SDK-compiled read-only helper distinguishes positively proven zombies from executable owners without treating them as absent. No unknown process was killed.

Two fresh formal-ledger backup copies successfully ran the actual signed validator authorization and candidate activate-only commands. Both preserved the full prior journal and all Paper/outbox tables, adding only AUTH 2449, ACT 2450 and RECOVERY_COMPLETED 2451. Repeated commands failed without additional events. See `docs/reviews/20260914-native-cli-clone.md`; these are isolated test receipts, not a production certificate.

Real isolated tmux lifecycle testing caught clean-exit sampling races and unrelated-session handling. Durable pre-termination and clean-terminal receipts are now implemented, including same-transaction recovery after interruption. Independent fault injection found a replaced receipt directory could still authorize a signal; the fix binds directory/file descriptors, inode and canonical bytes across fsync. All 27 lifecycle tests pass locally and in two independent full rounds (10.433s/10.486s, ResourceWarning treated as error). Native Host integration and the production apply fence remain disabled; an ALREADY_COMPLETE resume must still verify the new owner's fresh maintenance heartbeat and post-release CLOSED heartbeat, not merely count one PID.

An independently reviewed R2 bind-only attempt failed immediately because ADB reported no connected emulator; no APP navigation or binding upgrade occurred. A manual AC assertion initially stopped recovery and the user was incorrectly told reconnecting power was required. Inspection confirmed the frozen supervisor has no such post-close gate: battery was adequate and the screen unlocked. That operator-added restriction was corrected and the reviewed bounded no-snapshot/no-wipe emulator launcher was invoked at 97% battery. This post-close dependency change did not cause today's missed trades and is not a substitute explanation for the unresolved market-source incident.

Formal supervisor remains PID 83040 with the original birth/manifest, installed/effective core remains 149ca55f..., ledger/outbox remain 2448/2448 and binding revision 15. No formal authorization, activation, artifact replacement or financial action occurred. Repair is not complete.

After the emulator and exact simulation page recovered, R2 bind-only passed against the real read-only APP preflight, appending clone binding 16 and synchronizing clone cursor to head 2451. Backup comparisons preserve all original binding rows and account/order/fill tables. This does not resolve source finality/quiet-tape coverage or the missing 11:30, 14:55 and 15:00 endings, and those must not be relabeled as merely pending live acceptance without a proven source contract. A same-symbol/session complete OHLC finalization/revision contract, or native segment/sequence/totals plus exact complete-history reconciliation, remains unproven; no such route is admitted by this repair. September 7 established a quiet-tape coverage failure, but this review does not prove all September 14 omissions or mode transitions share that cause.
