# Core + supervisor anchor migration contract

Status: implementation and independent isolated failure-injection tests in progress; not a production release approval.

## Scope

A separate migration path may replace only `gridedge_ths_live`, the supervisor manifest and the manifest digest embedded in its launcher. The existing supervisor-only staging path retains its narrower permissions. All other eleven manifest-bound files remain byte-identical. The source of trust is an explicit previously reviewed manifest SHA, launcher SHA, effective core and execution binding revision/hash, plus exact candidate, validator and certification hashes. No development companion may enter this stage.

Stage construction reads and hashes the same frozen bytes, checks exact metadata and twelve-file allowlist, rejects symbolic links, and produces a content-indexed isolated directory. The new manifest changes exactly one file hash; the launcher changes exactly one literal manifest digest. Validation must recheck the frozen stage and installed companions before any owner is stopped.

## Exclusive transaction

The coordinator/maintenance owner spans revalidation, quiescence, authorization, publication, activation, binding and final synchronization. Quiescence requires native evidence of absent old supervisor/guard/worker PIDs and inodes, and absent launchd label. A retained dead tmux pane alone is not proof. The Android daily breaker cannot change.

Order: validate; acquire exclusive ownership; revalidate; quiesce and prove absence; append authorization; publish exact target core; activate only; publish exact new manifest and launcher; append execution identity using terminal read-only preflight; synchronize outbox; verify all identities and head/cursor; establish one supervisor. Its normal worker launch remains READ_ONLY and subject to all existing market/account gates.

Once authorization is appended, failures retain a maintenance fence and permit only exact-stage roll-forward. A file rollback or database snapshot restoration cannot undo journal facts. The durable transaction identity must bind the frozen stage index. Each retry observes existing pending/activated/bound facts and skips already committed work. Completion-response loss must not restart a healthy owner or repeat facts.

## Test boundary

The first implementation exposes a fixture-backed transaction protocol, not a native production apply command. Fake backend tests prove sequencing, replay and invariants but do not prove process identity, filesystem durability or Android preflight on a real host. A native adapter requires separate review of actual dual-lock recovery ownership, atomic rename/fsync, CLI receipt validation, PID birth/inode evidence, post-failure maintenance retention and idempotent continuation before it may be enabled.

Negative cases: wrong anchor/metadata/allowlist/baseline/candidate/index, companion mutation, existing owner after quiescence, launchd present, wrong pending transaction, binding fork, and failures before/after every file or journal mutation. No bad static stage may stop a healthy owner. No failure may start mixed identities, duplicate financial facts, clear the daily breaker, or overwrite the journal.

## September 14 isolated implementation evidence

`deploy/ops/core_identity_migration.py` now implements the frozen stage and fixture coordinator. Independent semantic red tests caught breaker rebaselining after an authorization-response failure and an unreachable launcher-before-manifest phase. Both were fixed; 18 tests, including 18 legal before/after transition interruption subcases, passed independent review.

`deploy/ops/core_migration_files.py` implements real adjacent file fsync/rename, immutable reservation directories, and exact-owner deployment fences. Darwin uses the SDK's `renameatx_np(..., RENAME_EXCL)` for no-replace publication; there is no check-then-overwrite fallback. Independent real-filesystem red tests caught missing retry fsync, replaced temporary inodes, redirected parents and foreign locks substituted after a prior owner check. Fixes bind parent descriptors, revalidate exact inodes/documents at publication, and sync the original directory descriptor. All 20 filesystem tests passed independent review, including the original legal interruption cases.

Red snapshots are retained at `/tmp/gridedge-core-migration-red.i79ed1`, `/tmp/gridedge-core-files-red.CdzOM1` and `/tmp/gridedge-core-files-parent-red.mkfiO2`. These tests are not proof against arbitrary kernel/adversarial races or approval for native deployment. Real SQLite fact reading, Rust command receipts and native process ownership integration are the next implementation boundary. No production apply CLI has been enabled.

The active supervisor was independently observed in the **default tmux socket**, session `gridedge_supervisor`, pane `%2`, PID 83040 (September 14 08:02:12). Other default-socket session `0` is unrelated user infrastructure. The `gridedge_codex` worker socket has no server after normal close. A native adapter must not infer supervisor ownership from the worker socket, kill a tmux server, or touch session `0`.

## Real SQLite / filesystem backend boundary

The independently reviewed `postclose_core_migration.py` now reads actual SQLite journal authorization/activation and append-only binding facts, and implements the transaction against real files with an injected OS/Rust host. The four suites passed 70 tests, twice independently, with ResourceWarning treated as an error. Additional red tests caught nullable/absent terminal contract evidence, a reservation-only retry incorrectly opening a new authorization outside the post-close window, and a false signature result being ignored. Evidence is retained in `/tmp/gridedge-postclose-reader-red.hV8Ej5`, `/tmp/gridedge-postclose-window-red.lC9sno`, and `/tmp/gridedge-postclose-signature-red.booTbL`.

This approval is explicitly limited to isolated real filesystem/SQLite with fake OS/Rust calls. Production remains disabled by the `/tmp`-home fence. The next native layer must verify complete kernel argv and PID birth/executable identity, clean tmux pane exit, native signing identities, and real Rust command effects. Starting a process is not sufficient: before releasing maintenance, require a new heartbeat from that exact PID/manifest with maintenance true and executed false; after release, require the same PID's subsequent CLOSED heartbeat without a worker. A stale heartbeat is not a receipt.

At 19:38 the unchanged old supervisor PID 83040 was freshly verified and power was AC 100%. Read-only native code signature verification passed for the frozen candidate. The Python framework launcher execs into its distinct Python.app executable, so native validation must explicitly bind both reviewed interpreter and executed-image identities; it must not silently broaden an inode comparison or adopt current paths as authorization.

## September 16 lunch: backend completion acceptance

Independent regression proved that the backend did not consume either lifecycle
heartbeat: both a first completion and `ALREADY_COMPLETE` could succeed even when
the injected host rejected every receipt. Eight independent cases failed before
the fix. Completion now requires a fresh maintenance acknowledgement from the
exact supervisor, unchanged complete state and owner identity, durable fence
release, then a subsequent released acknowledgement and another full-state check.
Host callbacks receive copies of the identity anchor. Release-wait changes to the
breaker, artifacts or journal cannot be accepted as completion.

A release acknowledgement failure leaves the published identities in place and
reports runtime acceptance failure; it does not fabricate a replacement fence,
repeat authorization or restart an already completed owner. Retry revalidates both
acknowledgements. Independent review passed 13 regression cases and the complete
45-test post-close suite. These are isolated filesystem/SQLite tests with an
injected host, not production acceptance. The `/tmp` fence remains enabled.

Remaining integration is explicit: a reviewed native host must compose exact
process routing, bounded allowlisted Rust CLI effects and lifecycle receipts in
real isolated end-to-end tests before a production apply path can be enabled.
The lifecycle currently accepts only CLOSED/non-executing receipts; its contract
must not be silently weakened for a cross-day trading-hours retry. This repair
does not supply missing quiet-tape/final-bucket market evidence and does not
constitute deployment of the pending core candidate.

The same-turn format check, strict all-target/all-feature Clippy, full Rust test
suite, 111 migration/native/lifecycle tests and isolated sample end-to-end replay
also passed. The replay used a fresh `/tmp` working directory and independent
SQLite database; no production account or installed artifact was changed.
