# September 14 isolated native CLI evidence — not production approval

Two fresh, closed SQLite-backup copies of the formal ledger/outbox were exercised:

- R1: `/tmp/gridedge-native-cli-clone.29V5EX`
- R2: `/tmp/gridedge-native-cli-clone-r2.x8tloI`
- Unmodified comparison backup: `/tmp/gridedge-core-migration-rebuild.qexTci`

Each configuration changes only the database path into its isolated directory. The certificate explicitly says `ISOLATED_CLONE_TEST_ONLY_NOT_PRODUCTION_APPROVAL`; it is a test input and must never authorize the formal installation. No broker or Android process was launched. Activate-only was passed nonexistent local credential/ADB paths and unused local log paths, proving those dependencies were not reached on this path.

Frozen signed validator SHA: `87000e77e15821997695cf23f611acdba6593814c9b0e7f83f1d64bc8db1fd7a`. Candidate worker SHA: `0af7f40ab66a5a08565164b04c4858ba04c52cf35c854bae07fb970c9a471c8a`.

Actual `authorize-platform-upgrade` and `--activate-platform-upgrade-only` exited zero in each round. Durable facts, independently read from both backups, are:

| Run sequence | Fact |
| --- | --- |
| 2448 | Unmodified source prefix ends |
| 2449 | PLATFORM_UPGRADE_AUTHORIZED, expected head 2448 |
| 2450 | PLATFORM_UPGRADE_ACTIVATED, exact adjacent authorization ID/sequence and target SHA |
| 2451 | RECOVERY_COMPLETED, zero unfinished orders |

The original run event prefix is unchanged. Paper accounts, orders and trade-date tables, and every outbox table are unchanged. No market log was created. Repeated direct authorization/activation attempts fail (`target is current or appears in platform history` / `requires one pending durable platform authorization`) and do not append another event. The transaction backend must read durable truth and skip these commands after response loss; direct CLI reexecution is deliberately not a successful no-op.

Initial command exit results are in the task's tool history, not archived stdout files; the SQLite facts are the retained authoritative receipts. R1 repeat-command stderr is also retained alongside the clone. This report does not invent missing historical logs.

This verifies real Rust authorization/activation and accounting preservation, not a complete native deployment. Execution-binding upgrade, real supervised process handoff and post-maintenance heartbeat acceptance remain separate gates. Formal ledger/outbox stayed at 2448/2448, binding revision 15, with both integrity checks OK; formal installed/effective identity and supervisor PID 83040 were not changed.

## Subsequent R2 bind-only attempt

After independent source-path review, a narrowly scoped real Android startup-preflight plus clone-outbox binding attempt was made. It exited 1 immediately: `Android Tonghuashun requires exactly one reviewed connected emulator`. Fresh `adb devices -l` was empty. It did not reach APP navigation or append binding revision 16. `bind-only.log` is retained in R2. A manually added AC assertion initially stopped emulator recovery; this was an operator-added restriction, not a gate in the frozen supervisor. After correcting that mistake, the reviewed no-snapshot, no-wipe emulator launcher was invoked with identity, unlocked-screen, absent-owner and adequate-battery checks (97%, estimated eight hours). This launch alone is not successful remote binding or financial-account reconciliation.

The subsequent attempt correctly refused while the app was not resumed. After opening the exact reviewed package and navigating through its application catalogue to 模拟炒股, the title and **0208 marker were observed. The same frozen candidate's `--bind-execution-identity-only` then exited zero. R2 now has binding revision 16, identity `f2bbc87e47ff34f0cbfbfb5a6dc9fa2a387c1c76f0ea459c551100da235c05e0`, previous identity `1be375fb951fd03a25f5f98100d43a746bea5f1ff7619f16ae73fba3b483bd53`, and head/cursor 2451/2451. Original binding rows 1..15 and all non-event/non-metadata/account/order/fill tables compare exactly with the backup. Both integrity checks pass. No prepare/submit/cancel path was invoked. Formal state remains head/cursor 2448/2448, binding 15 and core 149ca55f… . This is clone binding evidence, not a production upgrade or current-market acceptance.
