# Android dump exit status is not a fresh UI receipt

Status: OPEN; production artifact has not been replaced.

## Observed failure (Asia/Shanghai, 2026-09-19)

During the 18:58 weekend recovery patrol, the reviewed emulator was absent.
The approved supervisor launch function restored THSP_API_32 with user data
preserved and snapshot loading/saving disabled. The frozen read-only Android
probe first reported that THS was not resumed. Starting the exact reviewed THS
activity succeeded; a subsequent probe timed out after 45 seconds. A bounded
retry later reported `Android simulation tab is unavailable`.

Direct diagnostic `uiautomator dump` returned exit status 0 while printing
`ERROR: could not get idle state.`. The prior shared XML file remained present.
The adapter's `snapshot()` ignored dump stdout and immediately read that file.
Consequently successful process exit was incorrectly treated as fresh UI evidence.
Screenshots and the retained XML showed different pages. No financial action was
attempted, and this evidence does not prove that a particular wrong click occurred.

The three system animation scales were already zero. Reapplying animation settings
is not a repair. A later dump produced the real success receipt:
`UI hierchary dumped to: /sdcard/gridedge-ths-window.xml`.

## Required repair and acceptance

Before reading XML, require the exact successful dump receipt for the requested
path. Empty output, idle errors, another path, or contradictory error/success text
must reject the attempt without reading stale XML or issuing UI input. Preserve
bounded retries and all existing account/package/version checks. Independently
prove the old implementation fails using a valid but stale simulation-form XML;
then require zero XML reads and zero inputs on failed dumps, a successful fresh
retry, normal receipt compatibility, and the existing Android execution suite.

The installed core remains `149ca55f6ae239695f22e132d56b6cfcfd14514f230b4fe349e3fc9bb50e8422`
at execution binding revision 15. Source/test changes alone do not repair it.
The core+supervisor migration backend remains isolated under its `/tmp` fence;
do not bypass that fence, replace a bound binary directly, or alter the breaker.
Production release and an actual read-only account preflight remain required.

## Separate operational evidence

The NAS was reachable at SSH 22, MQTT TLS 8883 and browser WebSocket 9001 in this
patrol. PostgreSQL contains production sequences 1..57754; local raw still ends
at 56285. The 1469 additional exact payloads were checked for stored SHA, embedded
sequence and source identity. All have source receipt dates on September 17,
although the last database commit was September 18 19:53:34.411836. These are
delayed historical facts, not Friday strategy execution. Delivery counts were
1418 records at one and 51 at two; global conflicts/rejections remain 1/5.

Friday September 18 has zero journal events by both event time and recorded-at
Shanghai day boundaries. Supervisor records during 09:00–15:00 contain 392
`BLOCKED_UNLOCKED` observations, no unlocked observation and no guard/worker.
Friday is operationally FAILED, not a market-no-trigger day.

The missing reviewed Chrome URL was restored as native persistent tab 1034336408.
Latest-first was checked and no CAPTCHA was visible then. Actual popup identity
remains manifest 0.6.53 / build collector-0.6.51-stale-trade-coverage-only-v1,
accepted rows 32753, acknowledged events 54991, pending 4060, conflicts 0.
The popup reports `capture is on a non-trading day`. No capture, resend, reload,
queue reset, historical order or source-admission change was requested. The
pending queue and existing source-finality/activation defects are not repaired.

## Source repair and validation completed at 19:17

`src/ths_android_sim.rs::snapshot()` now requires the trimmed exact success
receipt above before reading XML. Independent regression first failed six of
seven cases on the old implementation, then passed all seven with the fix.
It covers empty/error/wrong-path/contradictory receipts, stale landing evidence
without navigation, successful receipt compatibility and transient retry.
The existing Android suite passed 26/26. Independent review approved the scoped
source change and isolated read-only validation, not production deployment.

`cargo fmt --check`, strict all-target/all-feature Clippy and the complete
all-target/all-feature test suite exited zero. Explicit real-account smoke tests
remained ignored; no test order was authorized. Both debug CLI binaries built.
An isolated sample replay in `/tmp/gridedge-android-dump-replay.7BmlDe` exited
zero with 233 events and zero open orders; it did not use the formal ledger or
Android execution. The rebuilt debug read-only `probe` was externally bounded
at 45 seconds and timed out. Therefore real identity/orders/fills/cancellable
acceptance has NOT passed. No prepare/submit/cancel action was performed.

Residual limitation: `SystemAdb::run` has no per-command wall-clock deadline.
Ten retry attempts are not a bound on total subprocess execution time. This
patch fixes stale XML admission, not Android UI availability or that deadline.

## Persistent safety fence and remaining release work

At approximately 19:09, under exclusive ownership and with guard/worker absent,
automation-2 established the existing deployment coordination and maintenance
directories. Both contain `android-stale-dump-owner.json`, state
`SAFETY_HOLD_PENDING_REVIEWED_RELEASE`. These are deliberate safety ownership
receipts, not stale locks to clear because the prior operator process ended.
Do not remove them until a reviewed signed same-run release and fresh read-only
account acceptance resolve this incident. Never reset the daily breaker.

At 19:16:34 the reviewed supervisor PID 19210 independently reported
`BLOCKED_MAINTENANCE`. Fresh 19:16:54 verification found AC100, unlocked,
reviewed emulator booted, Chrome and MQTT reachable, installed identities valid,
guard/worker both absent, ledger/outbox 2742/2742 and the breaker unchanged.
The native reviewed market tab 1034336408 remained present on final inventory.
The installed core and effective revision remain unchanged: no signing,
installation, activation or new binding was performed. The approved migration
backend's isolated-only fence is not permission to replace installed bytes.

Production restoration remains OPEN. Next work is bounded Android availability
diagnosis and fresh read-only account acceptance, followed by completion and
review of the same-run publication path. Existing source finality, mixed
extension identity and queued-event admission also remain open. No engineering
repair process is left running; the independent supervisor continues safety
supervision only. The original failed delivery acceptance is unchanged.
