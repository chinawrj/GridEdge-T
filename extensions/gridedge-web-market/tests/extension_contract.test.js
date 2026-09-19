"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const root = path.join(__dirname, "..");
const manifest = JSON.parse(fs.readFileSync(path.join(root, "manifest.json"), "utf8"));
const sources = [
  "src/background.js",
  "src/mqtt_namespace.js",
  "src/mqtt_ack.js",
  "src/outbox_delivery.js",
  "src/source_heartbeat.js",
  "src/content.js",
  "src/page_stability.js",
  "src/durable.js",
  "src/options.js",
  "src/popup.js",
  "src/providers/eastmoney.js",
  "src/shared.js",
].map((name) => [name, fs.readFileSync(path.join(root, name), "utf8")]);

test("extension is self-contained and can reach only Eastmoney, production MQTT, or loopback E2E", () => {
  assert.equal(manifest.manifest_version, 3);
  assert.deepEqual(manifest.host_permissions, [
    "https://quote.eastmoney.com/*",
    "http://192.168.1.201/*",
    "http://127.0.0.1/*",
  ]);
  assert.match(manifest.content_security_policy.extension_pages,
    /connect-src ws:\/\/192\.168\.1\.201:9001 ws:\/\/127\.0\.0\.1:19001/);
  assert.equal(JSON.stringify(manifest).includes("<all_urls>"), false);
  const allSources = sources.map(([, source]) => source).join("\n").toLowerCase();
  assert.equal(allSources.includes("postgres"), false);
  assert.equal(allSources.includes("companion"), false);
  assert.match(allSources, /account_marker is not market data/);
  const namespace = Object.fromEntries(sources)["src/mqtt_namespace.js"];
  assert.match(namespace, /ISOLATED_READ_ONLY_E2E/);
  assert.match(namespace, /production MQTT endpoint/);
  assert.match(namespace, /isolated E2E MQTT endpoint/);
  assert.match(namespace, /nonce-bound client identity/);
});

test("collector is disabled by default and waits for database commit ACK after MQTT PUBACK", () => {
  const background = Object.fromEntries(sources)["src/background.js"];
  assert.match(background, /enabled:\s*false/);
  assert.match(background, /protocolVersion:\s*5/);
  assert.match(background, /qos:\s*1/);
  assert.match(background, /GridEdgeDurable\.ingestCapture/);
  assert.match(background, /GridEdgeDurable\.ingestResumeBoundary/);
  assert.match(background, /GRIDEDGE_RESUME_BOUNDARY/);
  assert.match(background, /GRIDEDGE_COMPLETE_HISTORY_BRIDGE/);
  assert.match(background, /requireCompleteHistoryBridge:\s*completeHistoryBridge/);
  const delivery = background.slice(
    background.indexOf("async function deliverCapture"),
    background.indexOf("async function scanActiveTab"),
  );
  assert.ok(delivery.indexOf("GridEdgeDurable.ingestCapture") < delivery.indexOf("flushOutbox()"));
  assert.match(background, /GridEdgeOutboxDelivery\.flushPending/);
  const outboxDelivery = Object.fromEntries(sources)["src/outbox_delivery.js"];
  assert.match(outboxDelivery, /mqttAck\.waitForCommittedAck/);
  assert.match(outboxDelivery, /durable\.acknowledge\(database, event\.event_id/);
  assert.match(background, /STORE_GENERATION_CHANGED/);
  assert.match(background, /GridEdgeDurable\.DATABASE_NAME/);
  assert.doesNotMatch(outboxDelivery, /publishWithPuback\(client, event\);\s*await durable\.acknowledge/s);
  assert.match(background, /OUTBOX_ALARM\s*=\s*"gridedge-mqtt-outbox"/);
  assert.match(background, /chrome\.alarms\.create\(OUTBOX_ALARM/);
  assert.doesNotMatch(background, /fetch\(/);
});

test("runtime status proves the loaded collector version and critical script bytes", () => {
  const background = Object.fromEntries(sources)["src/background.js"];
  const popup = Object.fromEntries(sources)["src/popup.js"];
  assert.match(background, /chrome\.runtime\.getManifest\(\)\.version/);
  assert.match(background, /collector-0\.6\.51-stale-trade-coverage-only-v1/);
  assert.match(background, /runtime_identity/);
  assert.match(popup, /runtime_identity:\s*response\.runtime_identity/);
});

test("durable capture ingestion never serializes the source heartbeat behind database ACK latency", () => {
  const background = Object.fromEntries(sources)["src/background.js"];
  const delivery = background.slice(
    background.indexOf("async function deliverCapture"),
    background.indexOf("async function scanActiveTab"),
  );
  assert.match(delivery, /GridEdgeDurable\.ingestCapture/);
  assert.match(delivery, /sourceObservationOnly: message\.source_observation_policy != null/);
  assert.doesNotMatch(delivery, /await flushOutbox\(\)/);
  assert.match(delivery, /void flushOutbox\(\)\.catch/);
  assert.match(delivery, /queued:\s*store\.pending_events/);
});

test("MQTT credentials live in extension storage and never enter content scripts", () => {
  const background = Object.fromEntries(sources)["src/background.js"];
  const content = Object.fromEntries(sources)["src/content.js"];
  const options = Object.fromEntries(sources)["src/options.js"];
  assert.match(background, /mqtt_password/);
  assert.match(options, /mqtt_password/);
  assert.doesNotMatch(content, /mqtt_password|mqtt_username|mqtt_url/i);
  assert.match(background, /mqtt_password: current\.mqtt_password \? "\*\*\*" : ""/);
});

test("content collector serializes manual, mutation, stability, and retry scans", () => {
  const content = Object.fromEntries(sources)["src/content.js"];
  assert.match(content, /GRIDEDGE_SCAN_NOW/);
  assert.match(content, /createIndependentHeartbeatRouter\(scanOnce,\s*\{/);
  assert.match(content, /queued === "manual" \|\| incoming === "manual"/);
  assert.match(content, /queued === "heartbeat" \|\| incoming === "heartbeat"/);
  assert.match(content, /requestScan\("manual"\)/);
  assert.match(content, /requestScan\("mutation"\)/);
  assert.match(content, /observer\.observe\(document\.documentElement,\s*\{[\s\S]*childList:\s*true,[\s\S]*characterData:\s*true,[\s\S]*subtree:\s*true,[\s\S]*\}\)/);
  assert.match(content, /WAITING_FOR_STABLE_ROWSET/);
  assert.match(content, /requestScan\("stability"\)/);
  assert.match(content, /requestScan\("error-retry"\)/);
  assert.match(content, /cycleLatestFirstControl/);
  assert.match(content, /STALE_TRADE_COVERAGE/);
  assert.match(content, /captured_at_us: 0/);
});

test("content collector publishes only a clock-bound latest-first source observation", () => {
  const content = Object.fromEntries(sources)["src/content.js"];
  assert.match(
    content,
    /SCAN_HEARTBEAT_MS\s*=\s*12_000/,
    "the source heartbeat must retain enough scheduling and DB-ACK headroom to meet the 45-second operating target",
  );
  assert.match(content, /installSourceHeartbeat\(requestScan/);
  assert.match(content, /captureServerClockBoundSourceObservation\(/);
  assert.match(content, /SOURCE_OBSERVATION_DEADLINE_MS = 12_000/);
  assert.match(content, /REGULAR_SCAN_DEADLINE_MS = 8_000/);
  assert.match(content, /atomicReviewedPageCapture\(/);
  assert.match(content, /const regularDeadline = reason === "heartbeat" \? null : pageStability\.createDeadline/);
  assert.match(content, /readReviewedSnapshot\(regularDeadline\)/);
  assert.match(content, /navigateHistory\("首页", 1, regularDeadline\)/);
  assert.match(content, /stablePageCapture\(1, staleRowsetHash, 6, regularDeadline\)/);
  assert.match(content, /pageStability\.mergeRollingPageCaptures\(/);
  assert.match(content, /core\.stableMarketRowEvidence\(row\)/);
  assert.match(content, /WAITING_FOR_CONTIGUOUS_ROWSET/);
  assert.match(content, /establishResumeBoundary\(recoveryDeadline\)/);
  assert.match(content, /pageStability\.shouldRecoverLiveOverlap\(reason, error\)/);
  assert.match(content, /deadline\.run\(\(\) => deliverCapture\(/);
  assert.match(content, /function reportCaptureError\(/);
  assert.doesNotMatch(content, /await chrome\.runtime\.sendMessage\(\{\s*type: "GRIDEDGE_CAPTURE_ERROR"/s);
  assert.match(content, /createDeadline\(/);
  assert.match(content, /Math\.max\(1, Math\.min\(5_000, deadline\.remainingMs\(\)\)\)/);
  assert.match(content, /createUiMutationGuard\(/);
  assert.match(content, /REVIEWED_UI_CHANGED_DURING_HEARTBEAT/);
  assert.match(content, /reviewedControlIsUnchanged\(reviewedControl, latestFirstCheckbox\(\)\)/);
  assert.match(content, /sourceServerObservedAtUs/);
  assert.match(content, /validateServerClockBoundSourceObservationTiming/);
  assert.match(content, /REVIEWED_EASTMONEY_HTTPS_DATE_LATEST_FIRST_V3/);
  assert.match(content, /createIndependentHeartbeatRouter\(scanOnce/);
  const heartbeatBranch = content.slice(
    content.indexOf('if (reason === "heartbeat")'),
    content.indexOf("if (!ensureLatestFirst())"),
  );
  assert.match(heartbeatBranch, /atomicReviewedPageCapture/);
  assert.doesNotMatch(heartbeatBranch, /stablePageCapture/);
  assert.match(content, /routeCollectorInitialization\(/);
  assert.match(content, /atomicReviewedPageCapture\([\s\S]*expectedPageIndex,[\s\S]*reviewedControlMismatchDeadline/);
  assert.match(content, /shouldDeliverCapture\(reason, captureHash, lastDeliveredRowsetHash\)/);
  assert.match(content, /GRIDEDGE_SOURCE_HEARTBEAT/);
});

test("rolling page continuity and durable deduplication share one stable row projection", () => {
  const shared = Object.fromEntries(sources)["src/shared.js"];
  const content = Object.fromEntries(sources)["src/content.js"];
  const durable = Object.fromEntries(sources)["src/durable.js"];
  assert.match(shared, /function stableMarketRowEvidence\(row\)/);
  for (const field of [
    "source_table_ordinal",
    "source_row_ordinal",
    "source_same_second_ordinal",
    "raw_cells",
  ]) {
    assert.match(shared, new RegExp(`${field}: _`));
  }
  assert.match(content, /core\.canonicalJson\(core\.stableMarketRowEvidence\(row\)\)/);
  assert.match(durable, /row: core\.stableMarketRowEvidence\(row\)/);
  assert.match(durable, /core\.canonicalJson\(core\.stableMarketRowEvidence\(row\)\)/);
  assert.doesNotMatch(content, /source_same_second_ordinal: _sourceSameSecondOrdinal/);
  assert.doesNotMatch(durable, /function stableRowEvidence/);
});

test("MV3 background alarm independently wakes throttled source heartbeats", () => {
  const background = Object.fromEntries(sources)["src/background.js"];
  const content = Object.fromEntries(sources)["src/content.js"];
  const sourceHeartbeat = Object.fromEntries(sources)["src/source_heartbeat.js"];
  assert.match(background, /source_heartbeat\.js/);
  assert.match(background, /installSourceHeartbeatAlarmWatchdog/);
  assert.match(sourceHeartbeat, /gridedge-source-heartbeat-phase-/);
  assert.match(sourceHeartbeat, /HEARTBEAT_ALARM_PHASE_MS\s*=\s*Object\.freeze\(\[7_500, 15_000, 22_500, 30_000\]\)/);
  assert.match(sourceHeartbeat, /HEARTBEAT_ALARM_PERIOD_MINUTES\s*=\s*0\.5/);
  assert.match(background, /GridEdgeSourceHeartbeat\.dispatchSourceHeartbeat/);
  assert.match(background,
    /recoverTab:\s*async\s*\(tab, response\)\s*=>\s*response\.reason === "STALE_TRADE_COVERAGE"/,
    "background reload must be limited to live trade-coverage staleness");
  assert.match(background, /chrome\.storage\.session/,
    "live stale-coverage reloads require an MV3-persistent cooldown");
  assert.match(content, /captureReviewedInitializationPage/);
  assert.match(content, /initialization_error:\s*lastInitializationError/);
});

test("MV3 watchdog wires bounded reviewed-tab reload recovery into every heartbeat", () => {
  const background = Object.fromEntries(sources)["src/background.js"];
  const content = Object.fromEntries(sources)["src/content.js"];
  assert.match(background, /createReviewedTabRecovery/);
  assert.match(background, /chrome\.storage\.session/,
    "reload cooldown must survive MV3 service-worker re-evaluation");
  assert.match(background, /recoverTab:\s*async\s*\(tab, response\)/);
  assert.match(background, /reviewedTabRecovery\.request\(tab, response\)/);
  assert.match(content, /deliverStatusOnlyObservationAndClassifyTradeCoverage/,
    "the production heartbeat must surface stale coverage only after status delivery");
});

test("content collector retries only the reviewed transient DOM order mismatch", () => {
  const content = Object.fromEntries(sources)["src/content.js"];
  assert.match(content, /readCaptureWithRetry/);
  assert.match(content, /Eastmoney time-sales DOM order disagrees with its reviewed control/);
  assert.match(content, /isRetriableError:\s*isRetriableReviewedControlError/);
  assert.match(content, /maxAttempts:\s*MAX_REVIEWED_SNAPSHOT_ATTEMPTS/);
  assert.match(content, /maxStateAttempts:\s*60/);
  assert.match(content, /deadline,\s*\n\s*\}\);/);
  assert.match(content, /createLaneFailureBudget/);
  assert.match(content, /reason === "heartbeat" \? "heartbeat" : "regular"/);
  assert.match(content, /reason:\s*"STALE_TRADE_COVERAGE"/);
  assert.match(content, /recoverReviewedControlMismatch/);
  assert.match(content, /createReviewedControlRecoveryCoordinator/);
  assert.match(content, /reason === "reviewed-control-recovery"/);
  assert.match(content, /requestImmediateHeartbeat:\s*\(\) => scanRunner\.request\("heartbeat"\)/);
  assert.match(content, /REVIEWED_CONTROL_MISMATCH_DEADLINE_MS\s*=\s*4_000/);
  assert.match(content, /timeoutMs:\s*REVIEWED_CONTROL_MISMATCH_DEADLINE_MS/);
  assert.match(content, /preserveLastRetriableOnDeadline:\s*preserveMismatchAtDeadline/);
  assert.match(content, /reviewedControlMismatchDeadline,[\s\S]*true,/);
  assert.match(content, /reviewedControlRecovery\?\.beginRecovery\(\)/);
  assert.match(content, /reviewedControlRecovery\.completeRecovery\(true\)/);
  assert.match(content, /reviewedControlRecovery\.completeRecovery\(false\)/);
  assert.match(content, /queued === "reviewed-control-recovery"[\s\S]*incoming === "reviewed-control-recovery"/);
  const heartbeatStart = content.indexOf('if (reason === "heartbeat")');
  const heartbeatEnd = content.indexOf('if (reason === "reviewed-control-recovery")');
  assert.ok(heartbeatStart >= 0 && heartbeatEnd > heartbeatStart);
  const heartbeat = content.slice(heartbeatStart, heartbeatEnd);
  assert.doesNotMatch(heartbeat, /refreshLatestFirst/,
    "heartbeat must queue reviewed UI recovery without clicking the control");
  assert.doesNotMatch(heartbeat, /ensureLatestFirst|\.click\(/,
    "heartbeat mismatch handling must remain status-only");
  const recoveryEnd = content.indexOf('if (!ensureLatestFirst())', heartbeatEnd);
  assert.ok(recoveryEnd > heartbeatEnd);
  const recovery = content.slice(heartbeatEnd, recoveryEnd);
  assert.match(recovery, /recoverReviewedControlMismatch/);
  assert.match(recovery, /REVIEWED_CONTROL_RECOVERED/);
  assert.doesNotMatch(recovery, /deliverCapture|durable|chrome\.runtime\.sendMessage/,
    "the dedicated UI repair must not publish or mutate durable market state");
  const failureStart = content.indexOf('if (reason === "reviewed-control-recovery") {',
    content.indexOf('scanFailureBudget.recordFailure(lane)'));
  const failureEnd = content.indexOf('if (lane === "heartbeat"', failureStart);
  assert.ok(failureStart >= 0 && failureEnd > failureStart);
  const dedicatedFailure = content.slice(failureStart, failureEnd);
  assert.match(dedicatedFailure, /return \{ ok: false, reason: message \}/);
  assert.doesNotMatch(dedicatedFailure, /scheduleRegularRetry|requestScan/,
    "a failed dedicated repair must wait for a new external heartbeat");
});

test("stale regular trade coverage cannot occupy the reviewed UI mutation lane", () => {
  const content = Object.fromEntries(sources)["src/content.js"];
  const scan = content.slice(
    content.indexOf("async function scanOnce"),
    content.indexOf("let reviewedControlRecovery"),
  );
  assert.match(scan, /reason:\s*"STALE_TRADE_COVERAGE"/);
  assert.doesNotMatch(
    scan,
    /refreshStaleFirstPage/,
    "no-trade intervals must keep status heartbeats alive while the worker's independent trade-coverage gate remains READ_ONLY",
  );
});

test("popup can export exact durable MQTT replay bytes without database access", () => {
  const popupHtml = fs.readFileSync(path.join(root, "src/popup.html"), "utf8");
  const popup = Object.fromEntries(sources)["src/popup.js"];
  assert.match(popupHtml, /id="export"/);
  assert.ok(popupHtml.indexOf('src="shared.js"') < popupHtml.indexOf('src="durable.js"'));
  assert.ok(popupHtml.indexOf('src="durable.js"') < popupHtml.indexOf('src="popup.js"'));
  assert.match(popup, /GridEdgeDurable\.replayExport/);
  assert.match(popup, /application\/x-ndjson/);
  assert.match(popup, /exportButton\.disabled = true/);
  assert.match(popup, /finally\s*\{[\s\S]*database\?\.close\(\)[\s\S]*exportButton\.disabled = false/);
  assert.match(popup, /document\.body\.append\(anchor\)/);
  assert.match(popup, /setTimeout\([\s\S]*URL\.revokeObjectURL\(url\)/);
  assert.match(popup, /gridedge-web-market-v5/);
  assert.match(popup, /LOCAL_BROWSER_FORENSIC/);
  assert.doesNotMatch(popup, /deleteDatabase/);
  assert.doesNotMatch(popup, /postgres|fetch\(/i);
});

test("content collector selects the reviewed latest-first control before capturing rows", () => {
  const content = Object.fromEntries(sources)["src/content.js"];
  assert.match(content, /function ensureLatestFirst/);
  assert.match(content, /input\[type="checkbox"\]/);
  assert.match(content, /core\.normalizeText\(label\.textContent\) === "倒序"/);
  assert.match(content, /checkbox\.click\(\)/);
  assert.ok(content.indexOf("ensureLatestFirst()") < content.indexOf("provider.parseSnapshot"));
});

test("content collector finishes a bounded in-memory history crawl before publishing or observing live mutations", () => {
  const content = Object.fromEntries(sources)["src/content.js"];
  const pageStability = Object.fromEntries(sources)["src/page_stability.js"];
  assert.match(content, /GridEdgePageStability/);
  assert.match(content, /MAX_STABILITY_ATTEMPTS = 180/);
  assert.match(content, /async function stablePageCapture/);
  assert.match(content, /forbiddenRowsetHash/);
  assert.match(pageStability, /currentRowsetHash !== forbiddenRowsetHash/);
  assert.match(content, /previousPageRowsetHash = page\.rowsetHash/);
  assert.match(content, /previousPageRowsetHash = historyDeadline[\s\S]*?rowsetHash\(current\)/);
  assert.match(content, /if \(expectedPageCount === 1\)/);
  assert.match(content, /async function crawlSessionHistory/);
  assert.match(content, /const pageCaptures = \[\]/);
  assert.match(content, /provider\.assembleSessionHistory\(/);
  const crawl = content.slice(
    content.indexOf("async function crawlSessionHistory"),
    content.indexOf("async function initializeCollector"),
  );
  assert.ok(crawl.indexOf('navigateHistory("首页"') < crawl.indexOf("provider.assembleSessionHistory"));
  assert.ok(crawl.indexOf("provider.assembleSessionHistory") <
    crawl.indexOf("deliverCapture(", crawl.indexOf("provider.assembleSessionHistory")));
  assert.doesNotMatch(crawl.slice(0, crawl.indexOf("provider.assembleSessionHistory")), /deliverCapture\(/);
  // Startup initialization may crawl stable history. Runtime zero-overlap
  // recovery must never guess across rolling pagination: it establishes an
  // explicit same-day discontinuity and waits for a new full local bucket.
  assert.match(content, /crawlSessionHistory,/);
  assert.match(content, /const recoveryDeadline = pageStability\.createDeadline[\s\S]*?GRIDEDGE_GET_CAPTURE_STATE/);
  assert.match(content, /establishDiscontinuityBoundary:[\s\S]*?establishDiscontinuityBoundary\(recoveryDeadline\)/);
  assert.match(content, /GRIDEDGE_DISCONTINUITY_BOUNDARY/);
  assert.match(Object.fromEntries(sources)["src/durable.js"],
    /SAME_DAY_DISCONTINUITY_BOUNDARY_V2/);
  assert.match(content, /async function establishResumeBoundary/);
  assert.match(content, /GRIDEDGE_RESUME_BOUNDARY/);
  assert.match(pageStability, /function createRetriableInitializer/);
  assert.match(content, /createRetriableInitializer\(initializeCollector/);
  assert.match(content, /scheduleRegularRetry\(\(\) => void requestInitialization\(\)\)/);
  assert.ok(content.indexOf("await pageStability.routeCollectorInitialization") <
    content.indexOf("observer.observe"));
});

test("same-day complete or resume state skips redundant pagination but never bypasses the daily boundary", () => {
  const background = Object.fromEntries(sources)["src/background.js"];
  const content = Object.fromEntries(sources)["src/content.js"];
  const pageStability = Object.fromEntries(sources)["src/page_stability.js"];
  assert.match(background, /GRIDEDGE_GET_CAPTURE_STATE/);
  assert.match(background, /GridEdgeDurable\.sourceState/);
  assert.match(content, /GRIDEDGE_GET_CAPTURE_STATE/);
  assert.match(content, /routeCollectorInitialization\(/);
  const initialize = content.slice(
    content.indexOf("async function initializeCollector"),
    content.indexOf("const initializationRunner"),
  );
  assert.ok(initialize.indexOf("atomicReviewedPageCapture") < initialize.indexOf("GRIDEDGE_GET_CAPTURE_STATE"));
  assert.doesNotMatch(
    initialize.slice(0, initialize.indexOf("GRIDEDGE_GET_CAPTURE_STATE")),
    /stablePageCapture/,
  );
  assert.match(initialize, /captureReviewedInitializationPage/);
  assert.match(initialize, /isRefreshableError:\s*isRefreshableInitialError/);
  assert.doesNotMatch(initialize, /captureInitialPageWithRefresh/);
  assert.match(pageStability, /live capture has no overlap with the prior durable watermark/);
  const liveRecovery = pageStability.slice(
    pageStability.indexOf("function shouldRecoverLiveOverlap"),
    pageStability.indexOf("function installSourceHeartbeat"),
  );
  assert.match(liveRecovery, /reason !== "heartbeat"/);
  assert.match(liveRecovery, /===\s*\n\s*"live capture has no overlap with the prior durable watermark"/);
  assert.doesNotMatch(liveRecovery, /\.includes\(/);
  assert.doesNotMatch(liveRecovery, /crawlSessionHistory/);
  assert.match(content, /pageStability\.shouldRecoverLiveOverlap\(reason, error\)/);
  const boundary = content.slice(
    content.indexOf("async function establishResumeBoundary"),
    content.indexOf("async function initializeCollector"),
  );
  assert.match(boundary, /atomicReviewedPageCapture/);
  assert.doesNotMatch(boundary, /stablePageCapture/);
});

test("vendored MQTT library is local and pinned", () => {
  const packageJson = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
  assert.equal(packageJson.dependencies.mqtt, "5.15.2");
  assert.ok(fs.statSync(path.join(root, "vendor", "mqtt.min.js")).size > 100_000);
  assert.ok(fs.readFileSync(path.join(root, "vendor", "MQTT-LICENSE.md"), "utf8").includes("MIT License"));
  assert.match(
    Object.fromEntries(sources)["src/background.js"],
    /importScripts\(\s*"\.\.\/vendor\/mqtt\.min\.js"/,
  );
});
