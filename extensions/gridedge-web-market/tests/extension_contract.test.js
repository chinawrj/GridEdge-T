"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const root = path.join(__dirname, "..");
const manifest = JSON.parse(fs.readFileSync(path.join(root, "manifest.json"), "utf8"));
const sources = [
  "src/background.js",
  "src/content.js",
  "src/durable.js",
  "src/options.js",
  "src/popup.js",
  "src/providers/eastmoney.js",
  "src/shared.js",
].map((name) => [name, fs.readFileSync(path.join(root, name), "utf8")]);

test("extension is self-contained and can only reach Eastmoney plus the reviewed MQTT host", () => {
  assert.equal(manifest.manifest_version, 3);
  assert.deepEqual(manifest.host_permissions, [
    "https://quote.eastmoney.com/*",
    "http://192.168.1.201/*",
  ]);
  assert.match(manifest.content_security_policy.extension_pages, /connect-src ws:\/\/192\.168\.1\.201:9001/);
  assert.equal(JSON.stringify(manifest).includes("127.0.0.1"), false);
  assert.equal(JSON.stringify(manifest).includes("<all_urls>"), false);
  const allSources = sources.map(([, source]) => source).join("\n").toLowerCase();
  assert.equal(allSources.includes("postgres"), false);
  assert.equal(allSources.includes("companion"), false);
  assert.match(allSources, /account_marker is not market data/);
});

test("collector is disabled by default and publishes MQTT 5 QoS 1 only after durable storage", () => {
  const background = Object.fromEntries(sources)["src/background.js"];
  assert.match(background, /enabled:\s*false/);
  assert.match(background, /protocolVersion:\s*5/);
  assert.match(background, /qos:\s*1/);
  assert.match(background, /GridEdgeDurable\.ingestCapture/);
  const delivery = background.slice(
    background.indexOf("async function deliverCapture"),
    background.indexOf("async function scanActiveTab"),
  );
  assert.ok(delivery.indexOf("GridEdgeDurable.ingestCapture") < delivery.indexOf("flushOutbox()"));
  assert.match(background, /GridEdgeDurable\.acknowledge\(database, event\.event_id\)/);
  assert.match(background, /chrome\.alarms\.create\("gridedge-mqtt-outbox"/);
  assert.doesNotMatch(background, /fetch\(/);
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

test("content collector has manual scan and bounded mutation debounce", () => {
  const content = Object.fromEntries(sources)["src/content.js"];
  assert.match(content, /GRIDEDGE_SCAN_NOW/);
  assert.match(content, /setTimeout\(\(\) => void scan\("mutation"\), 3000\)/);
  assert.match(content, /observer\.observe\(document\.documentElement,\s*\{[\s\S]*childList:\s*true,[\s\S]*characterData:\s*true,[\s\S]*subtree:\s*true,[\s\S]*\}\)/);
  assert.match(content, /WAITING_FOR_STABLE_ROWSET/);
  assert.match(content, /setTimeout\(\(\) => void scan\("stability"\), 1000\)/);
  assert.match(content, /captured_at_us: 0/);
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
  assert.match(content, /async function stablePageCapture/);
  assert.match(content, /async function crawlSessionHistory/);
  assert.match(content, /const pageCaptures = \[\]/);
  assert.match(content, /provider\.assembleSessionHistory\(/);
  const crawl = content.slice(
    content.indexOf("async function crawlSessionHistory"),
    content.indexOf("async function initializeCollector"),
  );
  assert.ok(crawl.indexOf('navigateHistory("首页"') < crawl.indexOf("provider.assembleSessionHistory"));
  assert.ok(crawl.indexOf("provider.assembleSessionHistory") < crawl.indexOf("deliverCapture(completeCapture"));
  assert.doesNotMatch(crawl.slice(0, crawl.indexOf("provider.assembleSessionHistory")), /deliverCapture\(/);
  assert.match(content, /await crawlSessionHistory\(\)/);
  assert.ok(content.indexOf("await crawlSessionHistory()") < content.indexOf("observer.observe"));
});

test("completed session state skips redundant pagination but never bypasses the daily session boundary", () => {
  const background = Object.fromEntries(sources)["src/background.js"];
  const content = Object.fromEntries(sources)["src/content.js"];
  assert.match(background, /GRIDEDGE_GET_CAPTURE_STATE/);
  assert.match(background, /GridEdgeDurable\.sourceState/);
  assert.match(content, /GRIDEDGE_GET_CAPTURE_STATE/);
  assert.match(content, /complete_session_date === currentPage\.capture\.session_date/);
  assert.match(content, /live capture has no overlap with the prior durable watermark/);
  assert.match(content, /await crawlSessionHistory\(\)/);
});

test("vendored MQTT library is local and pinned", () => {
  const packageJson = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
  assert.equal(packageJson.dependencies.mqtt, "5.15.2");
  assert.ok(fs.statSync(path.join(root, "vendor", "mqtt.min.js")).size > 100_000);
  assert.ok(fs.readFileSync(path.join(root, "vendor", "MQTT-LICENSE.md"), "utf8").includes("MIT License"));
  assert.match(Object.fromEntries(sources)["src/background.js"], /importScripts\("\.\.\/vendor\/mqtt\.min\.js"/);
});
