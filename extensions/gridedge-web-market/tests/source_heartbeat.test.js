"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const {
  buildSourceHeartbeatAlarmPlan,
  createReviewedTabRecovery,
  createSourceHeartbeatCoordinator,
  dispatchSourceHeartbeat,
  installSourceHeartbeatAlarmWatchdog,
  isReviewedMarketTab,
  reviewedTabRecoveryReason,
} = require("../src/source_heartbeat.js");

test("MV3 watchdog survives loss of any phase inside the 45-second operating target", () => {
  const plan = buildSourceHeartbeatAlarmPlan({ nowMs: 1_000_000 });

  assert.deepEqual(plan, [
    {
      name: "gridedge-source-heartbeat-phase-1",
      options: { when: 1_007_500, periodInMinutes: 0.5 },
    },
    {
      name: "gridedge-source-heartbeat-phase-2",
      options: { when: 1_015_000, periodInMinutes: 0.5 },
    },
    {
      name: "gridedge-source-heartbeat-phase-3",
      options: { when: 1_022_500, periodInMinutes: 0.5 },
    },
    {
      name: "gridedge-source-heartbeat-phase-4",
      options: { when: 1_030_000, periodInMinutes: 0.5 },
    },
  ]);
  assert.equal(new Set(plan.map(({ name }) => name)).size, plan.length);

  const scheduled = Array.from({ length: 3 }, (_, cycle) =>
    plan.map((entry) => entry.options.when + cycle * 30_000)).flat()
    .sort((left, right) => left - right);
  for (let missed = 0; missed < scheduled.length; missed += 1) {
    const withOneMissed = scheduled.filter((_, index) => index !== missed);
    const maximumGap = Math.max(...withOneMissed.slice(1).map(
      (value, index) => value - withOneMissed[index],
    ));
    assert.ok(maximumGap <= 15_000, `missed index ${missed} produced ${maximumGap}ms gap`);
    assert.ok(maximumGap + 12_000 + 15_000 <= 45_000,
      "schedule, reviewed capture deadline, and DB ACK budget must meet the 45-second target");
  }
});

function fakeEvent() {
  const listeners = [];
  return {
    addListener(listener) { listeners.push(listener); },
    emit(value) { for (const listener of listeners) listener(value); },
  };
}

test("MV3 evaluation, install, startup, and every reviewed alarm repair and route the watchdog", async () => {
  const created = [];
  const onAlarm = fakeEvent();
  const onInstalled = fakeEvent();
  const onStartup = fakeEvent();
  let requests = 0;
  const alarms = {
    onAlarm,
    async create(name, options) { created.push([name, options]); },
  };

  const installed = installSourceHeartbeatAlarmWatchdog({
    alarms,
    runtime: { onInstalled, onStartup },
    coordinator: { request() { requests += 1; return Promise.resolve(); } },
    nowMs: 1_000_000,
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(created, installed.alarmPlan.map(({ name, options }) => [name, options]),
    "service-worker evaluation must repair every phase");

  created.length = 0;
  onInstalled.emit();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(created, installed.alarmPlan.map(({ name, options }) => [name, options]),
    "installation/update must repair every phase");

  created.length = 0;
  onStartup.emit();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(created, installed.alarmPlan.map(({ name, options }) => [name, options]),
    "browser startup must repair every phase");
  assert.equal(requests, 1, "startup must request one immediate reviewed observation");

  for (const { name } of installed.alarmPlan) onAlarm.emit({ name });
  assert.equal(requests, 1 + installed.alarmPlan.length,
    "each independently scheduled phase must route to the same coordinator");
  onAlarm.emit({ name: "gridedge-source-heartbeat-unknown" });
  assert.equal(requests, 1 + installed.alarmPlan.length,
    "an unknown alarm must not enter the source-observation coordinator");
});

test("source-heartbeat watchdog targets only reviewed Eastmoney time-sales tabs", async () => {
  const tabs = [
    { id: 7, url: "https://quote.eastmoney.com/f1.html?newcode=0.002256" },
    { id: 8, url: "https://quote.eastmoney.com/sz002256.html" },
    { id: 9, url: "https://evil.example/f1.html?newcode=0.002256" },
    { id: 10, url: "https://quote.eastmoney.com/f1.html?newcode=not-a-symbol" },
  ];
  const sent = [];
  const result = await dispatchSourceHeartbeat({
    queryTabs: async () => tabs,
    sendMessage: async (tabId, message) => sent.push([tabId, message]),
  });

  assert.equal(isReviewedMarketTab(tabs[0]), true);
  assert.equal(isReviewedMarketTab(tabs[1]), false);
  assert.deepEqual(sent, [[7, { type: "GRIDEDGE_SOURCE_HEARTBEAT" }]]);
  assert.deepEqual(result, {
    reviewedTabs: 1,
    delivered: 1,
    recovered: 0,
    failed: 0,
    recoverableTabs: [],
  });
});

test("initialization never enters reviewed tab reload recovery", async () => {
  let nowMs = 1_000_000;
  const reloaded = [];
  const tab = { id: 7, url: "https://quote.eastmoney.com/f1.html?newcode=0.002256" };
  const recovery = createReviewedTabRecovery({
    async getTab() { return tab; },
    async reloadTab(tabId, options) { reloaded.push([tabId, options]); },
    nowMs: () => nowMs,
    cooldownMs: 20_000,
  });
  const response = {
    ok: false,
    reason: "INITIALIZING_HISTORY",
    initialization_error: "capture latest row is stale",
  };

  assert.equal(reviewedTabRecoveryReason(response), null);
  assert.equal(await recovery.request(tab, response), false);
  assert.deepEqual(reloaded, []);

  nowMs += 7_500;
  assert.equal(await recovery.request(tab, response), false,
    "phased alarms must not create a reload storm inside the cooldown");
  assert.deepEqual(reloaded, []);

  nowMs += 20_000;
  assert.equal(await recovery.request(tab, response), false);
  assert.deepEqual(reloaded, []);
  assert.equal(await recovery.request(tab, {
    ok: false,
    reason: "INITIALIZING_HISTORY",
    initialization_error: "account_marker is not market data",
  }), false, "unreviewed failures must never navigate a production tab");
});

test("stale live trade coverage requests one bounded reviewed-tab reload", async () => {
  let nowMs = 1_000_000;
  const reloaded = [];
  const tab = { id: 7, url: "https://quote.eastmoney.com/f1.html?newcode=0.002256" };
  const recovery = createReviewedTabRecovery({
    async getTab() { return tab; },
    async reloadTab(tabId, options) { reloaded.push([tabId, options]); },
    nowMs: () => nowMs,
    cooldownMs: 20_000,
  });
  const response = { ok: false, reason: "STALE_TRADE_COVERAGE" };

  assert.equal(reviewedTabRecoveryReason(response), "STALE_TRADE_COVERAGE");
  assert.equal(await recovery.request(tab, response), true);
  assert.deepEqual(reloaded, [[7, { bypassCache: true }]]);

  nowMs += 7_500;
  assert.equal(await recovery.request(tab, response), false,
    "phased alarms must coalesce stale-coverage recovery inside the cooldown");
  assert.deepEqual(reloaded, [[7, { bypassCache: true }]]);
});

test("reviewed tab recovery classifier is an exact fail-closed allowlist", () => {
  const exact = (initialization_error) => ({
    ok: false,
    reason: "INITIALIZING_HISTORY",
    initialization_error,
  });
  assert.equal(reviewedTabRecoveryReason({
    ok: false,
    reason: "STALE_TRADE_COVERAGE",
  }), "STALE_TRADE_COVERAGE");
  for (const message of [
    "capture latest row is stale",
    "Eastmoney page ? did not become stable",
    "Eastmoney time-sales DOM order disagrees with its reviewed control",
    "Eastmoney time-sales page has no reviewed A-share sale rows",
    "Eastmoney latest-first cycle did not produce a reviewed rowset effect",
  ]) assert.equal(reviewedTabRecoveryReason(exact(message)), null);

  for (const response of [
    null,
    "INITIALIZING_HISTORY",
    {},
    { ok: true, reason: "INITIALIZING_HISTORY", initialization_error: "capture latest row is stale" },
    { ok: false, reason: "initializing_history", initialization_error: "capture latest row is stale" },
    exact(""),
    exact(" capture latest row is stale"),
    exact("capture latest row is stale "),
    exact("CAPTURE LATEST ROW IS STALE"),
    exact("capture latest row is stale: retry"),
    exact("prefix Eastmoney page ? did not become stable suffix"),
    exact("capture latest row is stale; Eastmoney page ? did not become stable"),
    exact("atomic reviewed page contains no reviewed rows"),
  ]) assert.equal(reviewedTabRecoveryReason(response), null, JSON.stringify(response));
});

test("reload revalidates the exact reviewed URL and rejects TOCTOU navigation", async () => {
  const initial = { id: 51, url: "https://quote.eastmoney.com/f1.html?newcode=0.002256" };
  const response = { ok: false, reason: "STALE_TRADE_COVERAGE" };
  for (const navigatedUrl of [
    "https://quote.eastmoney.com/sz002256.html",
    "https://quote.eastmoney.com/f1.html?newcode=0.002256&extra=1",
    "https://quote.eastmoney.com/f1.html?newcode=0.002256#fragment",
    "https://quote.eastmoney.com/f1.html?newcode=1.600000",
  ]) {
    let reloads = 0;
    const recovery = createReviewedTabRecovery({
      async getTab() { return { id: initial.id, url: navigatedUrl }; },
      async reloadTab() { reloads += 1; },
    });
    assert.equal(await recovery.request(initial, response), false, navigatedUrl);
    assert.equal(reloads, 0, navigatedUrl);
  }
});

test("four concurrent alarm phases atomically reserve one reload and failure stays cooled down", async () => {
  let releaseReload;
  let reloads = 0;
  let nowMs = 5_000;
  const tab = { id: 61, url: "https://quote.eastmoney.com/f1.html?newcode=0.002256" };
  const response = { ok: false, reason: "STALE_TRADE_COVERAGE" };
  const recovery = createReviewedTabRecovery({
    async getTab() { return tab; },
    async reloadTab() {
      reloads += 1;
      await new Promise((resolve) => { releaseReload = resolve; });
      throw new Error("reload failed");
    },
    nowMs: () => nowMs,
    cooldownMs: 20_000,
    timeoutMs: 100,
  });
  const requests = Array.from({ length: 4 }, () => recovery.request(tab, response));
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(reloads, 1);
  releaseReload();
  assert.deepEqual(await Promise.all(requests), [false, false, false, false]);
  assert.equal(await recovery.request(tab, response), false,
    "a failed reload must retain its atomic cooldown reservation");
  nowMs += 20_001;
  void recovery.request(tab, response);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(reloads, 2);
  releaseReload();
});

test("reload cooldown survives MV3 worker re-evaluation through session storage", async () => {
  let nowMs = 10_000;
  const sharedCooldowns = new Map();
  let firstReloadRelease;
  let reloads = 0;
  const tab = { id: 81, url: "https://quote.eastmoney.com/f1.html?newcode=0.002256" };
  const otherTab = { id: 82, url: "https://quote.eastmoney.com/f1.html?newcode=1.600000" };
  const response = { ok: false, reason: "STALE_TRADE_COVERAGE" };
  const persistent = {
    async loadCooldown(key) { return sharedCooldowns.get(key) ?? null; },
    async saveCooldown(key, value) { sharedCooldowns.set(key, value); },
  };
  const firstWorker = createReviewedTabRecovery({
    async getTab() { return tab; },
    async reloadTab() {
      reloads += 1;
      await new Promise((resolve) => { firstReloadRelease = resolve; });
    },
    nowMs: () => nowMs,
    cooldownMs: 20_000,
    ...persistent,
  });
  const first = firstWorker.request(tab, response);
  while (firstReloadRelease === undefined) await new Promise((resolve) => setImmediate(resolve));
  assert.equal(sharedCooldowns.size, 1,
    "the durable reservation must precede tabs.get/reload completion");

  const restartedWorker = createReviewedTabRecovery({
    async getTab(tabId) { return tabId === tab.id ? tab : otherTab; },
    async reloadTab() { reloads += 1; },
    nowMs: () => nowMs,
    cooldownMs: 20_000,
    ...persistent,
  });
  assert.equal(await restartedWorker.request(tab, response), false);
  assert.equal(reloads, 1, "a restarted service worker must observe the durable cooldown");
  firstReloadRelease();
  assert.equal(await first, true);

  assert.equal(await restartedWorker.request(otherTab, response), true,
    "a distinct tab and exact URL must have an independent cooldown");
  assert.equal(reloads, 2);

  nowMs += 20_001;
  const thirdWorker = createReviewedTabRecovery({
    async getTab() { return tab; },
    async reloadTab() { reloads += 1; },
    nowMs: () => nowMs,
    cooldownMs: 20_000,
    ...persistent,
  });
  assert.equal(await thirdWorker.request(tab, response), true);
  assert.equal(reloads, 3, "cooldown expiry permits exactly one later reload");
});

test("session-storage failure fails closed before tab inspection or reload", async () => {
  const tab = { id: 91, url: "https://quote.eastmoney.com/f1.html?newcode=0.002256" };
  const response = { ok: false, reason: "STALE_TRADE_COVERAGE" };
  for (const failingOperation of ["load", "save"]) {
    let gets = 0;
    let reloads = 0;
    const recovery = createReviewedTabRecovery({
      async getTab() { gets += 1; return tab; },
      async reloadTab() { reloads += 1; },
      async loadCooldown() {
        if (failingOperation === "load") throw new Error("session storage unavailable");
        return null;
      },
      async saveCooldown() {
        if (failingOperation === "save") throw new Error("session storage unavailable");
      },
    });
    assert.equal(await recovery.request(tab, response), false, failingOperation);
    assert.equal(gets, 0, failingOperation);
    assert.equal(reloads, 0, failingOperation);
  }
});

test("concurrent distinct tabs cannot lose either durable cooldown reservation", async () => {
  const sharedCooldowns = new Map();
  let nowMs = 100_000;
  let loads = 0;
  let releaseLoads;
  const bothLoaded = new Promise((resolve) => { releaseLoads = resolve; });
  const tabs = [
    { id: 101, url: "https://quote.eastmoney.com/f1.html?newcode=0.002256" },
    { id: 102, url: "https://quote.eastmoney.com/f1.html?newcode=1.600000" },
  ];
  const response = { ok: false, reason: "STALE_TRADE_COVERAGE" };
  let reloads = 0;
  const concurrentWorker = createReviewedTabRecovery({
    async getTab(tabId) { return tabs.find((tab) => tab.id === tabId); },
    async reloadTab() { reloads += 1; },
    async loadCooldown(key) {
      const snapshot = sharedCooldowns.get(key) ?? null;
      loads += 1;
      if (loads === 2) releaseLoads();
      await bothLoaded;
      return snapshot;
    },
    async saveCooldown(key, value) { sharedCooldowns.set(key, value); },
    nowMs: () => nowMs,
  });
  assert.deepEqual(await Promise.all(tabs.map((tab) => concurrentWorker.request(tab, response))),
    [true, true]);
  assert.equal(reloads, 2);

  const afterRestart = createReviewedTabRecovery({
    async getTab(tabId) { return tabs.find((tab) => tab.id === tabId); },
    async reloadTab() { reloads += 1; },
    async loadCooldown(key) { return sharedCooldowns.get(key) ?? null; },
    async saveCooldown(key, value) { sharedCooldowns.set(key, value); },
    nowMs: () => nowMs + 1,
  });
  assert.deepEqual(await Promise.all(tabs.map((tab) => afterRestart.request(tab, response))),
    [false, false], "both independent cooldowns must survive a worker restart");
  assert.equal(reloads, 2);
  nowMs += 20_001;
  const afterExpiry = createReviewedTabRecovery({
    async getTab(tabId) { return tabs.find((tab) => tab.id === tabId); },
    async reloadTab() { reloads += 1; },
    async loadCooldown(key) { return sharedCooldowns.get(key) ?? null; },
    async saveCooldown(key, value) { sharedCooldowns.set(key, value); },
    nowMs: () => nowMs,
  });
  assert.deepEqual(await Promise.all(tabs.map((tab) => afterExpiry.request(tab, response))),
    [true, true]);
  assert.equal(reloads, 4, "each independent cooldown expires exactly once");
});

test("a hanging recovery is bounded and does not suppress another reviewed tab", async () => {
  const tabs = [
    { id: 71, url: "https://quote.eastmoney.com/f1.html?newcode=0.002256" },
    { id: 72, url: "https://quote.eastmoney.com/f1.html?newcode=1.600000" },
  ];
  const response = { ok: false, reason: "STALE_TRADE_COVERAGE" };
  let secondReloaded = false;
  const recovery = createReviewedTabRecovery({
    async getTab(tabId) { return tabs.find((tab) => tab.id === tabId); },
    async reloadTab(tabId) {
      if (tabId === 71) return await new Promise(() => {});
      secondReloaded = true;
    },
    timeoutMs: 15,
  });
  const dispatched = dispatchSourceHeartbeat({
    queryTabs: async () => tabs,
    sendMessage: async () => response,
    recoverTab: (tab, received) => recovery.request(tab, received),
    timeoutMs: 25,
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(secondReloaded, true);
  assert.deepEqual(await dispatched, {
    reviewedTabs: 2,
    delivered: 2,
    recovered: 1,
    failed: 1,
    recoverableTabs: [71, 72],
  });
});

test("reviewed URL contract rejects every unreviewed URL component", () => {
  const rejected = [
    "https://quote.eastmoney.com/f1.html?newcode=0.002256&extra=1",
    "https://quote.eastmoney.com/f1.html?newcode=0.002256&newcode=0.002256",
    "https://quote.eastmoney.com/f1.html?newcode=0.002256#fragment",
    "https://user:secret@quote.eastmoney.com/f1.html?newcode=0.002256",
    "https://quote.eastmoney.com:444/f1.html?newcode=0.002256",
  ];
  for (const url of rejected) {
    assert.equal(isReviewedMarketTab({ id: 31, url }), false, url);
  }
});

test("one transient receiver failure cannot suppress another reviewed tab", async () => {
  const sent = [];
  const result = await dispatchSourceHeartbeat({
    queryTabs: async () => [
      { id: 21, url: "https://quote.eastmoney.com/f1.html?newcode=0.002256" },
      { id: 22, url: "https://quote.eastmoney.com/f1.html?newcode=1.600000" },
    ],
    sendMessage: async (tabId) => {
      sent.push(tabId);
      if (tabId === 21) throw new Error("content script is reloading");
    },
  });

  assert.deepEqual(sent, [21, 22]);
  assert.deepEqual(result, {
    reviewedTabs: 2, delivered: 1, recovered: 0, failed: 1, recoverableTabs: [],
  });
});

test("one hanging receiver times out without blocking another reviewed tab", async () => {
  let secondDelivered = false;
  const dispatched = dispatchSourceHeartbeat({
    queryTabs: async () => [
      { id: 41, url: "https://quote.eastmoney.com/f1.html?newcode=0.002256" },
      { id: 42, url: "https://quote.eastmoney.com/f1.html?newcode=1.600000" },
    ],
    sendMessage: async (tabId) => {
      if (tabId === 41) return await new Promise(() => {});
      secondDelivered = true;
    },
    timeoutMs: 15,
  });

  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(secondDelivered, true);
  assert.deepEqual(await dispatched, {
    reviewedTabs: 2, delivered: 1, recovered: 0, failed: 1, recoverableTabs: [],
  });
});

test("tab enumeration failure propagates and enumeration timeout is bounded", async () => {
  await assert.rejects(
    dispatchSourceHeartbeat({
      queryTabs: async () => { throw new Error("tabs unavailable"); },
      sendMessage: async () => {},
      timeoutMs: 15,
    }),
    /tabs unavailable/,
  );
  await assert.rejects(
    dispatchSourceHeartbeat({
      queryTabs: async () => await new Promise(() => {}),
      sendMessage: async () => {},
      timeoutMs: 15,
    }),
    /timed out/,
  );
});

test("overlapping watchdog requests share one run and at most one trailing run", async () => {
  let releaseFirst;
  let calls = 0;
  const coordinator = createSourceHeartbeatCoordinator(async () => {
    calls += 1;
    if (calls === 1) {
      await new Promise((resolve) => { releaseFirst = resolve; });
    }
    return calls;
  });

  const first = coordinator.request();
  coordinator.request();
  coordinator.request();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(calls, 1);
  releaseFirst();
  assert.equal(await first, 2);
  assert.equal(calls, 2);
});

test("a trailing watchdog request still runs after the in-flight request fails", async () => {
  let releaseFirst;
  let calls = 0;
  const coordinator = createSourceHeartbeatCoordinator(async () => {
    calls += 1;
    if (calls === 1) {
      await new Promise((resolve) => { releaseFirst = resolve; });
      throw new Error("tab enumeration failed");
    }
    return "recovered";
  });

  const first = coordinator.request();
  coordinator.request();
  releaseFirst();
  assert.equal(await first, "recovered");
  assert.equal(calls, 2);
});
