(function installGridEdgeSourceHeartbeat(root, factory) {
  const api = factory();
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  root.GridEdgeSourceHeartbeat = api;
})(typeof globalThis === "undefined" ? this : globalThis, function sourceHeartbeatFactory() {
  "use strict";

  const MESSAGE = Object.freeze({ type: "GRIDEDGE_SOURCE_HEARTBEAT" });
  const REVIEWED_URL = /^https:\/\/quote\.eastmoney\.com\/f1\.html\?newcode=[01]\.\d{6}$/;
  const HEARTBEAT_ALARM_PERIOD_MINUTES = 0.5;
  const HEARTBEAT_ALARM_PHASE_MS = Object.freeze([7_500, 15_000, 22_500, 30_000]);
  function buildSourceHeartbeatAlarmPlan({ nowMs = Date.now() } = {}) {
    if (!Number.isSafeInteger(nowMs) || nowMs < 0) {
      throw new Error("source heartbeat alarm clock must be a non-negative safe integer");
    }
    return HEARTBEAT_ALARM_PHASE_MS.map((delayMs, index) => ({
      name: `gridedge-source-heartbeat-phase-${index + 1}`,
      options: {
        when: nowMs + delayMs,
        periodInMinutes: HEARTBEAT_ALARM_PERIOD_MINUTES,
      },
    }));
  }

  function isReviewedMarketTab(tab) {
    if (!Number.isSafeInteger(tab?.id)) return false;
    return REVIEWED_URL.test(tab.url ?? "");
  }

  async function withTimeout(task, timeoutMs, label) {
    let timeoutId;
    const timeout = new Promise((_, reject) => {
      timeoutId = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
    });
    try {
      return await Promise.race([Promise.resolve().then(task), timeout]);
    } finally {
      clearTimeout(timeoutId);
    }
  }

  function reviewedTabRecoveryReason(response) {
    if (response === null || typeof response !== "object" || response.ok !== false) {
      return null;
    }
    return response.reason === "STALE_TRADE_COVERAGE" ? response.reason : null;
  }

  function createReviewedTabRecovery({
    getTab,
    reloadTab,
    loadCooldown = null,
    saveCooldown = null,
    nowMs = () => Date.now(),
    cooldownMs = 20_000,
    timeoutMs = 10_000,
  }) {
    if (typeof getTab !== "function" || typeof reloadTab !== "function" ||
        !Number.isSafeInteger(cooldownMs) || cooldownMs <= 0 ||
        !Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 10_000) {
      throw new Error("reviewed tab recovery requires bounded tab operations and cooldown");
    }
    const reservedAt = new Map();
    const inFlight = new Map();
    const fallbackCooldowns = new Map();
    const load = loadCooldown ?? (async (key) => fallbackCooldowns.get(key) ?? null);
    const save = saveCooldown ?? (async (key, value) => { fallbackCooldowns.set(key, value); });

    async function request(tab, response) {
      if (!isReviewedMarketTab(tab) || reviewedTabRecoveryReason(response) === null) return false;
      const key = `gridedge-reviewed-tab-reload-v1:${tab.id}:${encodeURIComponent(tab.url)}`;
      const now = nowMs();
      if (inFlight.has(key) || now - (reservedAt.get(key) ?? Number.NEGATIVE_INFINITY) < cooldownMs) {
        return false;
      }
      const operation = (async () => {
        try {
          const durableReservedAt = await withTimeout(
            () => load(key), timeoutMs, "reviewed tab reload cooldown load",
          );
          if (durableReservedAt !== null && !Number.isSafeInteger(durableReservedAt)) return false;
          if (Number.isSafeInteger(durableReservedAt) &&
              (now < durableReservedAt || now - durableReservedAt < cooldownMs)) {
            reservedAt.set(key, durableReservedAt);
            return false;
          }
          // Persist before any tab API call. This is the durable atomic
          // reservation observed after an MV3 service-worker re-evaluation.
          await withTimeout(
            () => save(key, now), timeoutMs, "reviewed tab reload cooldown save",
          );
          reservedAt.set(key, now);
          const current = await withTimeout(
            () => getTab(tab.id), timeoutMs, `reviewed tab ${tab.id} revalidation`,
          );
          if (!isReviewedMarketTab(current) || current.url !== tab.url) return false;
          await withTimeout(
            () => reloadTab(tab.id, { bypassCache: true }),
            timeoutMs,
            `reviewed tab ${tab.id} reload`,
          );
          return true;
        } catch (_error) {
          return false;
        }
      })();
      inFlight.set(key, operation);
      try {
        return await operation;
      } finally {
        if (inFlight.get(key) === operation) inFlight.delete(key);
      }
    }
    return { request };
  }

  async function dispatchSourceHeartbeat({
    queryTabs,
    sendMessage,
    recoverTab = null,
    timeoutMs = 10_000,
  }) {
    const queried = await withTimeout(queryTabs, timeoutMs, "reviewed tab query");
    const tabs = queried.filter(isReviewedMarketTab);
    const outcomes = await Promise.all(tabs.map(async (tab) => {
      try {
        const response = await withTimeout(
          () => sendMessage(tab.id, MESSAGE),
          timeoutMs,
          `source heartbeat to tab ${tab.id}`,
        );
        const recoverable = reviewedTabRecoveryReason(response) !== null;
        let recovered = false;
        let recoveryFailed = false;
        if (recoverable && typeof recoverTab === "function") {
          recovered = await recoverTab(tab, response);
          recoveryFailed = !recovered;
        }
        return { delivered: true, recoverable, recovered, recoveryFailed, tabId: tab.id };
      } catch (_error) {
        // A reviewed tab can be between navigation commit and content-script
        // installation. Do not let that transient suppress another tab.
        return { delivered: false, recoverable: false, recovered: false,
          recoveryFailed: false, tabId: tab.id };
      }
    }));
    const delivered = outcomes.filter((outcome) => outcome.delivered).length;
    const sendFailed = outcomes.length - delivered;
    const recoveryFailed = outcomes.filter((outcome) => outcome.recoveryFailed).length;
    return {
      reviewedTabs: tabs.length,
      delivered,
      recovered: outcomes.filter((outcome) => outcome.recovered).length,
      failed: sendFailed + recoveryFailed,
      recoverableTabs: outcomes.filter((outcome) => outcome.recoverable)
        .map((outcome) => outcome.tabId),
    };
  }

  function createSourceHeartbeatCoordinator(run) {
    let inFlight = null;
    let trailingRequested = false;

    function request() {
      trailingRequested = true;
      if (inFlight) return inFlight;
      inFlight = (async () => {
        let result;
        let lastError = null;
        do {
          trailingRequested = false;
          try {
            result = await run();
            lastError = null;
          } catch (error) {
            lastError = error;
          }
        } while (trailingRequested);
        if (lastError) throw lastError;
        return result;
      })().finally(() => { inFlight = null; });
      return inFlight;
    }

    return { request };
  }

  function installSourceHeartbeatAlarmWatchdog({ alarms, runtime, coordinator, nowMs = Date.now() }) {
    const alarmPlan = buildSourceHeartbeatAlarmPlan({ nowMs });
    const alarmNames = new Set(alarmPlan.map(({ name }) => name));

    async function ensureAlarms() {
      for (const { name, options } of alarmPlan) await alarms.create(name, options);
    }

    runtime.onInstalled.addListener(() => { void ensureAlarms(); });
    runtime.onStartup.addListener(() => {
      void ensureAlarms();
      void coordinator.request();
    });
    alarms.onAlarm.addListener((alarm) => {
      if (alarmNames.has(alarm?.name)) void coordinator.request();
    });
    // MV3 alarms can disappear across a browser or service-worker lifecycle.
    // Evaluation itself is therefore a repair edge, not just installation.
    void ensureAlarms();

    return { alarmNames, alarmPlan, ensureAlarms };
  }

  return {
    buildSourceHeartbeatAlarmPlan,
    createReviewedTabRecovery,
    createSourceHeartbeatCoordinator,
    dispatchSourceHeartbeat,
    installSourceHeartbeatAlarmWatchdog,
    isReviewedMarketTab,
    reviewedTabRecoveryReason,
  };
});
