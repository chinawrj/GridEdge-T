"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  captureStablePage,
  captureAtomicReviewedPage,
  captureInitialPageWithRefresh,
  captureReviewedInitializationPage,
  captureClockBoundSourceObservation,
  captureServerClockBoundSourceObservation,
  captureSourceObservation,
  completeProvisionalInitialization,
  createRetriableInitializer,
  createInitializationErrorTracker,
  createDeadline,
  createCooldownRetryScheduler,
  createReviewedControlRecoveryCoordinator,
  createLaneFailureBudget,
  createUiMutationGuard,
  createIndependentHeartbeatRouter,
  createSingleFlightRunner,
  deliverStatusOnlyObservationAndClassifyTradeCoverage,
  hasCurrentSessionEvidence,
  historyErrorAllowsResumeBoundary,
  routeCollectorInitialization,
  cycleLatestFirstControl,
  installSourceHeartbeat,
  mergeRollingPageCaptures,
  readCaptureWithRetry,
  recoverLiveOverlap,
  recoverReviewedControlMismatch,
  REVIEWED_CONTROL_RECOVERY_BUDGET_MS,
  reviewedControlRecoveryWorstCaseMs,
  reviewedControlIsUnchanged,
  refreshStaleFirstPage,
  shouldRecoverLiveOverlap,
  shouldDeliverCapture,
  waitForReviewedRowsetEffect,
} = require("../src/page_stability.js");

test("heartbeat commits status-only evidence before surfacing stale trade coverage", async () => {
  const calls = [];
  const capture = { marker: "reviewed-atomic" };
  const stale = await deliverStatusOnlyObservationAndClassifyTradeCoverage({
    capture,
    async deliver() {
      calls.push("deliver-status-only");
      return { ok: true, stored: { accepted: 0, source_observations: 1 } };
    },
    validateTradeCoverage(received) {
      calls.push("validate-trade-coverage");
      assert.equal(received, capture);
      throw new Error("capture latest row is stale");
    },
  });
  assert.deepEqual(calls, ["deliver-status-only", "validate-trade-coverage"]);
  assert.deepEqual(stale, { ok: false, reason: "STALE_TRADE_COVERAGE" });

  const fresh = await deliverStatusOnlyObservationAndClassifyTradeCoverage({
    capture,
    async deliver() { return { ok: true, stored: { accepted: 0 } }; },
    validateTradeCoverage() {},
  });
  assert.deepEqual(fresh, { ok: true, stored: { accepted: 0 } });

  let validatedAfterNonOk = false;
  const disabled = await deliverStatusOnlyObservationAndClassifyTradeCoverage({
    capture,
    async deliver() { return { ok: false, reason: "COLLECTOR_DISABLED" }; },
    validateTradeCoverage() { validatedAfterNonOk = true; },
  });
  assert.deepEqual(disabled, { ok: false, reason: "COLLECTOR_DISABLED" });
  assert.equal(validatedAfterNonOk, false,
    "a non-ok delivery must remain non-recoverable and preserve its exact reason");

  let validatedAfterFailure = false;
  await assert.rejects(
    deliverStatusOnlyObservationAndClassifyTradeCoverage({
      capture,
      async deliver() { throw new Error("unseen or conflicting trade"); },
      validateTradeCoverage() { validatedAfterFailure = true; },
    }),
    /unseen or conflicting trade/,
  );
  assert.equal(validatedAfterFailure, false,
    "delivery rejection must never be transformed into a reload request");
});

test("refreshable initialization errors recover in-place through one atomic retry", async () => {
  for (const message of [
    "capture latest row is stale",
    "Eastmoney page ? did not become stable",
    "Eastmoney time-sales DOM order disagrees with its reviewed control",
  ]) {
    const calls = [];
    let captures = 0;
    const deadline = {
      throwIfExpired() { calls.push("deadline"); },
      async run(operation) {
        calls.push("budget");
        return await operation();
      },
    };
    const recovered = await captureReviewedInitializationPage({
      async captureAtomicPage() {
        captures += 1;
        calls.push(`atomic:${captures}`);
        if (captures === 1) throw new Error(message);
        return { capture: { marker: "reviewed" }, rowsetHash: "atomic-hash" };
      },
      async refreshLatestFirst(receivedDeadline) {
        assert.equal(receivedDeadline, deadline);
        calls.push("refresh");
      },
      isRefreshableError(error) { return error.message === message; },
      validateCaptureTiming(capture) {
        assert.deepEqual(capture, { marker: "reviewed" });
        calls.push("timing");
      },
      deadline,
    });
    assert.equal(recovered.rowsetHash, "atomic-hash");
    assert.equal(captures, 2, "recovery must use exactly two atomic snapshots");
    assert.equal(calls.filter((value) => value === "refresh").length, 1);
    assert.equal(calls.filter((value) => value === "timing").length, 1);
    assert.equal(calls.some((value) => /stable|history|navigate|reload/.test(value)), false);
  }
});

test("initialization alone maps an atomic empty rowset into one reviewed in-place recovery", async () => {
  let reads = 0;
  let refreshes = 0;
  const result = await captureReviewedInitializationPage({
    captureAtomicPage: async () => await captureAtomicReviewedPage({
      readCapture: async () => {
        reads += 1;
        return {
          rows: reads === 1 ? [] : [{ source_row_key: "reviewed" }],
          completeness: { page_index: 1, page_count: 1 },
        };
      },
      stableCaptureHash: async () => "capture-a",
      rowsetHash: async () => "rowset-a",
      expectedPageIndex: 1,
    }),
    async refreshLatestFirst() { refreshes += 1; },
    isRefreshableError(error) {
      return error.message === "Eastmoney page ? did not become stable";
    },
    validateCaptureTiming() {},
    deadline: {
      throwIfExpired() {},
      async run(operation) { return await operation(); },
    },
  });
  assert.equal(reads, 2);
  assert.equal(refreshes, 1);
  assert.equal(result.rowsetHash, "rowset-a");
});

test("initialization maps a second atomic empty rowset without changing global atomic semantics", async () => {
  let refreshes = 0;
  await assert.rejects(captureReviewedInitializationPage({
    captureAtomicPage: async () => await captureAtomicReviewedPage({
      readCapture: async () => ({ rows: [], completeness: { page_index: 1, page_count: 1 } }),
      stableCaptureHash: async () => "capture-a",
      rowsetHash: async () => "rowset-a",
      expectedPageIndex: 1,
    }),
    async refreshLatestFirst() { refreshes += 1; },
    isRefreshableError(error) {
      return error.message === "Eastmoney page ? did not become stable";
    },
    validateCaptureTiming() { assert.fail("empty evidence must not validate"); },
    deadline: {
      throwIfExpired() {},
      async run(operation) { return await operation(); },
    },
  }), /Eastmoney page \? did not become stable/);
  assert.equal(refreshes, 1);
});

test("initialization recovery leaves unreviewed failures fatal and side-effect free", async () => {
  for (const message of [
    "prefix capture latest row is stale",
    "CAPTURE LATEST ROW IS STALE",
    "capture latest row is stale; Eastmoney page ? did not become stable",
    "reviewed account identity changed",
    "source state does not exist",
    "atomic reviewed page reused the forbidden rowset",
  ]) {
    let refreshes = 0;
    let validations = 0;
    await assert.rejects(captureReviewedInitializationPage({
      async captureAtomicPage() { throw new Error(message); },
      async refreshLatestFirst() { refreshes += 1; },
      isRefreshableError(error) {
        return [
          "capture latest row is stale",
          "Eastmoney page ? did not become stable",
          "Eastmoney time-sales DOM order disagrees with its reviewed control",
        ].includes(error.message);
      },
      validateCaptureTiming() { validations += 1; },
      deadline: {
        throwIfExpired() {},
        async run(operation) { return await operation(); },
      },
    }), new RegExp(message.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
    assert.equal(refreshes, 0, message);
    assert.equal(validations, 0, message);
  }
});

test("initialization recovery shares one deadline and publishes no partial success", async () => {
  let calls = 0;
  let refreshes = 0;
  let validations = 0;
  const deadline = {
    throwIfExpired() {},
    async run(operation) {
      calls += 1;
      if (calls === 3) throw new Error("initialization recovery exceeded its reviewed deadline");
      return await operation();
    },
  };
  let attempts = 0;
  await assert.rejects(captureReviewedInitializationPage({
    async captureAtomicPage() {
      attempts += 1;
      if (attempts === 1) throw new Error("capture latest row is stale");
      return { capture: { marker: "must-not-validate" }, rowsetHash: "late" };
    },
    async refreshLatestFirst(receivedDeadline) {
      assert.equal(receivedDeadline, deadline);
      refreshes += 1;
    },
    isRefreshableError(error) { return error.message === "capture latest row is stale"; },
    validateCaptureTiming() { validations += 1; },
    deadline,
  }), /exceeded its reviewed deadline/);
  assert.equal(refreshes, 1);
  assert.equal(validations, 0);
});

test("a regular lane repairs one exact reviewed-control mismatch without reloading the page", async () => {
  const calls = [];
  const recovered = await recoverReviewedControlMismatch({
    error: new Error("Eastmoney time-sales DOM order disagrees with its reviewed control"),
    isRecoverableError(error) {
      return error.message === "Eastmoney time-sales DOM order disagrees with its reviewed control";
    },
    async refreshLatestFirst(receivedDeadline) {
      calls.push(["refresh", receivedDeadline]);
    },
    async captureAtomicPage() {
      calls.push(["atomic"]);
      return { capture: { marker: "current" }, rowsetHash: "current" };
    },
    validateCaptureTiming(capture) {
      assert.deepEqual(capture, { marker: "current" });
      calls.push(["timing"]);
    },
    deadline: {
      throwIfExpired() { calls.push(["deadline"]); },
      async run(operation) { calls.push(["budget"]); return await operation(); },
    },
  });
  assert.equal(recovered.rowsetHash, "current");
  assert.equal(calls.filter(([name]) => name === "refresh").length, 1);
  assert.equal(calls.filter(([name]) => name === "atomic").length, 1);
  assert.equal(calls.filter(([name]) => name === "timing").length, 1);
  assert.equal(calls.some(([name]) => name === "reload"), false);
});

test("runtime reviewed-control recovery is exact, bounded, and fatal errors stay side-effect free", async () => {
  for (const message of [
    "prefix Eastmoney time-sales DOM order disagrees with its reviewed control",
    "Eastmoney time-sales DOM order disagrees with its reviewed control suffix",
    "Eastmoney instrument identity changed",
    "durable conflict",
  ]) {
    let refreshes = 0;
    await assert.rejects(recoverReviewedControlMismatch({
      error: new Error(message),
      isRecoverableError(error) {
        return error.message === "Eastmoney time-sales DOM order disagrees with its reviewed control";
      },
      async refreshLatestFirst() { refreshes += 1; },
      async captureAtomicPage() { assert.fail("fatal error must not recapture"); },
      validateCaptureTiming() { assert.fail("fatal error must not validate"); },
      deadline: { throwIfExpired() {}, async run(operation) { return await operation(); } },
    }), new RegExp(message));
    assert.equal(refreshes, 0);
  }
  let validations = 0;
  let budgetCalls = 0;
  await assert.rejects(recoverReviewedControlMismatch({
    error: new Error("Eastmoney time-sales DOM order disagrees with its reviewed control"),
    isRecoverableError() { return true; },
    async refreshLatestFirst() {},
    async captureAtomicPage() { return { capture: { marker: "late" } }; },
    validateCaptureTiming() { validations += 1; },
    deadline: {
      throwIfExpired() {},
      async run(operation) {
        budgetCalls += 1;
        if (budgetCalls === 2) {
          throw new Error("runtime recovery exceeded its reviewed deadline");
        }
        return await operation();
      },
    },
  }), /exceeded its reviewed deadline/);
  assert.equal(validations, 0);
});

test("reviewed-control recovery coordinator exact-matches and coalesces queued heartbeats", async () => {
  const scheduled = [];
  let recoveries = 0;
  let immediateHeartbeats = 0;
  let coordinator;
  coordinator = createReviewedControlRecoveryCoordinator({
    async requestRecovery() {
      recoveries += 1;
      assert.equal(coordinator.beginRecovery(), true);
      assert.equal(coordinator.completeRecovery(true), true);
      return { ok: true, reason: "REVIEWED_CONTROL_RECOVERED" };
    },
    async requestImmediateHeartbeat() { immediateHeartbeats += 1; },
    scheduleTask(callback) { scheduled.push(callback); },
  });
  const exact = {
    ok: false,
    reason: "Eastmoney time-sales DOM order disagrees with its reviewed control",
  };
  coordinator.observeHeartbeatResult(exact);
  coordinator.observeHeartbeatResult(exact);
  coordinator.observeHeartbeatResult(exact);
  for (const reason of [
    "prefix Eastmoney time-sales DOM order disagrees with its reviewed control",
    "Eastmoney time-sales DOM order disagrees with its reviewed control suffix",
    "eastmoney time-sales dom order disagrees with its reviewed control",
    "Eastmoney instrument identity changed",
    "durable conflict",
    "atomic reviewed page contains no reviewed rows",
    "capture latest row is stale",
  ]) coordinator.observeHeartbeatResult({ ok: false, reason });
  assert.equal(scheduled.length, 1);
  scheduled.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(recoveries, 1);
  assert.equal(immediateHeartbeats, 1);
  assert.deepEqual(coordinator.snapshot(), {
    queued: false, running: false, scheduled: false, started: false,
    cancelBeforeStart: false, recoverySucceeded: false,
  });
});

test("a heartbeat success cancels a queued but not-started reviewed-control recovery", async () => {
  const scheduled = [];
  let recoveries = 0;
  const coordinator = createReviewedControlRecoveryCoordinator({
    async requestRecovery() { recoveries += 1; return { ok: true }; },
    async requestImmediateHeartbeat() {},
    scheduleTask(callback) { scheduled.push(callback); },
  });
  coordinator.observeHeartbeatResult({
    ok: false,
    reason: "Eastmoney time-sales DOM order disagrees with its reviewed control",
  });
  coordinator.observeHeartbeatResult({ ok: true });
  scheduled.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(recoveries, 0);
});

test("reviewed-control recovery running latch coalesces storms", async () => {
  const scheduled = [];
  let recoveries = 0;
  let releaseRecovery;
  let coordinator;
  coordinator = createReviewedControlRecoveryCoordinator({
    async requestRecovery() {
      recoveries += 1;
      assert.equal(coordinator.beginRecovery(), true);
      await new Promise((resolve) => { releaseRecovery = resolve; });
      assert.equal(coordinator.completeRecovery(true), true);
      return { ok: true };
    },
    async requestImmediateHeartbeat() {},
    scheduleTask(callback) { scheduled.push(callback); },
  });
  const exact = {
    ok: false,
    reason: "Eastmoney time-sales DOM order disagrees with its reviewed control",
  };
  coordinator.observeHeartbeatResult(exact);
  scheduled.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(recoveries, 1);
  coordinator.observeHeartbeatResult(exact);
  coordinator.observeHeartbeatResult(exact);
  assert.equal(scheduled.length, 0);
  releaseRecovery();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(coordinator.snapshot(), {
    queued: false, running: false, scheduled: false, started: false,
    cancelBeforeStart: false, recoverySucceeded: false,
  });
});

test("failed reviewed-control recovery releases its latch and a later heartbeat can retry", async () => {
  const scheduled = [];
  let attempts = 0;
  let coordinator;
  coordinator = createReviewedControlRecoveryCoordinator({
    async requestRecovery() {
      attempts += 1;
      assert.equal(coordinator.beginRecovery(), true);
      if (attempts === 1) {
        assert.equal(coordinator.completeRecovery(false), true);
        throw new Error("bounded recovery failed");
      }
      assert.equal(coordinator.completeRecovery(true), true);
      return { ok: true };
    },
    async requestImmediateHeartbeat() {},
    scheduleTask(callback) { scheduled.push(callback); },
  });
  const exact = {
    ok: false,
    reason: "Eastmoney time-sales DOM order disagrees with its reviewed control",
  };
  coordinator.observeHeartbeatResult(exact);
  scheduled.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(coordinator.snapshot().running, false);
  coordinator.observeHeartbeatResult(exact);
  scheduled.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(attempts, 2);
});

test("busy regular lane preserves recovery priority and immediately heartbeats only after idle", async () => {
  const order = [];
  let releaseInitial;
  let heartbeatAttempts = 0;
  let refreshes = 0;
  let coordinator;
  const router = createIndependentHeartbeatRouter(async (reason) => {
    order.push(`start:${reason}`);
    if (reason === "initial") {
      await new Promise((resolve) => { releaseInitial = resolve; });
    } else if (reason === "heartbeat") {
      heartbeatAttempts += 1;
      return heartbeatAttempts === 1
        ? { ok: false, reason: "Eastmoney time-sales DOM order disagrees with its reviewed control" }
        : { ok: true, committed: true };
    } else if (reason === "reviewed-control-recovery") {
      assert.equal(coordinator.beginRecovery(), true);
      refreshes += 1;
      order.push("atomic:reviewed-control-recovery");
      assert.equal(coordinator.completeRecovery(true), true);
      return { ok: true, reason: "REVIEWED_CONTROL_RECOVERED" };
    }
    return { ok: true, reason };
  }, {
    mergeReason(queued, incoming) {
      if (queued === "reviewed-control-recovery" ||
          incoming === "reviewed-control-recovery") return "reviewed-control-recovery";
      if (queued === "manual" || incoming === "manual") return "manual";
      return incoming;
    },
  });
  coordinator = createReviewedControlRecoveryCoordinator({
    requestRecovery: () => router.request("reviewed-control-recovery"),
    requestImmediateHeartbeat: () => {
      assert.equal(router.regularRunning(), false,
        "immediate heartbeat must start only after the recovery Promise is fully idle");
      order.push("request:immediate-heartbeat");
      return router.request("heartbeat");
    },
  });
  async function externalHeartbeat() {
    const result = await router.request("heartbeat");
    coordinator.observeHeartbeatResult(result);
    return result;
  }

  const initial = router.request("initial");
  await new Promise((resolve) => setImmediate(resolve));
  await externalHeartbeat();
  await new Promise((resolve) => setImmediate(resolve));
  void router.request("manual");
  void router.request("mutation");
  releaseInitial();
  await initial;
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(refreshes, 1);
  assert.equal(heartbeatAttempts, 2);
  assert.deepEqual(order, [
    "start:initial",
    "start:heartbeat",
    "start:reviewed-control-recovery",
    "atomic:reviewed-control-recovery",
    "request:immediate-heartbeat",
    "start:heartbeat",
  ]);
});

test("a successful heartbeat cancels a recovery queued behind a busy regular lane", async () => {
  let releaseInitial;
  let heartbeatAttempts = 0;
  let refreshes = 0;
  let coordinator;
  const router = createIndependentHeartbeatRouter(async (reason) => {
    if (reason === "initial") {
      await new Promise((resolve) => { releaseInitial = resolve; });
      return { ok: true };
    }
    if (reason === "heartbeat") {
      heartbeatAttempts += 1;
      return heartbeatAttempts === 1
        ? { ok: false, reason: "Eastmoney time-sales DOM order disagrees with its reviewed control" }
        : { ok: true, committed: true };
    }
    if (reason === "reviewed-control-recovery") {
      if (!coordinator.beginRecovery()) {
        return { ok: false, reason: "REVIEWED_CONTROL_RECOVERY_CANCELLED" };
      }
      refreshes += 1;
      assert.equal(coordinator.completeRecovery(true), true);
      return { ok: true };
    }
    return { ok: true };
  }, {
    mergeReason: (queued, incoming) =>
      queued === "reviewed-control-recovery" || incoming === "reviewed-control-recovery"
        ? "reviewed-control-recovery" : incoming,
  });
  coordinator = createReviewedControlRecoveryCoordinator({
    requestRecovery: () => router.request("reviewed-control-recovery"),
    requestImmediateHeartbeat: () => router.request("heartbeat"),
  });
  async function externalHeartbeat() {
    const result = await router.request("heartbeat");
    coordinator.observeHeartbeatResult(result);
  }
  const initial = router.request("initial");
  await new Promise((resolve) => setImmediate(resolve));
  await externalHeartbeat();
  await new Promise((resolve) => setImmediate(resolve));
  await externalHeartbeat();
  releaseInitial();
  await initial;
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(refreshes, 0);
  assert.equal(heartbeatAttempts, 2);
});

test("dedicated recovery success survives a failing trailing regular result", async () => {
  let releaseRecovery;
  let markRecoveryStarted;
  const recoveryStarted = new Promise((resolve) => { markRecoveryStarted = resolve; });
  let immediateHeartbeats = 0;
  let refreshes = 0;
  let coordinator;
  const router = createIndependentHeartbeatRouter(async (reason) => {
    if (reason === "reviewed-control-recovery") {
      assert.equal(coordinator.beginRecovery(), true);
      refreshes += 1;
      markRecoveryStarted();
      await new Promise((resolve) => { releaseRecovery = resolve; });
      assert.equal(coordinator.completeRecovery(true), true);
      return { ok: true, reason: "REVIEWED_CONTROL_RECOVERED" };
    }
    if (reason === "mutation") return { ok: false, reason: "TRAILING_FAILED" };
    return { ok: true };
  });
  coordinator = createReviewedControlRecoveryCoordinator({
    requestRecovery: () => router.request("reviewed-control-recovery"),
    async requestImmediateHeartbeat() {
      assert.equal(router.regularRunning(), false);
      immediateHeartbeats += 1;
    },
  });
  coordinator.observeHeartbeatResult({
    ok: false,
    reason: "Eastmoney time-sales DOM order disagrees with its reviewed control",
  });
  await recoveryStarted;
  void router.request("mutation");
  releaseRecovery();
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(refreshes, 1);
  assert.equal(immediateHeartbeats, 1,
    "trailing failure must not erase the dedicated recovery success token");
});

test("dedicated recovery failure cannot borrow a successful trailing regular result", async () => {
  let releaseRecovery;
  let markRecoveryStarted;
  const recoveryStarted = new Promise((resolve) => { markRecoveryStarted = resolve; });
  let immediateHeartbeats = 0;
  let refreshes = 0;
  let coordinator;
  const router = createIndependentHeartbeatRouter(async (reason) => {
    if (reason === "reviewed-control-recovery") {
      assert.equal(coordinator.beginRecovery(), true);
      refreshes += 1;
      markRecoveryStarted();
      await new Promise((resolve) => { releaseRecovery = resolve; });
      assert.equal(coordinator.completeRecovery(false), true);
      return { ok: false, reason: "RECOVERY_FAILED" };
    }
    if (reason === "mutation") return { ok: true, reason: "TRAILING_SUCCEEDED" };
    return { ok: true };
  });
  coordinator = createReviewedControlRecoveryCoordinator({
    requestRecovery: () => router.request("reviewed-control-recovery"),
    async requestImmediateHeartbeat() { immediateHeartbeats += 1; },
  });
  coordinator.observeHeartbeatResult({
    ok: false,
    reason: "Eastmoney time-sales DOM order disagrees with its reviewed control",
  });
  await recoveryStarted;
  void router.request("mutation");
  releaseRecovery();
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(refreshes, 1);
  assert.equal(immediateHeartbeats, 0,
    "trailing success must not counterfeit the dedicated recovery outcome");
  assert.equal(coordinator.snapshot().running, false);
});

test("reviewed-control recovery worst-case committed ACK remains strictly under sixty seconds", () => {
  const immediate = reviewedControlRecoveryWorstCaseMs();
  const waitsForAnotherAlarm = immediate + 15_000;
  assert.deepEqual(REVIEWED_CONTROL_RECOVERY_BUDGET_MS, {
    alarm_phase: 15_000,
    mismatch_detection: 4_000,
    regular_recovery: 8_000,
    immediate_heartbeat: 12_000,
    committed_ack: 15_000,
  });
  assert.equal(immediate, 54_000);
  assert.ok(immediate < 60_000);
  assert.equal(waitsForAnotherAlarm, 69_000);
  assert.ok(waitsForAnotherAlarm >= 60_000,
    "waiting for the next alarm must remain a red availability contract");
});

test("the real mismatch retry path exhausts the code-bound four-second deadline", async () => {
  let now = 0;
  let attempts = 0;
  const deadline = createDeadline({
    timeoutMs: REVIEWED_CONTROL_RECOVERY_BUDGET_MS.mismatch_detection,
    now: () => now,
    scheduleTimeout() { return 1; },
    cancelTimeout() {},
  });
  await assert.rejects(readCaptureWithRetry({
    readCapture() {
      attempts += 1;
      throw new Error("Eastmoney time-sales DOM order disagrees with its reviewed control");
    },
    isRetriableError(error) {
      return error.message ===
        "Eastmoney time-sales DOM order disagrees with its reviewed control";
    },
    async delay() { now += 100; },
    maxAttempts: 60,
    deadline,
  }), /exceeded its reviewed deadline/);
  assert.equal(now, REVIEWED_CONTROL_RECOVERY_BUDGET_MS.mismatch_detection);
  assert.equal(attempts, 40);
});

test("heartbeat mismatch deadline preserves the exact reviewed-control classification", async () => {
  let now = 0;
  let attempts = 0;
  const exact = "Eastmoney time-sales DOM order disagrees with its reviewed control";
  const deadline = createDeadline({
    timeoutMs: REVIEWED_CONTROL_RECOVERY_BUDGET_MS.mismatch_detection,
    now: () => now,
    scheduleTimeout() { return 1; },
    cancelTimeout() {},
  });
  await assert.rejects(readCaptureWithRetry({
    readCapture() { attempts += 1; throw new Error(exact); },
    isRetriableError: (error) => error.message === exact,
    async delay() { now += 100; },
    maxAttempts: 60,
    deadline,
    preserveLastRetriableOnDeadline: true,
  }), (error) => error.message === exact);
  assert.equal(now, 4_000);
  assert.equal(attempts, 40);
});

test("initialization error tracker is generation-safe and clears only on current success", () => {
  const tracker = createInitializationErrorTracker();
  assert.equal(tracker.current(), null);
  const first = tracker.begin();
  tracker.fail(first, new Error("first failure"));
  assert.equal(tracker.current(), "first failure");
  const second = tracker.begin();
  tracker.fail(second, "replacement failure");
  assert.equal(tracker.current(), "replacement failure");
  tracker.succeed(second);
  assert.equal(tracker.current(), null);
  tracker.fail(first, new Error("late stale failure"));
  assert.equal(tracker.current(), null,
    "an old generation must not overwrite a newer successful initialization");
});

test("rolling reviewed snapshots merge one contiguous suffix-prefix transition without requiring market silence", () => {
  const make = (keys) => ({
    provider: "eastmoney",
    provider_version: "v6",
    source_url: "https://quote.eastmoney.com/f1.html?newcode=0.002256",
    session_date: "2026-08-31",
    instrument: { venue: "XSHE", symbol: "002256" },
    completeness: { page_index: 1, page_count: 20, row_count: keys.length },
    rows: keys.map((key) => ({ source_row_key: key, price: `3.${key.length}` })),
  });
  const merged = mergeRollingPageCaptures(make(["a", "b", "c"]), make(["b", "c", "d", "e"]), {
    rowIdentity: (row) => row.source_row_key,
    rowEvidence: (row) => JSON.stringify(row),
  });
  assert.deepEqual(merged.rows.map((row) => row.source_row_key), ["a", "b", "c", "d", "e"]);
  assert.equal(merged.completeness.row_count, 5);
});

test("rolling reviewed snapshots reject gaps conflicts reorder and identity drift", () => {
  const make = (keys, symbol = "002256") => ({
    provider: "eastmoney",
    provider_version: "v6",
    source_url: `https://quote.eastmoney.com/f1.html?newcode=0.${symbol}`,
    session_date: "2026-08-31",
    instrument: { venue: "XSHE", symbol },
    completeness: { page_index: 1, page_count: 20, row_count: keys.length },
    rows: keys.map((key) => ({ source_row_key: key, price: "3.37" })),
  });
  const options = {
    rowIdentity: (row) => row.source_row_key,
    rowEvidence: (row) => JSON.stringify(row),
  };
  assert.throws(() => mergeRollingPageCaptures(make(["a", "b", "c"]), make(["b", "d"]), options),
    /contiguous overlap/);
  const conflict = make(["b", "c", "d"]);
  conflict.rows[0].price = "9.99";
  assert.throws(() => mergeRollingPageCaptures(make(["a", "b", "c"]), conflict, options),
    /conflicting overlap/);
  assert.throws(() => mergeRollingPageCaptures(make(["a", "b", "c"]), make(["c", "b", "d"]), options),
    /contiguous overlap/);
  assert.throws(() => mergeRollingPageCaptures(make(["a", "b", "c"]), make(["b", "c", "d"], "000001"), options),
    /identity changed/);
});

test("an empty-state resume boundary accepts one immutable reviewed snapshot under a rolling table", async () => {
  let reads = 0;
  const capture = {
    rows: [{ source_row_key: "14:15:00#0" }],
    completeness: { page_index: 1, page_count: 3 },
  };
  const result = await captureAtomicReviewedPage({
    readCapture: async () => {
      reads += 1;
      return capture;
    },
    stableCaptureHash: async () => "capture-a",
    rowsetHash: async () => "rowset-a",
    expectedPageIndex: 1,
  });
  assert.equal(reads, 1, "a continuously rolling rowset must not require an identical second read");
  assert.deepEqual(result, { capture, hash: "capture-a", rowsetHash: "rowset-a" });
});

test("an atomic reviewed snapshot still rejects empty, wrong-page, and forbidden boundary evidence", async () => {
  const common = {
    stableCaptureHash: async () => "capture-a",
    rowsetHash: async () => "rowset-a",
    expectedPageIndex: 1,
  };
  await assert.rejects(
    captureAtomicReviewedPage({
      ...common,
      readCapture: async () => ({ rows: [], completeness: { page_index: 1, page_count: 1 } }),
    }),
    /atomic reviewed page contains no reviewed rows/,
  );
  await assert.rejects(
    captureAtomicReviewedPage({
      ...common,
      readCapture: async () => ({
        rows: [{ source_row_key: "x" }],
        completeness: { page_index: 2, page_count: 3 },
      }),
    }),
    /reviewed page identity/,
  );
  await assert.rejects(
    captureAtomicReviewedPage({
      ...common,
      forbiddenRowsetHash: "rowset-a",
      readCapture: async () => ({
        rows: [{ source_row_key: "x" }],
        completeness: { page_index: 1, page_count: 3 },
      }),
    }),
    /forbidden rowset/,
  );
});

test("only a regular lane may perform bounded live-overlap UI recovery", () => {
  const error = new Error("live capture has no overlap with the prior durable watermark");
  assert.equal(shouldRecoverLiveOverlap("heartbeat", error), false);
  assert.equal(shouldRecoverLiveOverlap("mutation", error), true);
  assert.equal(shouldRecoverLiveOverlap("manual", error), true);
  assert.equal(shouldRecoverLiveOverlap("mutation", new Error("durable conflict")), false);
  assert.equal(shouldRecoverLiveOverlap("mutation", new Error(`prefix: ${error.message}`)), false);
  assert.equal(shouldRecoverLiveOverlap("mutation", new Error(`${error.message}: suffix`)), false);
  assert.equal(
    shouldRecoverLiveOverlap("manual", new Error(`${error.message}; durable conflict`)),
    false,
  );
});

test("a live overlap loss establishes one bounded suffix boundary without recrawling history", async () => {
  const calls = [];
  const result = await recoverLiveOverlap({
    establishResumeBoundary: async () => {
      calls.push("boundary");
      return { ok: true, sequence: 9000 };
    },
  });
  assert.equal(result.mode, "RESUME_BOUNDARY");
  assert.deepEqual(result.boundary, { ok: true, sequence: 9000 });
  assert.deepEqual(calls, ["boundary"]);
});

test("same-day complete evidence repairs live overlap loss with an explicit discontinuity boundary", async () => {
  const calls = [];
  const result = await recoverLiveOverlap({
    state: { complete_session_date: "2026-09-03" },
    sessionDate: "2026-09-03",
    establishDiscontinuityBoundary: async () => {
      calls.push("discontinuity-boundary");
      return { ok: true, sequence: 25000 };
    },
    establishResumeBoundary: async () => {
      calls.push("boundary");
      return { ok: true, sequence: 25001 };
    },
  });
  assert.equal(result.mode, "DISCONTINUITY_BOUNDARY");
  assert.deepEqual(result.boundary, { ok: true, sequence: 25000 });
  assert.deepEqual(calls, ["discontinuity-boundary"]);
});

test("same-day resume boundary skips an unnecessary full history recrawl", () => {
  assert.equal(hasCurrentSessionEvidence({
    resume_boundary_session_date: "2026-08-27",
  }, "2026-08-27"), true);
  assert.equal(hasCurrentSessionEvidence({
    complete_session_date: "2026-08-27",
  }, "2026-08-27"), true);
  assert.equal(hasCurrentSessionEvidence({
    resume_boundary_session_date: "2026-08-26",
  }, "2026-08-27"), false);
});

test("initialization routes same-day evidence without crawling and returns to page one", async () => {
  const calls = [];
  const currentPage = { capture: {
    session_date: "2026-08-27",
    completeness: { page_index: 3 },
  }, rowsetHash: "page-three" };
  const result = await routeCollectorInitialization({
    state: { resume_boundary_session_date: "2026-08-27" },
    currentPage,
    navigateHome: async () => calls.push("home"),
    captureStableFirstPage: async (pageIndex, forbidden) => {
      calls.push(`capture:${pageIndex}:${forbidden}`);
      return { capture: {
        session_date: "2026-08-27",
        completeness: { page_index: 1 },
      }, rowsetHash: "page-one" };
    },
    crawlSessionHistory: async () => calls.push("crawl"),
    establishResumeBoundary: async () => calls.push("boundary"),
  });
  assert.equal(result.mode, "CURRENT_SESSION");
  assert.equal(result.currentPage.capture.completeness.page_index, 1);
  assert.deepEqual(calls, ["home", "capture:1:page-three"]);
});

test("initialization crawls exactly once across sessions and falls back to a boundary", async () => {
  const calls = [];
  const currentPage = { capture: {
    session_date: "2026-08-27",
    completeness: { page_index: 1 },
  }, rowsetHash: "current" };
  const result = await routeCollectorInitialization({
    state: { resume_boundary_session_date: "2026-08-26" },
    currentPage,
    navigateHome: async () => calls.push("home"),
    captureStableFirstPage: async () => { throw new Error("unexpected capture"); },
    crawlSessionHistory: async () => {
      calls.push("crawl");
      throw new Error("Eastmoney page 2 did not become stable");
    },
    establishResumeBoundary: async () => {
      calls.push("boundary");
      return { ok: true };
    },
  });
  assert.equal(result.mode, "RESUME_BOUNDARY");
  assert.deepEqual(calls, ["crawl", "boundary"]);
});

test("isolated empty-state E2E establishes one boundary without navigating protected history", async () => {
  const calls = [];
  const currentPage = { capture: {
    session_date: "2026-08-31",
    completeness: { page_index: 1 },
  }, rowsetHash: "current" };
  const result = await routeCollectorInitialization({
    state: null,
    currentPage,
    initializationPolicy: "ISOLATED_EMPTY_STATE_RESUME_BOUNDARY_V1",
    navigateHome: async () => calls.push("home"),
    captureStableFirstPage: async () => { throw new Error("unexpected capture"); },
    crawlSessionHistory: async () => calls.push("crawl"),
    establishResumeBoundary: async () => {
      calls.push("boundary");
      return { ok: true };
    },
  });
  assert.equal(result.mode, "RESUME_BOUNDARY");
  assert.deepEqual(calls, ["boundary"]);
});

test("fatal history identity or durable errors never downgrade to a partial boundary", async () => {
  assert.equal(historyErrorAllowsResumeBoundary(
    new Error("Eastmoney page 2 did not become stable")), true);
  assert.equal(historyErrorAllowsResumeBoundary(
    new Error("history pages do not share one reviewed source session")), false);
  assert.equal(historyErrorAllowsResumeBoundary(
    new Error("durable capture conflict")), false);
  let boundaryCalls = 0;
  await assert.rejects(routeCollectorInitialization({
    state: null,
    currentPage: { capture: {
      session_date: "2026-08-27",
      completeness: { page_index: 1 },
    } },
    navigateHome: async () => {},
    captureStableFirstPage: async () => { throw new Error("unexpected capture"); },
    crawlSessionHistory: async () => {
      throw new Error("history pages do not share one reviewed source session");
    },
    establishResumeBoundary: async () => { boundaryCalls += 1; },
  }), /do not share one reviewed source session/);
  assert.equal(boundaryCalls, 0);
});

test("source observation deadline bounds nested reviewed capture retries", async () => {
  let now = 0;
  const deadline = createDeadline({ timeoutMs: 12_000, now: () => now });
  await assert.rejects(readCaptureWithRetry({
    readCapture: async () => { throw new Error("transient reviewed redraw"); },
    isRetriableError: () => true,
    delay: async () => { now += 100; },
    maxAttempts: 600,
    deadline,
  }), /exceeded its reviewed deadline/);
  assert.equal(now, 12_000);
});

test("a near-expired observation budget bounds the final HEAD request", async () => {
  let now = 0;
  let scheduled = null;
  const deadline = createDeadline({
    timeoutMs: 12_000,
    now: () => now,
    scheduleTimeout(callback, milliseconds) {
      scheduled = { callback, milliseconds };
      return 1;
    },
    cancelTimeout() {},
  });
  now = 11_900;
  const result = deadline.run(async () => await new Promise(() => {}));
  assert.equal(deadline.remainingMs(), 100);
  assert.equal(scheduled.milliseconds, 100);
  scheduled.callback();
  await assert.rejects(result, /exceeded its reviewed deadline/);
});

test("two queued bounded regular mutations release a deferred heartbeat inside forty-five seconds", async () => {
  let now = 0;
  let nextTimerId = 1;
  const timers = new Map();
  const guard = createUiMutationGuard();
  const observations = [];
  const router = createIndependentHeartbeatRouter(async (reason) => {
    if (reason === "heartbeat") {
      if (!guard.isStable(guard.snapshot())) {
        return { ok: false, reason: "WAITING_FOR_STABLE_REVIEWED_UI" };
      }
      observations.push(now);
      return { ok: true, committed: true };
    }
    const finish = guard.beginMutation();
    try {
      const deadline = createDeadline({
        timeoutMs: 8_000,
        now: () => now,
        scheduleTimeout(callback, milliseconds) {
          const id = nextTimerId++;
          timers.set(id, { callback, due: now + milliseconds });
          return id;
        },
        cancelTimeout(id) { timers.delete(id); },
      });
      await deadline.run(async () => await new Promise(() => {}));
      return { ok: true };
    } catch (error) {
      return { ok: false, reason: String(error?.message ?? error) };
    } finally {
      finish();
    }
  });

  const regular = router.request("mutation");
  await new Promise((resolve) => setImmediate(resolve));
  router.request("stability");
  assert.deepEqual(
    await router.request("heartbeat"),
    { ok: false, reason: "WAITING_FOR_STABLE_REVIEWED_UI" },
  );
  for (const expectedNow of [8_000, 16_000]) {
    const next = [...timers.entries()].sort((left, right) => left[1].due - right[1].due)[0];
    assert.ok(next, `missing regular deadline ending at ${expectedNow}`);
    now = next[1].due;
    timers.delete(next[0]);
    next[1].callback();
    await new Promise((resolve) => setImmediate(resolve));
  }
  await regular;
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(observations, [16_000]);
  assert.ok(observations[0] + 12_000 + 15_000 <= 45_000,
    "two queued regular deadlines plus observation and DB ACK must meet the target");
});

test("a continuous regular mutation stream yields to heartbeat after every bounded batch", async () => {
  const order = [];
  let regularRuns = 0;
  let router;
  router = createIndependentHeartbeatRouter(async (reason) => {
    order.push(reason);
    if (reason === "heartbeat") {
      return { ok: true, committed: true };
    }
    regularRuns += 1;
    if (regularRuns < 6) router.request(`mutation-${regularRuns + 1}`);
    return { ok: false, reason: "UNCHANGED" };
  });

  await router.request("mutation-1");
  assert.deepEqual(order, [
    "mutation-1", "mutation-2", "heartbeat",
    "mutation-3", "mutation-4", "heartbeat",
    "mutation-5", "mutation-6",
  ]);
});

test("a heartbeat snapshot is invalidated by any overlapping reviewed UI mutation", () => {
  const guard = createUiMutationGuard();
  const before = guard.snapshot();
  assert.equal(guard.isStable(before), true);
  const finish = guard.beginMutation();
  assert.equal(guard.isStable(before), false);
  const during = guard.snapshot();
  assert.equal(guard.isStable(during), false);
  finish();
  assert.equal(guard.isStable(before), false);
  assert.equal(guard.isStable(guard.snapshot()), true);
  finish();
  assert.equal(guard.isStable(guard.snapshot()), true);
});

test("a heartbeat rejects a replaced or externally toggled reviewed control", () => {
  const original = { checked: true };
  assert.equal(reviewedControlIsUnchanged(original, original), true);
  assert.equal(reviewedControlIsUnchanged(original, { checked: true }), false);
  original.checked = false;
  assert.equal(reviewedControlIsUnchanged(original, original), false);
  assert.equal(reviewedControlIsUnchanged(null, null), false);
});

function page(index, rows) {
  return {
    completeness: { page_index: index, page_count: 2 },
    rows: rows.map((value) => ({ value })),
  };
}

function scriptedReader(captures) {
  let index = 0;
  return async () => captures[Math.min(index++, captures.length - 1)];
}

const fullHash = async (capture) =>
  `${capture.completeness.page_index}:${capture.rows.map((row) => row.value).join(",")}`;
const rowsHash = async (capture) => capture.rows.map((row) => row.value).join(",");
const noDelay = async () => {};

test("an unchanged reviewed page schedules a bounded source heartbeat", async () => {
  const requests = [];
  let scheduled = null;
  let interval = null;
  const handle = installSourceHeartbeat(
    async (reason) => requests.push(reason),
    {
      intervalMs: 30_000,
      scheduleEvery(callback, milliseconds) {
        scheduled = callback;
        interval = milliseconds;
        return "heartbeat-handle";
      },
    },
  );

  assert.equal(handle, "heartbeat-handle");
  assert.equal(interval, 30_000);
  await scheduled();
  assert.deepEqual(requests, ["heartbeat"]);
  assert.equal(shouldDeliverCapture("mutation", "same", "same"), false);
  assert.equal(shouldDeliverCapture("heartbeat", "same", "same"), false);
  assert.equal(shouldDeliverCapture("manual", "same", "same"), true);
  assert.equal(shouldDeliverCapture("mutation", "new", "old"), true);
});

test("a source heartbeat actively refreshes latest-first before accepting one stable observation", async () => {
  const calls = [];
  const stable = { capture: { captured_at_us: 123 }, rowsetHash: "stable" };
  const result = await captureSourceObservation({
    async refreshLatestFirst() {
      calls.push("refresh");
    },
    async captureStableFirstPage(expectedPageIndex) {
      calls.push(`capture:${expectedPageIndex}`);
      return stable;
    },
    validateObservationTiming(capture) {
      calls.push(`validate:${capture.captured_at_us}`);
    },
  });

  assert.equal(result, stable);
  assert.deepEqual(calls, ["refresh", "capture:1", "validate:123"]);
});

test("a clock-bound source heartbeat does not depend on a changing trade rowset", async () => {
  const calls = [];
  const stable = {
    capture: { captured_at_us: 123_000_000, source_row_order: "LATEST_FIRST" },
    rowsetHash: "unchanged",
  };
  const result = await captureClockBoundSourceObservation({
    async captureStableFirstPage(expectedPageIndex) {
      calls.push(`capture:${expectedPageIndex}`);
      return stable;
    },
    sourcePageObservedAtUs() {
      calls.push("source-clock");
      return 122_000_000;
    },
    validateObservationTiming(capture) {
      calls.push(`validate:${capture.source_page_observed_at_us}`);
    },
  });

  assert.equal(result.rowsetHash, "unchanged");
  assert.equal(result.capture.source_page_observed_at_us, 122_000_000);
  assert.deepEqual(calls, ["capture:1", "source-clock", "validate:122000000"]);
});

test("an HTTPS server-clock heartbeat binds independent source liveness", async () => {
  const stable = {
    capture: { captured_at_us: 123_000_000, source_row_order: "LATEST_FIRST" },
    rowsetHash: "unchanged",
  };
  const result = await captureServerClockBoundSourceObservation({
    async captureStableFirstPage() { return stable; },
    async sourceServerObservedAtUs() { return 122_000_000; },
    capturedAtUs() { return 123_500_000; },
    validateObservationTiming(capture) {
      assert.equal(capture.source_clock_origin, "EASTMONEY_HTTPS_DATE_HEADER");
      assert.equal(capture.source_server_observed_at_us, 122_000_000);
    },
  });
  assert.equal(result.capture.source_server_observed_at_us, 122_000_000);
  assert.equal(result.capture.captured_at_us, 123_500_000);
});

test("an HTTPS clock slightly ahead of the Mac waits inside the reviewed deadline", async () => {
  const stable = {
    capture: { captured_at_us: 123_000_000, source_row_order: "LATEST_FIRST" },
    rowsetHash: "unchanged",
  };
  const localClocks = [124_500_000, 125_000_000];
  const waits = [];
  const result = await captureServerClockBoundSourceObservation({
    async captureStableFirstPage() { return stable; },
    async sourceServerObservedAtUs() { return 125_000_000; },
    capturedAtUs() { return localClocks.shift(); },
    async waitForLocalClock(sourceClock, remainingUs) {
      waits.push([sourceClock, remainingUs]);
    },
    validateObservationTiming(capture) {
      assert.ok(capture.captured_at_us >= capture.source_server_observed_at_us);
    },
  });
  assert.equal(result.capture.source_server_observed_at_us, 125_000_000);
  assert.equal(result.capture.captured_at_us, 125_000_000);
  assert.deepEqual(waits, [[125_000_000, 500_000]]);
});

test("an HTTPS clock beyond the twelve-second budget fails before validation", async () => {
  let nowMs = 0;
  let localClockUs = 0;
  let validateCalls = 0;
  const deadline = createDeadline({
    timeoutMs: 12_000,
    now: () => nowMs,
    scheduleTimeout() { return 1; },
    cancelTimeout() {},
  });
  await assert.rejects(captureServerClockBoundSourceObservation({
    async captureStableFirstPage() {
      return { capture: { source_row_order: "LATEST_FIRST" }, rowsetHash: "same" };
    },
    async sourceServerObservedAtUs() { return 13_000_000; },
    capturedAtUs() { return localClockUs; },
    async waitForLocalClock() {
      nowMs = 12_000;
      localClockUs = 12_000_000;
    },
    validateObservationTiming() { validateCalls += 1; },
    deadline,
  }), /exceeded its reviewed deadline/);
  assert.equal(validateCalls, 0);
});

test("an HTTPS wait that does not advance the Mac clock fails before validation", async () => {
  let validateCalls = 0;
  await assert.rejects(captureServerClockBoundSourceObservation({
    async captureStableFirstPage() {
      return { capture: { source_row_order: "LATEST_FIRST" }, rowsetHash: "same" };
    },
    async sourceServerObservedAtUs() { return 2_000_000; },
    capturedAtUs() { return 1_000_000; },
    async waitForLocalClock() {},
    validateObservationTiming() { validateCalls += 1; },
  }), /local capture clock did not advance/);
  assert.equal(validateCalls, 0);
});

test("catching up too far still delegates to the strict fifteen-second stale check", async () => {
  const localClocks = [1_000_000, 18_000_001];
  await assert.rejects(captureServerClockBoundSourceObservation({
    async captureStableFirstPage() {
      return { capture: { source_row_order: "LATEST_FIRST" }, rowsetHash: "same" };
    },
    async sourceServerObservedAtUs() { return 2_000_000; },
    capturedAtUs() { return localClocks.shift(); },
    async waitForLocalClock() {},
    validateObservationTiming(capture) {
      if (capture.captured_at_us - capture.source_server_observed_at_us > 15_000_000) {
        throw new Error("source HTTPS clock is stale");
      }
    },
  }), /source HTTPS clock is stale/);
});

test("an HTTPS clock already behind the Mac never enters the wait path", async () => {
  let waitCalls = 0;
  const result = await captureServerClockBoundSourceObservation({
    async captureStableFirstPage() {
      return { capture: { source_row_order: "LATEST_FIRST" }, rowsetHash: "same" };
    },
    async sourceServerObservedAtUs() { return 1_000_000; },
    capturedAtUs() { return 2_000_000; },
    async waitForLocalClock() { waitCalls += 1; },
    validateObservationTiming() {},
  });
  assert.equal(result.capture.captured_at_us, 2_000_000);
  assert.equal(waitCalls, 0);
});

test("an active source observation rejects a latest-first cycle with no reviewed rowset effect", async () => {
  let reads = 0;
  await assert.rejects(
    () => waitForReviewedRowsetEffect({
      previousRowsetHash: "stale",
      async readRowsetHash() {
        reads += 1;
        return "stale";
      },
      delay: async () => {},
      maxAttempts: 3,
    }),
    /did not produce a reviewed rowset effect/,
  );
  assert.equal(reads, 3);
});

test("an active source observation accepts one explicit reviewed rowset effect", async () => {
  const hashes = ["stale", "fresh"];
  await waitForReviewedRowsetEffect({
    previousRowsetHash: "stale",
    async readRowsetHash() {
      return hashes.shift();
    },
    delay: async () => {},
    maxAttempts: 3,
  });
  assert.deepEqual(hashes, []);
});

test("a transient reviewed-control mismatch is retried before the snapshot is used", async () => {
  let reads = 0;
  const capture = page(1, ["11:27:03", "11:27:00"]);
  const result = await readCaptureWithRetry({
    async readCapture() {
      reads += 1;
      if (reads <= 3) {
        throw new Error("Eastmoney time-sales DOM order disagrees with its reviewed control");
      }
      return capture;
    },
    isRetriableError(error) {
      return error.message === "Eastmoney time-sales DOM order disagrees with its reviewed control";
    },
    delay: noDelay,
    maxAttempts: 4,
  });

  assert.equal(result, capture);
  assert.equal(reads, 4);
});

test("an unreviewed capture error is never hidden by the transient retry", async () => {
  let reads = 0;
  await assert.rejects(
    readCaptureWithRetry({
      async readCapture() {
        reads += 1;
        throw new Error("Eastmoney instrument identity changed");
      },
      isRetriableError(error) {
        return error.message === "Eastmoney time-sales DOM order disagrees with its reviewed control";
      },
      delay: noDelay,
      maxAttempts: 4,
    }),
    /instrument identity changed/,
  );
  assert.equal(reads, 1);
});

test("page stability waits for rows after the page token changes", async () => {
  const staleRows = ["old-a", "old-b"];
  const freshRows = ["new-a", "new-b"];
  const result = await captureStablePage({
    readCapture: scriptedReader([
      page(1, staleRows),
      page(1, staleRows),
      page(1, freshRows),
      page(1, freshRows),
    ]),
    stableCaptureHash: fullHash,
    rowsetHash: rowsHash,
    delay: noDelay,
    expectedPageIndex: 1,
    forbiddenRowsetHash: staleRows.join(","),
    maxAttempts: 4,
  });

  assert.equal(result.rowsetHash, freshRows.join(","));
});

test("an empty initial Eastmoney table is refreshed once before initialization retries", async () => {
  let captures = 0;
  let refreshes = 0;
  const recovered = { capture: page(1, ["live-row"]), hash: "live", rowsetHash: "live-row" };
  const result = await captureInitialPageWithRefresh({
    async captureStableFirstPage(forbiddenRowsetHash) {
      captures += 1;
      assert.equal(forbiddenRowsetHash, null);
      if (captures === 1) throw new Error("Eastmoney page ? did not become stable");
      return recovered;
    },
    async refreshLatestFirst() {
      refreshes += 1;
    },
    isRefreshableInitialError(error) {
      return error.message === "Eastmoney page ? did not become stable";
    },
    validateCaptureTiming() {},
  });

  assert.equal(result, recovered);
  assert.equal(captures, 2);
  assert.equal(refreshes, 1);
});

test("a stale initial page must change rowset and become current after refresh", async () => {
  const stale = { capture: page(1, ["09:35:00"]), hash: "stale", rowsetHash: "stale-rows" };
  const current = { capture: page(1, ["11:05:00"]), hash: "current", rowsetHash: "current-rows" };
  const forbidden = [];
  let captures = 0;
  let refreshes = 0;
  const result = await captureInitialPageWithRefresh({
    async captureStableFirstPage(forbiddenRowsetHash) {
      forbidden.push(forbiddenRowsetHash);
      captures += 1;
      return captures === 1 ? stale : current;
    },
    async refreshLatestFirst() {
      refreshes += 1;
    },
    isRefreshableInitialError(error) {
      return error.message === "capture latest row is stale";
    },
    validateCaptureTiming(capture) {
      if (capture === stale.capture) throw new Error("capture latest row is stale");
    },
  });

  assert.equal(result, current);
  assert.deepEqual(forbidden, [null, "stale-rows"]);
  assert.equal(refreshes, 1);
});

test("an unknown initial identity error is fatal without refreshing or rereading", async () => {
  const identityError = new Error("Eastmoney instrument identity changed");
  let captures = 0;
  let refreshes = 0;
  await assert.rejects(
    captureInitialPageWithRefresh({
      async captureStableFirstPage() {
        captures += 1;
        throw identityError;
      },
      async refreshLatestFirst() {
        refreshes += 1;
      },
      isRefreshableInitialError(error) {
        return error.message === "Eastmoney page ? did not become stable";
      },
      validateCaptureTiming() {},
    }),
    (error) => error === identityError,
  );
  assert.equal(captures, 1);
  assert.equal(refreshes, 0);
});

test("single-page history may stabilize without inventing a prior-page guard", async () => {
  const onlyPage = page(1, ["only-a", "only-b"]);
  onlyPage.completeness.page_count = 1;
  const result = await captureStablePage({
    readCapture: scriptedReader([onlyPage, onlyPage]),
    stableCaptureHash: fullHash,
    rowsetHash: rowsHash,
    delay: noDelay,
    expectedPageIndex: 1,
    forbiddenRowsetHash: null,
    maxAttempts: 2,
  });

  assert.equal(result.rowsetHash, "only-a,only-b");
});

test("a stale rowset never becomes deliverable merely because its page token is stable", async () => {
  const stale = page(1, ["old-a", "old-b"]);
  await assert.rejects(
    captureStablePage({
      readCapture: scriptedReader([stale, stale, stale]),
      stableCaptureHash: fullHash,
      rowsetHash: rowsHash,
      delay: noDelay,
      expectedPageIndex: 1,
      forbiddenRowsetHash: "old-a,old-b",
      maxAttempts: 3,
    }),
    /did not become stable/,
  );
});

test("a reviewed slow page may replace many stable token-only snapshots before succeeding", async () => {
  const stale = page(2, ["old-a", "old-b"]);
  const fresh = page(2, ["new-a", "new-b"]);
  const captures = Array.from({ length: 30 }, () => stale).concat([fresh, fresh]);
  const result = await captureStablePage({
    readCapture: scriptedReader(captures),
    stableCaptureHash: fullHash,
    rowsetHash: rowsHash,
    delay: noDelay,
    expectedPageIndex: 2,
    forbiddenRowsetHash: "old-a,old-b",
    maxAttempts: 32,
  });

  assert.equal(result.rowsetHash, "new-a,new-b");
});

test("a stale live first page is refreshed through the reviewed order control before delivery", async () => {
  const stale = page(1, ["09:32:33", "09:32:30"]);
  const fresh = page(1, ["09:38:21", "09:38:18"]);
  const actions = [];
  const result = await refreshStaleFirstPage({
    staleCapture: stale,
    rowsetHash: rowsHash,
    refreshLatestFirst: async () => actions.push("latest-first-refresh"),
    captureStableFirstPage: async (forbiddenRowsetHash) => {
      actions.push(`forbidden:${forbiddenRowsetHash}`);
      return { capture: fresh, hash: await fullHash(fresh), rowsetHash: await rowsHash(fresh) };
    },
    validateCaptureTiming(capture) {
      actions.push(`validated:${capture.rows[0].value}`);
    },
  });

  assert.deepEqual(actions, [
    "latest-first-refresh",
    "forbidden:09:32:33,09:32:30",
    "validated:09:38:21",
  ]);
  assert.equal(result.rowsetHash, "09:38:21,09:38:18");
});

test("a stale live first page never reuses the old rowset after the refresh control fires", async () => {
  const stale = page(1, ["09:32:33", "09:32:30"]);
  let refreshed = false;
  await assert.rejects(
    refreshStaleFirstPage({
      staleCapture: stale,
      rowsetHash: rowsHash,
      refreshLatestFirst: async () => { refreshed = true; },
      captureStableFirstPage: async (forbiddenRowsetHash) => {
        assert.equal(forbiddenRowsetHash, "09:32:33,09:32:30");
        throw new Error("Eastmoney page 1 did not become stable");
      },
      validateCaptureTiming() {
        assert.fail("stale rowset must not reach timing validation or delivery");
      },
      maxRefreshAttempts: 1,
    }),
    /did not become stable/,
  );
  assert.equal(refreshed, true);
});

test("latest-first refresh confirms checked to unchecked to checked across replaced controls", async () => {
  const states = [true, false, true];
  const actions = [];
  await cycleLatestFirstControl({
    readControl() {
      const checked = states[0];
      return {
        checked,
        click() {
          actions.push(`click:${checked}`);
          states.shift();
        },
      };
    },
    delay: noDelay,
    async waitForUncheckedEffect() {
      actions.push("unchecked-effect");
    },
    maxStateAttempts: 2,
  });
  assert.deepEqual(actions, ["click:true", "unchecked-effect", "click:false"]);
  assert.deepEqual(states, [true]);
});

test("latest-first refresh times out without inventing a checked control", async () => {
  let clicks = 0;
  await assert.rejects(
    cycleLatestFirstControl({
      readControl: () => null,
      delay: noDelay,
      maxStateAttempts: 3,
    }),
    /did not become checked/,
  );
  assert.equal(clicks, 0);
});

test("stale recovery retries a bounded refresh and validates only the fresh result", async () => {
  const stale = page(1, ["09:32:33"]);
  const fresh = page(1, ["09:39:00"]);
  let refreshes = 0;
  let captures = 0;
  let validations = 0;
  const result = await refreshStaleFirstPage({
    staleCapture: stale,
    rowsetHash: rowsHash,
    refreshLatestFirst: async () => { refreshes += 1; },
    captureStableFirstPage: async () => {
      captures += 1;
      if (captures < 3) throw new Error("transient React redraw");
      return { capture: fresh, hash: await fullHash(fresh), rowsetHash: await rowsHash(fresh) };
    },
    validateCaptureTiming() { validations += 1; },
    retryDelay: noDelay,
    maxRefreshAttempts: 3,
  });
  assert.equal(result.rowsetHash, "09:39:00");
  assert.equal(refreshes, 3);
  assert.equal(captures, 3);
  assert.equal(validations, 1);
});

test("single-flight runner coalesces concurrent mutations into one trailing scan", async () => {
  const started = [];
  const releases = [];
  const runner = createSingleFlightRunner(async (reason) => {
    started.push(reason);
    await new Promise((resolve) => releases.push(resolve));
    return reason;
  });
  const first = runner.request("manual");
  await new Promise((resolve) => setImmediate(resolve));
  const mutationA = runner.request("mutation-a");
  const mutationB = runner.request("mutation-b");
  assert.deepEqual(started, ["manual"]);
  releases.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(started, ["manual", "mutation-b"]);
  releases.shift()();
  assert.equal(await first, "mutation-b");
  assert.equal(await mutationA, "mutation-b");
  assert.equal(await mutationB, "mutation-b");
});

test("a bounded source heartbeat is never starved behind a long mutation scan", async () => {
  const started = [];
  let releaseMutation;
  const router = createIndependentHeartbeatRouter(async (reason) => {
    started.push(reason);
    if (reason === "mutation") {
      await new Promise((resolve) => { releaseMutation = resolve; });
    }
    return reason;
  });
  const mutation = router.request("mutation");
  await new Promise((resolve) => setImmediate(resolve));
  const heartbeat = router.request("heartbeat");
  assert.equal(await heartbeat, "heartbeat");
  assert.deepEqual(started, ["mutation", "heartbeat"]);
  releaseMutation();
  await mutation;
});

test("a heartbeat deferred by reviewed UI mutation catches up once after the regular lane becomes idle", async () => {
  let releaseRegular;
  let regularActive = false;
  let heartbeatAttempts = 0;
  let committedAcks = 0;
  const router = createIndependentHeartbeatRouter(async (reason) => {
    if (reason === "heartbeat") {
      heartbeatAttempts += 1;
      if (regularActive) {
        return { ok: false, reason: "WAITING_FOR_STABLE_REVIEWED_UI" };
      }
      committedAcks += 1;
      return { ok: true, committed: true };
    }
    regularActive = true;
    await new Promise((resolve) => { releaseRegular = resolve; });
    regularActive = false;
    return { ok: false, reason: "REGULAR_UI_RECOVERY_COOLDOWN" };
  });

  const regular = router.request("mutation");
  await new Promise((resolve) => setImmediate(resolve));
  const deferred = router.request("heartbeat");
  assert.deepEqual(await deferred, { ok: false, reason: "WAITING_FOR_STABLE_REVIEWED_UI" });
  assert.equal(heartbeatAttempts, 1);

  releaseRegular();
  await regular;
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(heartbeatAttempts, 2, "regular guard release must trigger one catch-up heartbeat");
  assert.equal(committedAcks, 1, "the catch-up must reach the committed application ACK path once");
});

test("a heartbeat catches up when a fast regular scan starts and finishes before its UI-changed result", async () => {
  let releaseHeartbeat;
  let heartbeatAttempts = 0;
  const router = createIndependentHeartbeatRouter(async (reason) => {
    if (reason !== "heartbeat") return { ok: false, reason: "UNCHANGED" };
    heartbeatAttempts += 1;
    if (heartbeatAttempts === 1) {
      await new Promise((resolve) => { releaseHeartbeat = resolve; });
      return { ok: false, reason: "REVIEWED_UI_CHANGED_DURING_HEARTBEAT" };
    }
    return { ok: true, committed: true };
  });

  const heartbeat = router.request("heartbeat");
  await new Promise((resolve) => setImmediate(resolve));
  await router.request("mutation");
  releaseHeartbeat();
  await heartbeat;
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(heartbeatAttempts, 2, "the completed regular generation must still cause one catch-up");
});

test("many deferred heartbeats during one regular scan merge into one catch-up", async () => {
  let releaseRegular;
  let heartbeatAttempts = 0;
  let committed = 0;
  const router = createIndependentHeartbeatRouter(async (reason) => {
    if (reason === "heartbeat") {
      heartbeatAttempts += 1;
      if (releaseRegular) return { ok: false, reason: "WAITING_FOR_STABLE_REVIEWED_UI" };
      committed += 1;
      return { ok: true, committed: true };
    }
    await new Promise((resolve) => { releaseRegular = resolve; });
    releaseRegular = null;
    return { ok: false, reason: "REGULAR_UI_RECOVERY_COOLDOWN" };
  });

  const regular = router.request("mutation");
  await new Promise((resolve) => setImmediate(resolve));
  for (let index = 0; index < 3; index += 1) await router.request("heartbeat");
  releaseRegular();
  await regular;
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(heartbeatAttempts, 4);
  assert.equal(committed, 1, "three deferred observations must merge into one committed catch-up");
});

test("a catch-up that is still waiting without a new regular overlap does not self-spin", async () => {
  let releaseRegular;
  let regularRunning = false;
  let heartbeatAttempts = 0;
  const router = createIndependentHeartbeatRouter(async (reason) => {
    if (reason === "heartbeat") {
      heartbeatAttempts += 1;
      return { ok: false, reason: "WAITING_FOR_STABLE_REVIEWED_UI" };
    }
    regularRunning = true;
    await new Promise((resolve) => { releaseRegular = resolve; });
    regularRunning = false;
    return { ok: false, reason: "UNCHANGED" };
  });

  const regular = router.request("mutation");
  await new Promise((resolve) => setImmediate(resolve));
  await router.request("heartbeat");
  assert.equal(regularRunning, true);
  releaseRegular();
  await regular;
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(heartbeatAttempts, 2, "the one catch-up must not recursively schedule itself");
});

test("a successful periodic heartbeat clears a pending catch-up before regular idle", async () => {
  let releaseRegular;
  let heartbeatAttempts = 0;
  let committed = 0;
  const router = createIndependentHeartbeatRouter(async (reason) => {
    if (reason === "heartbeat") {
      heartbeatAttempts += 1;
      if (heartbeatAttempts === 1) {
        return { ok: false, reason: "WAITING_FOR_STABLE_REVIEWED_UI" };
      }
      committed += 1;
      return { ok: true, committed: true };
    }
    await new Promise((resolve) => { releaseRegular = resolve; });
    return { ok: false, reason: "UNCHANGED" };
  });

  const regular = router.request("mutation");
  await new Promise((resolve) => setImmediate(resolve));
  await router.request("heartbeat");
  await router.request("heartbeat");
  releaseRegular();
  await regular;
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(heartbeatAttempts, 2, "the successful periodic heartbeat replaces the pending catch-up");
  assert.equal(committed, 1);
});

test("heartbeat success cannot reset the regular UI recovery failure budget", () => {
  let now = 1_000;
  const budget = createLaneFailureBudget({
    maxFailures: 3,
    cooldownMs: 45_000,
    now: () => now,
  });
  budget.recordFailure("regular");
  budget.recordFailure("regular");
  budget.recordSuccess("heartbeat");
  assert.equal(budget.snapshot("regular").failures, 2);
  budget.recordFailure("regular");
  assert.equal(budget.canAttempt("regular"), false);
  assert.equal(budget.canAttempt("heartbeat"), true);
  now += 45_000;
  assert.equal(budget.canAttempt("regular"), true);
  assert.equal(budget.snapshot("regular").failures, 0);
});

test("cooldown expiry schedules exactly one unattended regular recovery", () => {
  let now = 1_000;
  let nextTimerId = 1;
  const timers = new Map();
  const budget = createLaneFailureBudget({
    maxFailures: 3,
    cooldownMs: 45_000,
    now: () => now,
  });
  budget.recordFailure("regular");
  budget.recordFailure("regular");
  budget.recordFailure("regular");
  const scheduler = createCooldownRetryScheduler({
    failureBudget: budget,
    lane: "regular",
    minimumDelayMs: 1_500,
    now: () => now,
    scheduleTimeout(callback, delayMs) {
      const id = nextTimerId++;
      timers.set(id, { callback, due: now + delayMs });
      return id;
    },
    cancelTimeout(id) { timers.delete(id); },
  });
  let recoveries = 0;
  scheduler.schedule(() => { recoveries += 1; });
  scheduler.schedule(() => { recoveries += 100; });
  assert.equal(timers.size, 1, "mutation/manual storms must not stack retry timers");
  assert.equal([...timers.values()][0].due, 46_000);
  now = 46_000;
  const timer = [...timers.entries()][0];
  timers.delete(timer[0]);
  timer[1].callback();
  assert.equal(recoveries, 1);
  assert.equal(scheduler.pending(), false);
  assert.equal(budget.snapshot("regular").failures, 0);
});

test("three bounded UI failures still allow committed heartbeats inside forty-five seconds", async () => {
  let now = 0;
  const observedAt = [];
  const committedAt = [];
  const guard = createUiMutationGuard();
  const budget = createLaneFailureBudget({
    maxFailures: 3,
    cooldownMs: 45_000,
    now: () => now,
  });
  let releaseRegular = null;
  let regularAttempts = 0;
  let heartbeatAttempts = 0;
  const ackDelays = [0, 8_000, 0, 8_000];
  const router = createIndependentHeartbeatRouter(async (reason) => {
    if (reason === "heartbeat") {
      heartbeatAttempts += 1;
      const snapshot = guard.snapshot();
      if (!guard.isStable(snapshot)) {
        return { ok: false, reason: "WAITING_FOR_STABLE_REVIEWED_UI" };
      }
      observedAt.push(now);
      // Model durable ingest returning immediately and the exact PostgreSQL
      // COMMITTED application ACK becoming visible eight seconds later.
      committedAt.push(now + ackDelays[committedAt.length]);
      budget.recordSuccess("heartbeat");
      return { ok: true, committed: true };
    }
    regularAttempts += 1;
    const finish = guard.beginMutation();
    try {
      await new Promise((resolve) => { releaseRegular = resolve; });
      throw new Error("Eastmoney latest-first cycle did not produce a reviewed rowset effect");
    } catch (error) {
      budget.recordFailure("regular");
      return { ok: false, reason: error.message };
    } finally {
      finish();
    }
  });

  await router.request("heartbeat");
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const regular = router.request("mutation");
    await new Promise((resolve) => setImmediate(resolve));
    now += 12_000;
    assert.deepEqual(
      await router.request("heartbeat"),
      { ok: false, reason: "WAITING_FOR_STABLE_REVIEWED_UI" },
    );
    releaseRegular();
    await regular;
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.equal(guard.activeMutations(), 0);
  assert.equal(regularAttempts, 3, "the regular cooldown path must not retry in a storm");
  assert.equal(budget.canAttempt("regular"), false);
  assert.equal(heartbeatAttempts, 7, "each deferred heartbeat gets exactly one post-guard catch-up");
  const sourceGaps = observedAt.slice(1).map((value, index) => value - observedAt[index]);
  const committedGaps = committedAt.slice(1).map((value, index) => value - committedAt[index]);
  assert.ok(Math.max(...sourceGaps) <= 36_000, `source V3 gaps were ${sourceGaps.join(",")}`);
  assert.ok(Math.max(...committedGaps) <= 45_000, `committed V3 gaps were ${committedGaps.join(",")}`);

  const worstPhaseCommittedGap = (heartbeatMs) => {
    const sources = [0, 3 * heartbeatMs, 6 * heartbeatMs];
    const visible = [sources[0], sources[1] + 8_000, sources[2]];
    return Math.max(visible[1] - visible[0], visible[2] - visible[1]);
  };
  assert.ok(worstPhaseCommittedGap(12_000) <= 45_000);
  assert.ok(
    worstPhaseCommittedGap(15_000) > 45_000,
    "the jitter model must reject the old fifteen-second contract",
  );
});

test("a failed latest-first cycle releases its guard and restores checked state", async () => {
  const guard = createUiMutationGuard();
  let checked = true;
  let operations = 0;
  const deadline = {
    throwIfExpired() {
      if (operations >= 3) throw new Error("source observation exceeded its reviewed deadline");
    },
    async run(operation) {
      this.throwIfExpired();
      operations += 1;
      return await operation();
    },
  };
  const finish = guard.beginMutation();
  try {
    await assert.rejects(
      cycleLatestFirstControl({
        readControl() {
          return {
            checked,
            click() { checked = !checked; },
          };
        },
        delay: async () => {},
        waitForUncheckedEffect: async () => {
          operations = 3;
        },
        maxStateAttempts: 60,
        deadline,
      }),
      /reviewed deadline/,
    );
  } finally {
    finish();
  }
  assert.equal(checked, true, "failure must not leave the reviewed page earliest-first");
  assert.equal(guard.activeMutations(), 0);
  assert.equal(guard.snapshot().stable, true);
});

test("single-flight runner cannot downgrade a queued manual delivery to a later mutation", async () => {
  const started = [];
  const releases = [];
  const runner = createSingleFlightRunner(async (reason) => {
    started.push(reason);
    await new Promise((resolve) => releases.push(resolve));
    return reason;
  }, {
    mergeReason: (queued, incoming) =>
      queued === "manual" || incoming === "manual" ? "manual" : incoming,
  });
  const first = runner.request("initial");
  await new Promise((resolve) => setImmediate(resolve));
  const manual = runner.request("manual");
  const mutation = runner.request("mutation");
  const stability = runner.request("stability");
  assert.deepEqual(started, ["initial"]);
  releases.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(started, ["initial", "manual"]);
  releases.shift()();
  assert.equal(await first, "manual");
  assert.equal(await manual, "manual");
  assert.equal(await mutation, "manual");
  assert.equal(await stability, "manual");
});

test("a recovered collector ignores both its internal retry and a stale external retry", async () => {
  let attempts = 0;
  let active = 0;
  let maximumActive = 0;
  let initialized = false;
  const retries = [];
  const errors = [];
  const initializer = createRetriableInitializer(async () => {
    attempts += 1;
    active += 1;
    maximumActive = Math.max(maximumActive, active);
    try {
      if (attempts === 1) throw new Error("history crawl was transiently incomplete");
      initialized = true;
      return { ok: true, attempt: attempts };
    } finally {
      active -= 1;
    }
  }, {
    isInitialized: () => initialized,
    scheduleRetry(callback) {
      retries.push(callback);
    },
    async onError(error) {
      errors.push(error.message);
    },
  });

  const first = initializer.request();
  const concurrent = initializer.request();
  assert.equal(first, concurrent);
  assert.deepEqual(await first, {
    ok: false,
    reason: "history crawl was transiently incomplete",
  });
  assert.equal(attempts, 1);
  assert.deepEqual(errors, ["history crawl was transiently incomplete"]);
  assert.equal(retries.length, 1);

  assert.deepEqual(await initializer.request(), { ok: true, attempt: 2 });
  const staleExternalRetry = () => initializer.request();
  assert.deepEqual(await staleExternalRetry(), { ok: true, reason: "ALREADY_INITIALIZED" });
  retries.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(attempts, 2);
  assert.equal(maximumActive, 1);
  assert.equal(retries.length, 0);
});

test("a post-close empty rowset retry initializes automatically when the next session becomes live", async () => {
  let attempts = 0;
  let initialized = false;
  let refreshes = 0;
  let observerStarts = 0;
  let observerStops = 0;
  let initialDeliveries = 0;
  const retries = [];
  const errors = [];
  const validatedRows = [];
  const initializer = createRetriableInitializer(async () => {
    attempts += 1;
    const initialPage = await captureInitialPageWithRefresh({
      async captureStableFirstPage() {
        if (attempts <= 2) throw new Error("Eastmoney page ? did not become stable");
        const capture = page(1, ["09:30:03", "09:30:00"]);
        return { capture, hash: "live-page", rowsetHash: "09:30:03,09:30:00" };
      },
      async refreshLatestFirst() {
        refreshes += 1;
        if (attempts <= 2) {
          throw new Error("Eastmoney latest-first cycle did not produce a reviewed rowset effect");
        }
      },
      isRefreshableInitialError(error) {
        return error.message === "Eastmoney page ? did not become stable";
      },
      validateCaptureTiming(capture) {
        validatedRows.push(capture.rows);
      },
    });
    return await completeProvisionalInitialization({
      markInitialized(value) {
        initialized = value;
      },
      startObserving() {
        observerStarts += 1;
      },
      stopObserving() {
        observerStops += 1;
      },
      async finish() {
        initialDeliveries += 1;
        return { ok: true, rowsetHash: initialPage.rowsetHash };
      },
    });
  }, {
    isInitialized: () => initialized,
    scheduleRetry(callback) {
      retries.push(callback);
    },
    async onError(error) {
      errors.push(error.message);
    },
  });

  assert.deepEqual(await initializer.request(), {
    ok: false,
    reason: "Eastmoney latest-first cycle did not produce a reviewed rowset effect",
  });
  assert.equal(initialized, false);
  assert.equal(attempts, 1);
  assert.equal(refreshes, 1);
  assert.deepEqual(validatedRows, []);
  assert.equal(observerStarts, 0);
  assert.equal(observerStops, 0);
  assert.equal(initialDeliveries, 0);
  assert.deepEqual(errors, [
    "Eastmoney latest-first cycle did not produce a reviewed rowset effect",
  ]);
  assert.equal(retries.length, 1);

  const firstRetry = retries.shift();
  firstRetry();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(initialized, false);
  assert.equal(attempts, 2);
  assert.equal(refreshes, 2);
  assert.deepEqual(validatedRows, []);
  assert.equal(observerStarts, 0);
  assert.equal(observerStops, 0);
  assert.equal(initialDeliveries, 0);
  assert.deepEqual(errors, [
    "Eastmoney latest-first cycle did not produce a reviewed rowset effect",
    "Eastmoney latest-first cycle did not produce a reviewed rowset effect",
  ]);
  assert.equal(retries.length, 1);

  const secondRetry = retries.shift();
  secondRetry();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(initialized, true);
  assert.equal(attempts, 3);
  assert.equal(refreshes, 2);
  assert.equal(validatedRows.length, 1);
  assert.deepEqual(validatedRows[0].map((row) => row.value), ["09:30:03", "09:30:00"]);
  assert.equal(observerStarts, 1);
  assert.equal(observerStops, 0);
  assert.equal(initialDeliveries, 1);
  assert.equal(retries.length, 0);

  firstRetry();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(attempts, 3);
  assert.equal(observerStarts, 1);
  assert.equal(initialDeliveries, 1);
  assert.deepEqual(await initializer.request(), {
    ok: true,
    reason: "ALREADY_INITIALIZED",
  });
});

test("a late initialization failure rolls back readiness and observation before retry", async () => {
  let initialized = false;
  let observing = false;
  const failure = new Error("initial scan reporting failed");

  await assert.rejects(() => completeProvisionalInitialization({
    markInitialized(value) {
      initialized = value;
    },
    startObserving() {
      observing = true;
    },
    stopObserving() {
      observing = false;
    },
    async finish() {
      throw failure;
    },
  }), failure);

  assert.equal(initialized, false);
  assert.equal(observing, false);
});

test("an initialization request already in flight wins over provisional readiness", async () => {
  let initialized = false;
  let release;
  let attempts = 0;
  const started = new Promise((resolve) => {
    release = resolve;
  });
  let finish;
  const gate = new Promise((resolve) => {
    finish = resolve;
  });
  const initializer = createRetriableInitializer(async () => {
    attempts += 1;
    initialized = true;
    release();
    await gate;
    return { ok: true, attempt: attempts };
  }, {
    isInitialized: () => initialized,
    scheduleRetry() {},
  });

  const first = initializer.request();
  await started;
  const concurrent = initializer.request();
  assert.equal(concurrent, first);
  finish();
  assert.deepEqual(await first, { ok: true, attempt: 1 });
  assert.equal(attempts, 1);
});
