"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  captureStablePage,
  captureInitialPageWithRefresh,
  captureSourceObservation,
  completeProvisionalInitialization,
  createRetriableInitializer,
  createSingleFlightRunner,
  cycleLatestFirstControl,
  installSourceHeartbeat,
  readCaptureWithRetry,
  refreshStaleFirstPage,
  shouldDeliverCapture,
  waitForReviewedRowsetEffect,
} = require("../src/page_stability.js");

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
