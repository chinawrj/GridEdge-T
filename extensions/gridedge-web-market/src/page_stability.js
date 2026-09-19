(function initializeGridEdgePageStability(root, factory) {
  const api = factory();
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  root.GridEdgePageStability = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function pageStabilityFactory() {
  "use strict";

  const REVIEWED_CONTROL_RECOVERY_BUDGET_MS = Object.freeze({
    alarm_phase: 15_000,
    mismatch_detection: 4_000,
    regular_recovery: 8_000,
    immediate_heartbeat: 12_000,
    committed_ack: 15_000,
  });

  async function readCaptureWithRetry({
    readCapture,
    isRetriableError,
    delay,
    maxAttempts,
    deadline = null,
    preserveLastRetriableOnDeadline = false,
  }) {
    let lastError = null;
    function preserveRetriable(error) {
      if (preserveLastRetriableOnDeadline && lastError !== null &&
          String(error?.message ?? error) ===
            "source observation exceeded its reviewed deadline") return lastError;
      return error;
    }
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      try {
        deadline?.throwIfExpired();
      } catch (error) {
        throw preserveRetriable(error);
      }
      try {
        return deadline ? await deadline.run(readCapture) : await readCapture();
      } catch (error) {
        if (!isRetriableError(error)) throw error;
        lastError = error;
        if (attempt + 1 < maxAttempts) {
          try {
            deadline?.throwIfExpired();
            if (deadline) await deadline.run(delay);
            else await delay();
          } catch (deadlineError) {
            throw preserveRetriable(deadlineError);
          }
        }
      }
    }
    throw lastError ?? new Error("Eastmoney capture retry exhausted its reviewed attempts");
  }

  async function captureStablePage({
    readCapture,
    stableCaptureHash,
    rowsetHash,
    delay,
    expectedPageIndex = null,
    forbiddenRowsetHash = null,
    maxAttempts,
    deadline = null,
  }) {
    let previousHash = null;
    let previousRowsetHash = null;
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      deadline?.throwIfExpired();
      const capture = deadline ? await deadline.run(readCapture) : await readCapture();
      const pageIndex = capture.completeness.page_index;
      const pageCount = capture.completeness.page_count;
      if (capture.rows.length > 0 && Number.isSafeInteger(pageIndex) &&
          Number.isSafeInteger(pageCount) && pageIndex >= 1 && pageCount >= pageIndex &&
          (expectedPageIndex === null || pageIndex === expectedPageIndex)) {
        const hash = deadline
          ? await deadline.run(() => stableCaptureHash(capture))
          : await stableCaptureHash(capture);
        const currentRowsetHash = deadline
          ? await deadline.run(() => rowsetHash(capture))
          : await rowsetHash(capture);
        if (currentRowsetHash !== forbiddenRowsetHash && hash === previousHash &&
            currentRowsetHash === previousRowsetHash) {
          return { capture, hash, rowsetHash: currentRowsetHash };
        }
        previousHash = hash;
        previousRowsetHash = currentRowsetHash;
      } else {
        previousHash = null;
        previousRowsetHash = null;
      }
      deadline?.throwIfExpired();
      if (deadline) await deadline.run(delay);
      else await delay();
    }
    throw new Error(`Eastmoney page ${expectedPageIndex ?? "?"} did not become stable`);
  }

  async function captureAtomicReviewedPage({
    readCapture,
    stableCaptureHash,
    rowsetHash,
    expectedPageIndex = null,
    forbiddenRowsetHash = null,
    deadline = null,
  }) {
    deadline?.throwIfExpired();
    const capture = deadline ? await deadline.run(readCapture) : await readCapture();
    const pageIndex = capture.completeness.page_index;
    const pageCount = capture.completeness.page_count;
    if (capture.rows.length === 0) {
      throw new Error("atomic reviewed page contains no reviewed rows");
    }
    if (!Number.isSafeInteger(pageIndex) || !Number.isSafeInteger(pageCount) ||
        pageIndex < 1 || pageCount < pageIndex ||
        (expectedPageIndex !== null && pageIndex !== expectedPageIndex)) {
      throw new Error("atomic reviewed page has an invalid reviewed page identity");
    }
    const hash = deadline
      ? await deadline.run(() => stableCaptureHash(capture))
      : await stableCaptureHash(capture);
    const currentRowsetHash = deadline
      ? await deadline.run(() => rowsetHash(capture))
      : await rowsetHash(capture);
    if (currentRowsetHash === forbiddenRowsetHash) {
      throw new Error("atomic reviewed page reused the forbidden rowset");
    }
    return { capture, hash, rowsetHash: currentRowsetHash };
  }

  function mergeRollingPageCaptures(previousCapture, currentCapture, {
    rowIdentity,
    rowEvidence,
  }) {
    const identity = (capture) => JSON.stringify({
      provider: capture?.provider,
      provider_version: capture?.provider_version,
      source_url: capture?.source_url,
      session_date: capture?.session_date,
      instrument: capture?.instrument,
      page_kind: capture?.page_kind,
      source_row_order: capture?.source_row_order,
      page_index: capture?.completeness?.page_index,
      identity_policy: capture?.completeness?.identity_policy,
    });
    if (identity(previousCapture) !== identity(currentCapture) ||
        previousCapture?.completeness?.page_index !== 1 ||
        currentCapture?.completeness?.page_index !== 1 ||
        !Number.isSafeInteger(previousCapture?.completeness?.page_count) ||
        !Number.isSafeInteger(currentCapture?.completeness?.page_count) ||
        currentCapture.completeness.page_count < previousCapture.completeness.page_count) {
      throw new Error("rolling reviewed page identity changed");
    }
    if (!Array.isArray(previousCapture.rows) || previousCapture.rows.length === 0 ||
        !Array.isArray(currentCapture.rows) || currentCapture.rows.length === 0) {
      throw new Error("rolling reviewed page has no reviewed rows");
    }
    const previousIds = previousCapture.rows.map(rowIdentity);
    const currentIds = currentCapture.rows.map(rowIdentity);
    if (new Set(previousIds).size !== previousIds.length ||
        new Set(currentIds).size !== currentIds.length) {
      throw new Error("rolling reviewed page contains duplicate row identity");
    }
    const previousEvidence = new Map(previousCapture.rows.map((row) =>
      [rowIdentity(row), rowEvidence(row)]));
    for (const row of currentCapture.rows) {
      const key = rowIdentity(row);
      if (previousEvidence.has(key) && previousEvidence.get(key) !== rowEvidence(row)) {
        throw new Error("rolling reviewed page has conflicting overlap evidence");
      }
    }
    let overlap = 0;
    for (let candidate = Math.min(previousIds.length, currentIds.length);
      candidate >= 1; candidate -= 1) {
      const previousStart = previousIds.length - candidate;
      if (previousIds.slice(previousStart).every((key, index) => key === currentIds[index])) {
        overlap = candidate;
        break;
      }
    }
    if (overlap === 0) {
      throw new Error("rolling reviewed page lacks one contiguous overlap");
    }
    const mergedRows = previousCapture.rows
      .slice(0, previousCapture.rows.length - overlap)
      .concat(currentCapture.rows);
    const mergedIds = mergedRows.map(rowIdentity);
    if (new Set(mergedIds).size !== mergedIds.length) {
      throw new Error("rolling reviewed page lacks one contiguous overlap");
    }
    return {
      ...currentCapture,
      completeness: {
        ...currentCapture.completeness,
        row_count: mergedRows.length,
      },
      rows: mergedRows,
    };
  }

  async function captureInitialPageWithRefresh({
    captureStableFirstPage,
    refreshLatestFirst,
    isRefreshableInitialError,
    validateCaptureTiming,
  }) {
    let initialPage = null;
    try {
      initialPage = await captureStableFirstPage(null);
      validateCaptureTiming(initialPage.capture);
      return initialPage;
    } catch (initialError) {
      if (!isRefreshableInitialError(initialError)) throw initialError;
      await refreshLatestFirst();
      const refreshed = await captureStableFirstPage(initialPage?.rowsetHash ?? null);
      validateCaptureTiming(refreshed.capture);
      return refreshed;
    }
  }

  async function captureReviewedInitializationPage({
    captureAtomicPage,
    refreshLatestFirst,
    isRefreshableError,
    validateCaptureTiming,
    deadline,
  }) {
    function initializationError(error) {
      if (String(error?.message ?? error) !==
          "atomic reviewed page contains no reviewed rows") return error;
      const mapped = new Error("Eastmoney page ? did not become stable");
      mapped.cause = error;
      return mapped;
    }
    const captureAndValidate = async () => {
      try {
        deadline.throwIfExpired();
        const reviewed = await deadline.run(captureAtomicPage);
        await deadline.run(() => validateCaptureTiming(reviewed.capture));
        return reviewed;
      } catch (error) {
        throw initializationError(error);
      }
    };
    try {
      return await captureAndValidate();
    } catch (initialError) {
      if (!isRefreshableError(initialError)) throw initialError;
      await deadline.run(() => refreshLatestFirst(deadline));
      return await captureAndValidate();
    }
  }

  async function recoverReviewedControlMismatch({
    error,
    isRecoverableError,
    refreshLatestFirst,
    captureAtomicPage,
    validateCaptureTiming,
    deadline,
  }) {
    if (!isRecoverableError(error)) throw error;
    deadline.throwIfExpired();
    await deadline.run(() => refreshLatestFirst(deadline));
    const reviewed = await deadline.run(captureAtomicPage);
    await deadline.run(() => validateCaptureTiming(reviewed.capture));
    return reviewed;
  }

  function createReviewedControlRecoveryCoordinator({
    requestRecovery,
    requestImmediateHeartbeat,
    scheduleTask = (callback) => queueMicrotask(callback),
  }) {
    const exactMismatch =
      "Eastmoney time-sales DOM order disagrees with its reviewed control";
    let queued = false;
    let running = false;
    let scheduled = false;
    let started = false;
    let cancelBeforeStart = false;
    let recoverySucceeded = false;

    function scheduleFlush() {
      if (scheduled || running || !queued) return;
      scheduled = true;
      scheduleTask(() => {
        scheduled = false;
        void flush();
      });
    }

    async function flush() {
      if (running || !queued) return;
      queued = false;
      running = true;
      started = false;
      cancelBeforeStart = false;
      recoverySucceeded = false;
      try {
        // The regular single-flight Promise resolves to its final trailing run,
        // not necessarily to the dedicated recovery result. The dedicated
        // branch therefore reports through completeRecovery() below.
        await requestRecovery();
        if (started && recoverySucceeded) {
          // This is deliberately a raw heartbeat request. Its result is not
          // fed back into this coordinator, so a persistent mismatch cannot
          // recursively spin without a new external heartbeat.
          await requestImmediateHeartbeat();
        }
      } catch (_error) {
        // scanOnce owns diagnostics and failure-budget accounting. The
        // coordinator only releases its latch so a later external heartbeat
        // can request a fresh bounded recovery.
      } finally {
        running = false;
        started = false;
        cancelBeforeStart = false;
        recoverySucceeded = false;
      }
    }

    return {
      observeHeartbeatResult(result) {
        if (result?.ok === true) {
          if (!running) queued = false;
          else if (!started) cancelBeforeStart = true;
          return;
        }
        if (result?.reason !== exactMismatch || queued || running) return;
        queued = true;
        scheduleFlush();
      },
      beginRecovery() {
        if (!running || started || cancelBeforeStart) return false;
        started = true;
        return true;
      },
      completeRecovery(success) {
        if (!running || !started) return false;
        recoverySucceeded = success === true;
        return true;
      },
      snapshot() {
        return { queued, running, scheduled, started, cancelBeforeStart, recoverySucceeded };
      },
    };
  }

  function reviewedControlRecoveryWorstCaseMs() {
    return Object.values(REVIEWED_CONTROL_RECOVERY_BUDGET_MS)
      .reduce((total, milliseconds) => total + milliseconds, 0);
  }

  async function captureSourceObservation({
    refreshLatestFirst,
    captureStableFirstPage,
    validateObservationTiming,
  }) {
    await refreshLatestFirst();
    const stable = await captureStableFirstPage(1);
    validateObservationTiming(stable.capture);
    return stable;
  }

  async function captureClockBoundSourceObservation({
    captureStableFirstPage,
    sourcePageObservedAtUs,
    validateObservationTiming,
  }) {
    const stable = await captureStableFirstPage(1);
    const capture = {
      ...stable.capture,
      source_page_observed_at_us: sourcePageObservedAtUs(stable.capture),
    };
    validateObservationTiming(capture);
    return { ...stable, capture };
  }

  async function captureServerClockBoundSourceObservation({
    captureStableFirstPage,
    sourceServerObservedAtUs,
    capturedAtUs,
    waitForLocalClock = null,
    validateObservationTiming,
    deadline = null,
  }) {
    deadline?.throwIfExpired();
    const stable = await captureStableFirstPage(1);
    deadline?.throwIfExpired();
    const sourceServerClock = deadline
      ? await deadline.run(sourceServerObservedAtUs)
      : await sourceServerObservedAtUs();
    deadline?.throwIfExpired();
    let captureClock = capturedAtUs();
    while (captureClock < sourceServerClock) {
      if (typeof waitForLocalClock !== "function") break;
      const remainingUs = sourceServerClock - captureClock;
      if (deadline) {
        await deadline.run(() => waitForLocalClock(sourceServerClock, remainingUs));
      } else {
        await waitForLocalClock(sourceServerClock, remainingUs);
      }
      deadline?.throwIfExpired();
      const nextCaptureClock = capturedAtUs();
      if (nextCaptureClock <= captureClock) {
        throw new Error("local capture clock did not advance toward the reviewed HTTPS clock");
      }
      captureClock = nextCaptureClock;
    }
    const capture = {
      ...stable.capture,
      captured_at_us: captureClock,
      source_server_observed_at_us: sourceServerClock,
      source_clock_origin: "EASTMONEY_HTTPS_DATE_HEADER",
    };
    validateObservationTiming(capture);
    return { ...stable, capture };
  }

  async function deliverStatusOnlyObservationAndClassifyTradeCoverage({
    capture,
    deliver,
    validateTradeCoverage,
  }) {
    const response = await deliver();
    if (response?.ok !== true) return response;
    try {
      validateTradeCoverage(capture);
    } catch (error) {
      if (String(error?.message ?? error) === "capture latest row is stale") {
        return { ok: false, reason: "STALE_TRADE_COVERAGE" };
      }
      throw error;
    }
    return response;
  }

  function createDeadline({
    timeoutMs,
    now = () => Date.now(),
    scheduleTimeout = setTimeout,
    cancelTimeout = clearTimeout,
  }) {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs >= 60_000) {
      throw new Error("source observation deadline must be inside the live watermark bound");
    }
    const expiresAt = now() + timeoutMs;
    return {
      expiresAtMs() {
        return expiresAt;
      },
      remainingMs() {
        return Math.max(0, expiresAt - now());
      },
      throwIfExpired() {
        if (now() >= expiresAt) {
          throw new Error("source observation exceeded its reviewed deadline");
        }
      },
      async run(operation) {
        this.throwIfExpired();
        const remaining = this.remainingMs();
        let timeoutHandle;
        const timeout = new Promise((_resolve, reject) => {
          timeoutHandle = scheduleTimeout(() =>
            reject(new Error("source observation exceeded its reviewed deadline")), remaining);
        });
        try {
          return await Promise.race([Promise.resolve().then(operation), timeout]);
        } finally {
          cancelTimeout(timeoutHandle);
        }
      },
    };
  }

  function createUiMutationGuard() {
    let activeMutations = 0;
    let revision = 0;
    return {
      beginMutation() {
        activeMutations += 1;
        revision += 1;
        let finished = false;
        return () => {
          if (finished) return;
          finished = true;
          activeMutations -= 1;
          revision += 1;
        };
      },
      snapshot() {
        return { revision, stable: activeMutations === 0 };
      },
      isStable(snapshot) {
        return Boolean(snapshot?.stable) && activeMutations === 0 &&
          snapshot.revision === revision;
      },
      activeMutations() {
        return activeMutations;
      },
    };
  }

  function createLaneFailureBudget({ maxFailures, cooldownMs, now = () => Date.now() }) {
    if (!Number.isSafeInteger(maxFailures) || maxFailures <= 0 ||
        !Number.isSafeInteger(cooldownMs) || cooldownMs <= 0) {
      throw new Error("lane failure budget must be positive and bounded");
    }
    const lanes = new Map();
    function state(lane) {
      if (!lanes.has(lane)) lanes.set(lane, { failures: 0, cooldownUntil: 0 });
      return lanes.get(lane);
    }
    return {
      canAttempt(lane) {
        const current = state(lane);
        if (current.cooldownUntil === 0) return true;
        if (now() < current.cooldownUntil) return false;
        current.failures = 0;
        current.cooldownUntil = 0;
        return true;
      },
      recordFailure(lane) {
        const current = state(lane);
        current.failures += 1;
        if (current.failures >= maxFailures) current.cooldownUntil = now() + cooldownMs;
        return { ...current };
      },
      recordSuccess(lane) {
        const current = state(lane);
        current.failures = 0;
        current.cooldownUntil = 0;
      },
      snapshot(lane) {
        return { ...state(lane) };
      },
    };
  }

  function createCooldownRetryScheduler({
    failureBudget,
    lane,
    minimumDelayMs,
    now = () => Date.now(),
    scheduleTimeout = setTimeout,
    cancelTimeout = clearTimeout,
  }) {
    if (!failureBudget || !Number.isSafeInteger(minimumDelayMs) || minimumDelayMs <= 0) {
      throw new Error("cooldown retry scheduler requires a bounded delay and failure budget");
    }
    let timer = null;
    let pendingCallback = null;
    function arm() {
      if (timer !== null || pendingCallback === null) return;
      const cooldownUntil = failureBudget.snapshot(lane).cooldownUntil;
      const delayMs = Math.max(minimumDelayMs, cooldownUntil - now(), 0);
      timer = scheduleTimeout(() => {
        timer = null;
        if (pendingCallback === null) return;
        if (!failureBudget.canAttempt(lane)) {
          arm();
          return;
        }
        const callback = pendingCallback;
        pendingCallback = null;
        callback();
      }, delayMs);
    }
    return {
      schedule(callback) {
        pendingCallback = pendingCallback ?? callback;
        arm();
      },
      cancel() {
        pendingCallback = null;
        if (timer !== null) cancelTimeout(timer);
        timer = null;
      },
      pending() {
        return pendingCallback !== null;
      },
    };
  }

  function reviewedControlIsUnchanged(initialControl, currentControl) {
    return Boolean(initialControl) && initialControl === currentControl &&
      initialControl.checked === true && currentControl.checked === true;
  }

  async function cycleLatestFirstControl({
    readControl,
    delay,
    waitForUncheckedEffect = async () => {},
    maxStateAttempts,
    deadline = null,
  }) {
    async function waitFor(expectedChecked) {
      for (let attempt = 0; attempt < maxStateAttempts; attempt += 1) {
        deadline?.throwIfExpired();
        const control = readControl();
        if (control && control.checked === expectedChecked) return control;
        if (deadline) await deadline.run(delay);
        else await delay();
      }
      throw new Error(`Eastmoney latest-first control did not become ${expectedChecked ? "checked" : "unchecked"}`);
    }

    const checked = await waitFor(true);
    checked.click();
    try {
      await waitFor(false);
      if (deadline) await deadline.run(waitForUncheckedEffect);
      else await waitForUncheckedEffect();
    } finally {
      const current = readControl();
      if (current && current.checked === false) current.click();
    }
    await waitFor(true);
  }

  async function waitForReviewedRowsetEffect({
    previousRowsetHash,
    readRowsetHash,
    delay,
    maxAttempts,
    deadline = null,
  }) {
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      deadline?.throwIfExpired();
      const currentRowsetHash = deadline
        ? await deadline.run(readRowsetHash)
        : await readRowsetHash();
      if (currentRowsetHash !== previousRowsetHash) return;
      if (attempt + 1 < maxAttempts) {
        if (deadline) await deadline.run(delay);
        else await delay();
      }
    }
    throw new Error("Eastmoney latest-first cycle did not produce a reviewed rowset effect");
  }

  function createSingleFlightRunner(run, {
    mergeReason = (_queued, incoming) => incoming,
    onIdle = () => {},
    maxRunsBeforeYield = Number.POSITIVE_INFINITY,
    yieldBetweenBatches = async () => {},
  } = {}) {
    if (!(maxRunsBeforeYield === Number.POSITIVE_INFINITY ||
        (Number.isSafeInteger(maxRunsBeforeYield) && maxRunsBeforeYield > 0))) {
      throw new Error("single-flight batch bound must be positive");
    }
    let inFlight = null;
    let queuedReason = null;
    return {
      request(reason) {
        queuedReason = queuedReason === null ? reason : mergeReason(queuedReason, reason);
        if (inFlight) return inFlight;
        // Defer execution by one microtask so a synchronous request made by
        // run() observes this flight as active instead of starting a nested
        // unbounded flight before the assignment below completes.
        inFlight = Promise.resolve().then(async () => {
          let result;
          let batchRuns = 0;
          while (queuedReason !== null) {
            const nextReason = queuedReason;
            queuedReason = null;
            result = await run(nextReason);
            batchRuns += 1;
            if (queuedReason !== null && batchRuns >= maxRunsBeforeYield) {
              await yieldBetweenBatches();
              batchRuns = 0;
            }
          }
          return result;
        }).finally(() => {
          inFlight = null;
          onIdle();
        });
        return inFlight;
      },
      running() {
        return inFlight !== null;
      },
    };
  }

  function createIndependentHeartbeatRouter(run, options = {}) {
    const heartbeatShouldCatchUp = options.heartbeatShouldCatchUp ?? ((result) =>
      result?.ok === false && (
        result?.reason === "WAITING_FOR_STABLE_REVIEWED_UI" ||
        result?.reason === "REVIEWED_UI_CHANGED_DURING_HEARTBEAT"
      ));
    let regularGeneration = 0;
    let catchUpRequested = false;
    let regular = null;
    const heartbeat = createSingleFlightRunner(async (reason) => {
      const generationAtStart = regularGeneration;
      const regularWasRunning = regular?.running() ?? false;
      const result = await run(reason);
      if (result?.ok === true) catchUpRequested = false;
      const overlappedRegular = regularWasRunning || regularGeneration !== generationAtStart;
      if (heartbeatShouldCatchUp(result) && overlappedRegular) {
        if (regular?.running()) catchUpRequested = true;
        else void heartbeat.request("heartbeat");
      }
      return result;
    });
    regular = createSingleFlightRunner(async (reason) => {
      regularGeneration += 1;
      return await run(reason);
    }, {
      ...options,
      maxRunsBeforeYield: options.maxRegularRunsBeforeHeartbeat ?? 2,
      async yieldBetweenBatches() {
        catchUpRequested = false;
        await heartbeat.request("heartbeat");
      },
      onIdle() {
        options.onRegularIdle?.();
        if (!catchUpRequested) return;
        catchUpRequested = false;
        void heartbeat.request("heartbeat");
      },
    });
    return {
      request(reason) {
        return reason === "heartbeat" ? heartbeat.request(reason) : regular.request(reason);
      },
      regularRunning() {
        return regular.running();
      },
    };
  }

  function shouldDeliverCapture(reason, captureHash, lastDeliveredRowsetHash) {
    return captureHash !== lastDeliveredRowsetHash ||
      reason === "manual";
  }

  function shouldRecoverLiveOverlap(reason, error) {
    return reason !== "heartbeat" &&
      String(error?.message ?? error) ===
        "live capture has no overlap with the prior durable watermark";
  }

  function installSourceHeartbeat(requestScan, {
    intervalMs,
    scheduleEvery = setInterval,
  }) {
    if (!Number.isSafeInteger(intervalMs) || intervalMs <= 0 || intervalMs > 60_000) {
      throw new Error("source heartbeat interval must be within the live watermark bound");
    }
    return scheduleEvery(() => requestScan("heartbeat"), intervalMs);
  }

  function createRetriableInitializer(initialize, {
    scheduleRetry,
    onError = async () => {},
    isInitialized = () => false,
  }) {
    let inFlight = null;
    let retryScheduled = false;

    function request() {
      if (inFlight) return inFlight;
      if (isInitialized()) {
        retryScheduled = false;
        return Promise.resolve({ ok: true, reason: "ALREADY_INITIALIZED" });
      }
      inFlight = (async () => {
        try {
          const result = await initialize();
          retryScheduled = false;
          return result;
        } catch (error) {
          try {
            await onError(error);
          } catch (_reportError) {
            // Reporting must never suppress the bounded initialization retry.
          }
          if (!retryScheduled) {
            retryScheduled = true;
            scheduleRetry(() => {
              if (!retryScheduled) return;
              retryScheduled = false;
              void request();
            });
          }
          return { ok: false, reason: String(error?.message ?? error) };
        }
      })().finally(() => {
        inFlight = null;
      });
      return inFlight;
    }

    return { request };
  }

  function createInitializationErrorTracker() {
    let generation = 0;
    let error = null;
    return {
      begin() {
        generation += 1;
        return generation;
      },
      fail(token, value) {
        if (token !== generation) return false;
        error = String(value?.message ?? value);
        return true;
      },
      succeed(token) {
        if (token !== generation) return false;
        error = null;
        return true;
      },
      current() {
        return error;
      },
    };
  }

  async function completeProvisionalInitialization({
    markInitialized,
    startObserving,
    stopObserving,
    finish,
  }) {
    markInitialized(true);
    try {
      startObserving();
      return await finish();
    } catch (error) {
      try {
        stopObserving();
      } catch (_disconnectError) {
        // The original initialization failure is authoritative; readiness must
        // still roll back even when a replaced DOM observer cannot disconnect.
      }
      markInitialized(false);
      throw error;
    }
  }

  function hasCurrentSessionEvidence(state, sessionDate) {
    return state?.complete_session_date === sessionDate ||
      state?.resume_boundary_session_date === sessionDate;
  }

  async function recoverLiveOverlap({
    state,
    sessionDate,
    establishDiscontinuityBoundary,
    establishResumeBoundary,
  }) {
    if (typeof sessionDate === "string" &&
        state?.complete_session_date === sessionDate) {
      if (typeof establishDiscontinuityBoundary !== "function") {
        throw new Error("same-day complete overlap recovery requires a discontinuity boundary");
      }
      const boundary = await establishDiscontinuityBoundary();
      return { mode: "DISCONTINUITY_BOUNDARY", boundary };
    }
    const boundary = await establishResumeBoundary();
    return { mode: "RESUME_BOUNDARY", boundary };
  }

  function historyErrorAllowsResumeBoundary(error) {
    const message = String(error?.message ?? error);
    return /^Eastmoney page (?:\?|\d+) did not become stable$/.test(message) ||
      message === "capture latest row is stale" ||
      message === "Eastmoney time-sales DOM order disagrees with its reviewed control" ||
      /^Eastmoney history navigation did not reach page \d+$/.test(message) ||
      message === "Eastmoney history crawl exhausted its restart bound" ||
      message === "history assembly must capture every history page exactly once" ||
      message === "final live page has no overlap with the initial live page";
  }

  async function routeCollectorInitialization({
    state,
    currentPage,
    navigateHome,
    captureStableFirstPage,
    crawlSessionHistory,
    establishResumeBoundary,
    initializationPolicy = "FULL_HISTORY_OR_REVIEWED_FALLBACK_V1",
  }) {
    if (hasCurrentSessionEvidence(state, currentPage.capture.session_date)) {
      if (currentPage.capture.completeness.page_index !== 1) {
        await navigateHome();
        currentPage = await captureStableFirstPage(1, currentPage.rowsetHash);
      }
      return { mode: "CURRENT_SESSION", currentPage };
    }
    if (initializationPolicy === "ISOLATED_EMPTY_STATE_RESUME_BOUNDARY_V1") {
      const boundary = await establishResumeBoundary();
      return { mode: "RESUME_BOUNDARY", currentPage, boundary };
    }
    if (initializationPolicy !== "FULL_HISTORY_OR_REVIEWED_FALLBACK_V1") {
      throw new Error("collector initialization policy is invalid");
    }
    try {
      await crawlSessionHistory();
      return { mode: "HISTORY_REBUILT", currentPage };
    } catch (historyError) {
      if (!historyErrorAllowsResumeBoundary(historyError)) throw historyError;
      const boundary = await establishResumeBoundary();
      return { mode: "RESUME_BOUNDARY", currentPage, boundary };
    }
  }

  async function refreshStaleFirstPage({
    staleCapture,
    rowsetHash,
    refreshLatestFirst,
    captureStableFirstPage,
    validateCaptureTiming,
    retryDelay = async () => {},
    maxRefreshAttempts = 3,
    deadline = null,
  }) {
    deadline?.throwIfExpired();
    const forbiddenRowsetHash = deadline
      ? await deadline.run(() => rowsetHash(staleCapture))
      : await rowsetHash(staleCapture);
    let lastError = null;
    for (let attempt = 0; attempt < maxRefreshAttempts; attempt += 1) {
      try {
        deadline?.throwIfExpired();
        if (deadline) await deadline.run(() => refreshLatestFirst(deadline));
        else await refreshLatestFirst();
        const refreshed = deadline
          ? await deadline.run(() => captureStableFirstPage(forbiddenRowsetHash, deadline))
          : await captureStableFirstPage(forbiddenRowsetHash);
        if (refreshed.rowsetHash === forbiddenRowsetHash) {
          throw new Error("Eastmoney latest-first refresh reused the stale rowset");
        }
        validateCaptureTiming(refreshed.capture);
        return refreshed;
      } catch (error) {
        lastError = error;
        if (attempt + 1 < maxRefreshAttempts) {
          if (deadline) await deadline.run(retryDelay);
          else await retryDelay();
        }
      }
    }
    throw lastError ?? new Error("Eastmoney latest-first refresh exhausted its reviewed attempts");
  }

  return {
    captureInitialPageWithRefresh,
    captureReviewedInitializationPage,
    captureAtomicReviewedPage,
    captureClockBoundSourceObservation,
    captureServerClockBoundSourceObservation,
    captureSourceObservation,
    captureStablePage,
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
    cycleLatestFirstControl,
    installSourceHeartbeat,
    mergeRollingPageCaptures,
    hasCurrentSessionEvidence,
    historyErrorAllowsResumeBoundary,
    routeCollectorInitialization,
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
  };
});
