(function initializeGridEdgePageStability(root, factory) {
  const api = factory();
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  root.GridEdgePageStability = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function pageStabilityFactory() {
  "use strict";

  async function readCaptureWithRetry({
    readCapture,
    isRetriableError,
    delay,
    maxAttempts,
  }) {
    let lastError = null;
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      try {
        return await readCapture();
      } catch (error) {
        if (!isRetriableError(error)) throw error;
        lastError = error;
        if (attempt + 1 < maxAttempts) await delay();
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
  }) {
    let previousHash = null;
    let previousRowsetHash = null;
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const capture = await readCapture();
      const pageIndex = capture.completeness.page_index;
      const pageCount = capture.completeness.page_count;
      if (capture.rows.length > 0 && Number.isSafeInteger(pageIndex) &&
          Number.isSafeInteger(pageCount) && pageIndex >= 1 && pageCount >= pageIndex &&
          (expectedPageIndex === null || pageIndex === expectedPageIndex)) {
        const hash = await stableCaptureHash(capture);
        const currentRowsetHash = await rowsetHash(capture);
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
      await delay();
    }
    throw new Error(`Eastmoney page ${expectedPageIndex ?? "?"} did not become stable`);
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

  async function cycleLatestFirstControl({
    readControl,
    delay,
    waitForUncheckedEffect = async () => {},
    maxStateAttempts,
  }) {
    async function waitFor(expectedChecked) {
      for (let attempt = 0; attempt < maxStateAttempts; attempt += 1) {
        const control = readControl();
        if (control && control.checked === expectedChecked) return control;
        await delay();
      }
      throw new Error(`Eastmoney latest-first control did not become ${expectedChecked ? "checked" : "unchecked"}`);
    }

    const checked = await waitFor(true);
    checked.click();
    await waitFor(false);
    await waitForUncheckedEffect();
    const unchecked = await waitFor(false);
    unchecked.click();
    await waitFor(true);
  }

  function createSingleFlightRunner(run, { mergeReason = (_queued, incoming) => incoming } = {}) {
    let inFlight = null;
    let queuedReason = null;
    return {
      request(reason) {
        queuedReason = queuedReason === null ? reason : mergeReason(queuedReason, reason);
        if (inFlight) return inFlight;
        inFlight = (async () => {
          let result;
          while (queuedReason !== null) {
            const nextReason = queuedReason;
            queuedReason = null;
            result = await run(nextReason);
          }
          return result;
        })().finally(() => {
          inFlight = null;
        });
        return inFlight;
      },
    };
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

  async function refreshStaleFirstPage({
    staleCapture,
    rowsetHash,
    refreshLatestFirst,
    captureStableFirstPage,
    validateCaptureTiming,
    retryDelay = async () => {},
    maxRefreshAttempts = 3,
  }) {
    const forbiddenRowsetHash = await rowsetHash(staleCapture);
    let lastError = null;
    for (let attempt = 0; attempt < maxRefreshAttempts; attempt += 1) {
      try {
        await refreshLatestFirst();
        const refreshed = await captureStableFirstPage(forbiddenRowsetHash);
        if (refreshed.rowsetHash === forbiddenRowsetHash) {
          throw new Error("Eastmoney latest-first refresh reused the stale rowset");
        }
        validateCaptureTiming(refreshed.capture);
        return refreshed;
      } catch (error) {
        lastError = error;
        if (attempt + 1 < maxRefreshAttempts) await retryDelay();
      }
    }
    throw lastError ?? new Error("Eastmoney latest-first refresh exhausted its reviewed attempts");
  }

  return {
    captureInitialPageWithRefresh,
    captureStablePage,
    completeProvisionalInitialization,
    createRetriableInitializer,
    createSingleFlightRunner,
    cycleLatestFirstControl,
    readCaptureWithRetry,
    refreshStaleFirstPage,
  };
});
