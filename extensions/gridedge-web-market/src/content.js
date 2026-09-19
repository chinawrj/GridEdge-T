(function startGridEdgeContentCollector() {
  "use strict";

  const core = globalThis.GridEdgeMarket;
  const provider = core?.providers?.eastmoney;
  const pageStability = globalThis.GridEdgePageStability;
  if (!core || !provider || !pageStability || !provider.matches(location.href) ||
      location.pathname !== "/f1.html") return;

  const MAX_HISTORY_PAGES = 200;
  const MAX_HISTORY_RESTARTS = 3;
  const MAX_STABILITY_ATTEMPTS = 180;
  const COMPLETE_HISTORY_BRIDGE_DEADLINE_MS = 40_000;
  const COMPLETE_HISTORY_BRIDGE_STABILITY_DELAY_MS = 100;
  const COMPLETE_HISTORY_BRIDGE_STABILITY_ATTEMPTS = 30;
  const MAX_SCAN_ERROR_RETRIES = 3;
  const SCAN_ERROR_RETRY_MS = 1500;
  // Keep three opportunities plus DB-ACK scheduling headroom inside the
  // 45-second operating target and well inside the 60-second hard gate.
  const SCAN_HEARTBEAT_MS = 12_000;
  const SOURCE_OBSERVATION_POLICY = "REVIEWED_EASTMONEY_HTTPS_DATE_LATEST_FIRST_V3";
  const SOURCE_OBSERVATION_DEADLINE_MS = 12_000;
  // A DOM-order mismatch is only a trigger for the dedicated regular-lane UI
  // repair. Bound its detection separately so alarm phase + detection + UI
  // repair + immediate observation + DB ACK remains below the 60s hard gate.
  const REVIEWED_CONTROL_MISMATCH_DEADLINE_MS = 4_000;
  // The router yields to heartbeat after every two regular scans, including a
  // continuous MutationObserver stream. Two bounded scans (16s) plus a full
  // observation (12s) and DB ACK (15s) stay inside the 45-second operating
  // target and prevent reviewed UI recovery from starving source liveness.
  const REGULAR_SCAN_DEADLINE_MS = 8_000;
  const MAX_REVIEWED_SNAPSHOT_ATTEMPTS = 60;
  let initialized = false;
  const initializationErrors = pageStability.createInitializationErrorTracker();
  let lastInitializationError = null;
  let scheduled = false;
  let lastObservedRowsetHash = null;
  let lastDeliveredRowsetHash = null;
  let lastObservedCapture = null;
  const uiMutationGuard = pageStability.createUiMutationGuard();
  const scanFailureBudget = pageStability.createLaneFailureBudget({
    maxFailures: MAX_SCAN_ERROR_RETRIES,
    cooldownMs: 45_000,
  });
  const regularRetryScheduler = pageStability.createCooldownRetryScheduler({
    failureBudget: scanFailureBudget,
    lane: "regular",
    minimumDelayMs: SCAN_ERROR_RETRY_MS,
  });

  const delay = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

  function cellEvidence(cell) {
    return {
      text: core.normalizeText(cell.textContent),
      class_name: core.normalizeText(cell.className),
    };
  }

  function documentSnapshot() {
    const tables = Array.from(document.querySelectorAll("table")).map((table) =>
      Array.from(table.querySelectorAll("tr")).map((row) =>
        Array.from(row.children).map(cellEvidence),
      ),
    );
    return {
      url: location.href,
      title: document.title,
      bodyText: document.body?.innerText ?? "",
      tables,
      capturedAtUs: core.unixMicrosNow(),
      sessionDate: core.shanghaiDate(),
      rowOrder: reviewedRowOrder(),
    };
  }

  function reviewedRowOrder() {
    const checkbox = latestFirstCheckbox();
    if (!checkbox) throw new Error("Eastmoney reviewed row-order control is missing");
    return checkbox.checked ? "LATEST_FIRST" : "EARLIEST_FIRST";
  }

  function latestFirstCheckbox() {
    return Array.from(document.querySelectorAll('input[type="checkbox"]')).find((input) =>
      Array.from(input.labels ?? []).some((label) =>
        core.normalizeText(label.textContent) === "倒序",
      ),
    );
  }

  function ensureLatestFirst() {
    const checkbox = latestFirstCheckbox();
    if (!checkbox) return false;
    if (!checkbox.checked) {
      const finishMutation = uiMutationGuard.beginMutation();
      try {
        checkbox.click();
      } finally {
        finishMutation();
      }
      return false;
    }
    return true;
  }

  function isRetriableReviewedControlError(error) {
    return String(error?.message ?? error) ===
      "Eastmoney time-sales DOM order disagrees with its reviewed control";
  }

  function isRefreshableInitialError(error) {
    const message = String(error?.message ?? error);
    return message === "Eastmoney page ? did not become stable" ||
      message === "capture latest row is stale" ||
      message === "Eastmoney time-sales page has no reviewed A-share sale rows" ||
      message === "Eastmoney latest-first cycle did not produce a reviewed rowset effect" ||
      message === "Eastmoney time-sales DOM order disagrees with its reviewed control";
  }

  async function readReviewedSnapshot(deadline = null, preserveMismatchAtDeadline = false) {
    return await pageStability.readCaptureWithRetry({
      readCapture: async () => provider.parseSnapshot(documentSnapshot()),
      isRetriableError: isRetriableReviewedControlError,
      delay: async () => await delay(100),
      maxAttempts: MAX_REVIEWED_SNAPSHOT_ATTEMPTS,
      deadline,
      preserveLastRetriableOnDeadline: preserveMismatchAtDeadline,
    });
  }

  async function refreshLatestFirst(deadline = pageStability.createDeadline({
    timeoutMs: SOURCE_OBSERVATION_DEADLINE_MS,
  })) {
    const finishMutation = uiMutationGuard.beginMutation();
    try {
    let previousRowsetHash = null;
    try {
      const previousCapture = provider.parseSnapshot(documentSnapshot());
      if (previousCapture.rows.length > 0) previousRowsetHash = await rowsetHash(previousCapture);
    } catch (_error) {
      // An empty initial table has no rowset; wait for any reviewed rows below.
    }
      await pageStability.cycleLatestFirstControl({
      readControl: latestFirstCheckbox,
      delay: async () => await delay(100),
      waitForUncheckedEffect: async () => {
        await pageStability.waitForReviewedRowsetEffect({
          previousRowsetHash,
          readRowsetHash: async () => {
            try {
              const capture = provider.parseSnapshot(documentSnapshot());
              if (capture.rows.length === 0) return previousRowsetHash;
              return await rowsetHash(capture);
            } catch (_error) {
              // Eastmoney can replace the pagination token before its rows.
              return previousRowsetHash;
            }
          },
          delay: async () => await delay(250),
          maxAttempts: 20,
          deadline,
        });
      },
      maxStateAttempts: 60,
      deadline,
      });
    } finally {
      finishMutation();
    }
  }

  function stableCaptureValue(capture) {
    return {
      ...capture,
      captured_at_us: 0,
      source_page_observed_at_us: 0,
      source_server_observed_at_us: 0,
    };
  }

  async function rowsetHash(capture) {
    return await core.sha256Hex(core.canonicalJson(capture.rows.map((row) => ({
      source_row_key: row.source_row_key,
      source_trade_time: row.source_trade_time,
      price: row.price,
      quantity: row.quantity,
      quantity_hands: row.quantity_hands,
      unit: row.unit,
      side: row.side,
      occurrence: row.occurrence,
      source_same_second_ordinal: row.source_same_second_ordinal,
    }))));
  }

  async function stablePageCapture(
    expectedPageIndex = null,
    forbiddenRowsetHash = null,
    maxAttempts = MAX_STABILITY_ATTEMPTS,
    deadline = null,
    stabilityDelayMs = 500,
  ) {
    return await pageStability.captureStablePage({
      readCapture: async () => await readReviewedSnapshot(deadline),
      stableCaptureHash: async (capture) =>
        await core.sha256Hex(core.canonicalJson(stableCaptureValue(capture))),
      rowsetHash,
      delay: async () => await delay(stabilityDelayMs),
      expectedPageIndex,
      forbiddenRowsetHash,
      maxAttempts,
      deadline,
    });
  }

  async function atomicReviewedPageCapture(
    expectedPageIndex = null,
    forbiddenRowsetHash = null,
    deadline = null,
    preserveMismatchAtDeadline = false,
  ) {
    return await pageStability.captureAtomicReviewedPage({
      readCapture: async () =>
        await readReviewedSnapshot(deadline, preserveMismatchAtDeadline),
      stableCaptureHash: async (capture) =>
        await core.sha256Hex(core.canonicalJson(stableCaptureValue(capture))),
      rowsetHash,
      expectedPageIndex,
      forbiddenRowsetHash,
      deadline,
    });
  }

  function historyLink(label) {
    return Array.from(document.querySelectorAll("a")).find((link) =>
      core.normalizeText(link.textContent) === label && link.getClientRects().length > 0,
    );
  }

  async function navigateHistory(label, expectedPageIndex, deadline = null) {
    const finishMutation = uiMutationGuard.beginMutation();
    try {
    const link = historyLink(label);
    if (!link) throw new Error(`Eastmoney history navigation link is missing: ${label}`);
    link.click();
    for (let attempt = 0; attempt < 20; attempt += 1) {
      if (deadline) await deadline.run(() => delay(250));
      else await delay(250);
      deadline?.throwIfExpired();
      try {
        const capture = provider.parseSnapshot(documentSnapshot());
        if (capture.completeness.page_index === expectedPageIndex) return;
      } catch (_error) {
        // The table can be transiently incomplete while Eastmoney redraws it.
      }
    }
      throw new Error(`Eastmoney history navigation did not reach page ${expectedPageIndex}`);
    } finally {
      finishMutation();
    }
  }

  async function deliverCapture(
    capture,
    rowsetHash,
    sourceObservationPolicy = null,
    requireCompleteHistoryBridge = false,
  ) {
    if (sourceObservationPolicy === null) core.validateCaptureTiming(capture);
    else core.validateSourceObservationTiming(capture);
    const captureSha256 = await core.sha256Hex(core.canonicalJson(capture));
    const response = await chrome.runtime.sendMessage({
      type: requireCompleteHistoryBridge
        ? "GRIDEDGE_COMPLETE_HISTORY_BRIDGE"
        : "GRIDEDGE_CAPTURE_BATCH",
      capture,
      capture_sha256: captureSha256,
      rowset_hash: rowsetHash,
      source_observation_policy: sourceObservationPolicy,
    });
    if (!response?.ok) throw new Error(response?.error ?? response?.reason ?? "capture delivery failed");
    return response;
  }

  async function crawlSessionHistory({ requireCompleteHistoryBridge = false } = {}) {
    const historyDeadline = requireCompleteHistoryBridge
      ? pageStability.createDeadline({ timeoutMs: COMPLETE_HISTORY_BRIDGE_DEADLINE_MS })
      : null;
    const historyAttempts = requireCompleteHistoryBridge
      ? COMPLETE_HISTORY_BRIDGE_STABILITY_ATTEMPTS
      : MAX_STABILITY_ATTEMPTS;
    const historyDelayMs = requireCompleteHistoryBridge
      ? COMPLETE_HISTORY_BRIDGE_STABILITY_DELAY_MS
      : 500;
    for (let restart = 0; restart < MAX_HISTORY_RESTARTS; restart += 1) {
      const current = await readReviewedSnapshot(historyDeadline);
      let previousPageRowsetHash = null;
      if (current.completeness.page_index !== 1) {
        previousPageRowsetHash = historyDeadline
          ? await historyDeadline.run(() => rowsetHash(current))
          : await rowsetHash(current);
        await navigateHistory("首页", 1, historyDeadline);
      }
      const pageCaptures = [];
      const pageHashes = [];
      let expectedPageCount = null;
      for (let pageIndex = 1; pageIndex <= (expectedPageCount ?? 1); pageIndex += 1) {
        const page = await stablePageCapture(
          pageIndex,
          previousPageRowsetHash,
          historyAttempts,
          historyDeadline,
          historyDelayMs,
        );
        expectedPageCount = Math.max(expectedPageCount ?? 0, page.capture.completeness.page_count);
        if (expectedPageCount > MAX_HISTORY_PAGES) throw new Error("Eastmoney history exceeds the reviewed page bound");
        pageCaptures.push(page.capture);
        pageHashes.push(page.hash);
        previousPageRowsetHash = page.rowsetHash;
        if (pageIndex < expectedPageCount) {
          await navigateHistory("下一页", pageIndex + 1, historyDeadline);
        }
      }
      let finalFirstPage;
      if (expectedPageCount === 1) {
        finalFirstPage = await stablePageCapture(
          1, null, historyAttempts, historyDeadline, historyDelayMs,
        );
      } else {
        await navigateHistory("首页", 1, historyDeadline);
        finalFirstPage = await stablePageCapture(
          1, previousPageRowsetHash, historyAttempts, historyDeadline, historyDelayMs,
        );
      }
      try {
        const completeCapture = provider.assembleSessionHistory(
          pageCaptures,
          finalFirstPage.capture,
          pageHashes,
          finalFirstPage.hash,
        );
        await deliverCapture(
          completeCapture,
          finalFirstPage.hash,
          null,
          requireCompleteHistoryBridge,
        );
        lastObservedRowsetHash = finalFirstPage.hash;
        lastDeliveredRowsetHash = finalFirstPage.hash;
        lastObservedCapture = null;
        return completeCapture;
      } catch (error) {
        if (restart + 1 === MAX_HISTORY_RESTARTS) throw error;
      }
    }
    throw new Error("Eastmoney history crawl exhausted its restart bound");
  }

  async function establishResumeBoundary(deadline = pageStability.createDeadline({
      timeoutMs: SOURCE_OBSERVATION_DEADLINE_MS,
    })) {
    let current = await readReviewedSnapshot(deadline);
    let forbiddenRowsetHash = null;
    if (current.completeness.page_index !== 1) {
      forbiddenRowsetHash = await deadline.run(() => rowsetHash(current));
      await navigateHistory("首页", 1, deadline);
    }
    const stable = await atomicReviewedPageCapture(1, forbiddenRowsetHash, deadline);
    core.validateCaptureTiming(stable.capture);
    const captureSha256 = await deadline.run(() =>
      core.sha256Hex(core.canonicalJson(stable.capture)));
    const response = await deadline.run(() => chrome.runtime.sendMessage({
      type: "GRIDEDGE_RESUME_BOUNDARY",
      capture: stable.capture,
      capture_sha256: captureSha256,
      rowset_hash: stable.rowsetHash,
    }));
    if (!response?.ok) {
      throw new Error(response?.error ?? response?.reason ?? "resume boundary delivery failed");
    }
    lastObservedRowsetHash = stable.rowsetHash;
    lastDeliveredRowsetHash = stable.rowsetHash;
    lastObservedCapture = null;
    return response;
  }

  async function establishDiscontinuityBoundary(deadline = pageStability.createDeadline({
      timeoutMs: SOURCE_OBSERVATION_DEADLINE_MS,
    })) {
    let current = await readReviewedSnapshot(deadline);
    let forbiddenRowsetHash = null;
    if (current.completeness.page_index !== 1) {
      forbiddenRowsetHash = await deadline.run(() => rowsetHash(current));
      await navigateHistory("首页", 1, deadline);
    }
    const stable = await atomicReviewedPageCapture(1, forbiddenRowsetHash, deadline);
    core.validateCaptureTiming(stable.capture);
    const captureSha256 = await deadline.run(() =>
      core.sha256Hex(core.canonicalJson(stable.capture)));
    const response = await deadline.run(() => chrome.runtime.sendMessage({
      type: "GRIDEDGE_DISCONTINUITY_BOUNDARY",
      capture: stable.capture,
      capture_sha256: captureSha256,
      rowset_hash: stable.rowsetHash,
      deadline_at_ms: deadline.expiresAtMs(),
    }));
    if (!response?.ok) {
      throw new Error(response?.error ?? response?.reason ?? "discontinuity boundary delivery failed");
    }
    lastObservedRowsetHash = stable.rowsetHash;
    lastDeliveredRowsetHash = stable.rowsetHash;
    lastObservedCapture = null;
    return response;
  }

  async function initializeCollector() {
    const initializationGeneration = initializationErrors.begin();
    try {
      while (!ensureLatestFirst()) await delay(750);
      const deadline = pageStability.createDeadline({
        timeoutMs: SOURCE_OBSERVATION_DEADLINE_MS,
      });
      let currentPage = await pageStability.captureReviewedInitializationPage({
        captureAtomicPage: async () => await atomicReviewedPageCapture(null, null, deadline),
        refreshLatestFirst,
        isRefreshableError: isRefreshableInitialError,
        validateCaptureTiming: core.validateCaptureTiming,
        deadline,
      });
      const stateResponse = await chrome.runtime.sendMessage({
        type: "GRIDEDGE_GET_CAPTURE_STATE",
        instrument: currentPage.capture.instrument,
      });
      if (!stateResponse?.ok) throw new Error(stateResponse?.error ?? "capture state query failed");
      const initialization = await pageStability.routeCollectorInitialization({
        state: stateResponse.state,
        currentPage,
        navigateHome: async () => await navigateHistory("首页", 1),
        captureStableFirstPage: atomicReviewedPageCapture,
        crawlSessionHistory,
        establishResumeBoundary,
        initializationPolicy: stateResponse.initialization_policy,
      });
      currentPage = initialization.currentPage;
      const result = await pageStability.completeProvisionalInitialization({
        markInitialized(value) {
          initialized = value;
        },
        startObserving() {
          observer.observe(document.documentElement, {
            childList: true,
            characterData: true,
            subtree: true,
          });
        },
        stopObserving() {
          observer.disconnect();
        },
        finish() {
          return requestScan("initial");
        },
      });
      initializationErrors.succeed(initializationGeneration);
      lastInitializationError = initializationErrors.current();
      return result;
    } catch (error) {
      initializationErrors.fail(initializationGeneration, error);
      lastInitializationError = initializationErrors.current();
      throw error;
    }
  }

  const initializationRunner = pageStability.createRetriableInitializer(initializeCollector, {
    isInitialized: () => initialized,
    scheduleRetry(callback) {
      setTimeout(callback, 15_000);
    },
    async onError(error) {
      reportCaptureError(String(error?.message ?? error));
    },
  });

  function requestInitialization() {
    return initializationRunner.request();
  }

  function successfulScan(reason, result) {
    const lane = reason === "heartbeat" ? "heartbeat" : "regular";
    scanFailureBudget.recordSuccess(lane);
    if (lane === "regular") regularRetryScheduler.cancel();
    return result;
  }

  function scheduleRegularRetry(callback) {
    regularRetryScheduler.schedule(callback);
  }

  function reportCaptureError(message) {
    // Reporting is diagnostic, not a market-data or readiness fact. It must
    // never hold either scan lane past its reviewed deadline.
    void Promise.resolve().then(() => chrome.runtime.sendMessage({
      type: "GRIDEDGE_CAPTURE_ERROR",
      provider: "eastmoney",
      page_url: location.href,
      message,
    })).catch(() => {});
  }

  async function scanOnce(reason) {
    const regularDeadline = reason === "heartbeat" ? null : pageStability.createDeadline({
      timeoutMs: REGULAR_SCAN_DEADLINE_MS,
    });
    try {
      if (!initialized) {
        void requestInitialization();
        return {
          ok: false,
          reason: "INITIALIZING_HISTORY",
          initialization_error: lastInitializationError,
        };
      }
      if (reason === "heartbeat") {
        const uiSnapshot = uiMutationGuard.snapshot();
        const reviewedControl = latestFirstCheckbox();
        if (!uiMutationGuard.isStable(uiSnapshot) ||
            !pageStability.reviewedControlIsUnchanged(reviewedControl, reviewedControl)) {
          return successfulScan(reason, { ok: false, reason: "WAITING_FOR_STABLE_REVIEWED_UI" });
        }
        if (reviewedRowOrder() !== "LATEST_FIRST") {
          return successfulScan(reason, { ok: false, reason: "WAITING_FOR_LATEST_FIRST" });
        }
        const deadline = pageStability.createDeadline({
          timeoutMs: SOURCE_OBSERVATION_DEADLINE_MS,
        });
        const reviewedControlMismatchDeadline = pageStability.createDeadline({
          timeoutMs: REVIEWED_CONTROL_MISMATCH_DEADLINE_MS,
        });
        const observed = await pageStability.captureServerClockBoundSourceObservation({
          captureStableFirstPage: async (expectedPageIndex) =>
            await atomicReviewedPageCapture(
              expectedPageIndex,
              null,
              reviewedControlMismatchDeadline,
              true,
            ),
          sourceServerObservedAtUs: async () =>
            await provider.sourceServerObservedAtUs(
              location.href,
              globalThis.fetch,
              Math.max(1, Math.min(5_000, deadline.remainingMs())),
            ),
          capturedAtUs: core.unixMicrosNow,
          waitForLocalClock: async (_sourceClock, remainingUs) => {
            const milliseconds = Math.max(1, Math.ceil(remainingUs / 1000));
            await delay(milliseconds);
          },
          validateObservationTiming: core.validateServerClockBoundSourceObservationTiming,
          deadline,
        });
        if (!uiMutationGuard.isStable(uiSnapshot) ||
            !pageStability.reviewedControlIsUnchanged(reviewedControl, latestFirstCheckbox()) ||
            reviewedRowOrder() !== "LATEST_FIRST") {
          return successfulScan(reason, { ok: false, reason: "REVIEWED_UI_CHANGED_DURING_HEARTBEAT" });
        }
        const response = await pageStability.deliverStatusOnlyObservationAndClassifyTradeCoverage({
          capture: observed.capture,
          deliver: async () => await deadline.run(() => deliverCapture(
            observed.capture,
            observed.rowsetHash,
            SOURCE_OBSERVATION_POLICY,
          )),
          validateTradeCoverage: core.validateCaptureTiming,
        });
        return successfulScan(reason, response);
      }
      if (reason === "reviewed-control-recovery") {
        if (!reviewedControlRecovery?.beginRecovery()) {
          return successfulScan(reason, {
            ok: false,
            reason: "REVIEWED_CONTROL_RECOVERY_CANCELLED",
          });
        }
        try {
          const recovered = await pageStability.recoverReviewedControlMismatch({
            error: new Error(
              "Eastmoney time-sales DOM order disagrees with its reviewed control",
            ),
            isRecoverableError: isRetriableReviewedControlError,
            refreshLatestFirst,
            captureAtomicPage: async () =>
              await atomicReviewedPageCapture(1, null, regularDeadline),
            validateCaptureTiming: core.validateCaptureTiming,
            deadline: regularDeadline,
          });
          reviewedControlRecovery.completeRecovery(true);
          return successfulScan(reason, {
            ok: true,
            reason: "REVIEWED_CONTROL_RECOVERED",
            row_count: recovered.capture.rows.length,
          });
        } catch (error) {
          reviewedControlRecovery.completeRecovery(false);
          throw error;
        }
      }
      if (!ensureLatestFirst()) {
        setTimeout(() => void requestScan("latest-first"), 1500);
        return successfulScan(reason, { ok: false, reason: "WAITING_FOR_LATEST_FIRST" });
      }
      let capture = await readReviewedSnapshot(regularDeadline);
      if (capture.completeness.page_index !== 1) {
        const staleRowsetHash = await regularDeadline.run(() => rowsetHash(capture));
        await navigateHistory("首页", 1, regularDeadline);
        capture = (await stablePageCapture(1, staleRowsetHash, 6, regularDeadline)).capture;
      }
      if (capture.rows.length === 0) {
        return successfulScan(reason, { ok: false, reason: "NO_TIME_SALES_ROWS" });
      }
      try {
        core.validateCaptureTiming(capture);
      } catch (error) {
        if (String(error?.message ?? error) !== "capture latest row is stale") throw error;
        // A quiet symbol and a frozen table are intentionally indistinguishable
        // here. Keep the independent source heartbeat observable and let the
        // worker's trade-coverage gate remain READ_ONLY. A later DOM mutation
        // will re-enter this regular lane and ingest the new trade; this lane
        // must not occupy the reviewed UI guard by toggling the table while no
        // new row exists.
        return successfulScan(reason, { ok: false, reason: "STALE_TRADE_COVERAGE" });
      }
      const captureHash = await regularDeadline.run(() =>
        core.sha256Hex(core.canonicalJson(stableCaptureValue(capture))));
      if (lastObservedCapture === null) {
        lastObservedRowsetHash = captureHash;
        lastObservedCapture = { capture, hash: captureHash };
        setTimeout(() => void requestScan("stability"), 1000);
        return successfulScan(reason, { ok: false, reason: "WAITING_FOR_STABLE_ROWSET" });
      }
      let deliverableCapture = capture;
      if (captureHash !== lastObservedCapture.hash) {
        try {
          deliverableCapture = pageStability.mergeRollingPageCaptures(
            lastObservedCapture.capture,
            capture,
            {
              rowIdentity: (row) => row.source_row_key,
              rowEvidence: (row) =>
                core.canonicalJson(core.stableMarketRowEvidence(row)),
            },
          );
        } catch (_error) {
          lastObservedRowsetHash = captureHash;
          lastObservedCapture = { capture, hash: captureHash };
          setTimeout(() => void requestScan("stability"), 1000);
          return successfulScan(reason, { ok: false, reason: "WAITING_FOR_CONTIGUOUS_ROWSET" });
        }
      }
      if (!pageStability.shouldDeliverCapture(reason, captureHash, lastDeliveredRowsetHash)) {
        lastObservedCapture = { capture, hash: captureHash };
        return successfulScan(reason, { ok: true, reason: "UNCHANGED" });
      }
      const response = await regularDeadline.run(() => deliverCapture(deliverableCapture, captureHash));
      lastObservedRowsetHash = captureHash;
      lastObservedCapture = { capture, hash: captureHash };
      lastDeliveredRowsetHash = captureHash;
      return successfulScan(reason, response);
    } catch (error) {
      const message = String(error?.message ?? error);
      if (pageStability.shouldRecoverLiveOverlap(reason, error)) {
        observer.disconnect();
        initialized = false;
        try {
          const recoveryDeadline = pageStability.createDeadline({
            timeoutMs: SOURCE_OBSERVATION_DEADLINE_MS,
          });
          const recoveryPage = await readReviewedSnapshot(recoveryDeadline);
          const recoveryState = await recoveryDeadline.run(() => chrome.runtime.sendMessage({
            type: "GRIDEDGE_GET_CAPTURE_STATE",
            instrument: recoveryPage.instrument,
          }));
          if (!recoveryState?.ok) {
            throw new Error(recoveryState?.error ?? "capture state query failed during overlap recovery");
          }
          const recovery = await pageStability.recoverLiveOverlap({
            state: recoveryState.state,
            sessionDate: recoveryPage.session_date,
            establishDiscontinuityBoundary: async () =>
              await establishDiscontinuityBoundary(recoveryDeadline),
            establishResumeBoundary: async () => await establishResumeBoundary(recoveryDeadline),
          });
          initialized = true;
          observer.observe(document.documentElement, {
            childList: true,
            characterData: true,
            subtree: true,
          });
          return successfulScan(reason, {
            ok: true,
            reason: recovery.mode === "DISCONTINUITY_BOUNDARY"
              ? "DISCONTINUITY_BOUNDARY_RECOVERED"
              : "PARTIAL_SESSION_BOUNDARY_RECOVERED",
            recovery,
          });
        } catch (boundaryError) {
          const boundaryMessage = String(boundaryError?.message ?? boundaryError);
          reportCaptureError(boundaryMessage);
          scanFailureBudget.recordFailure("regular");
          scheduleRegularRetry(() => void requestInitialization());
          return { ok: false, reason: boundaryMessage };
        }
      }
      reportCaptureError(message);
      const lane = reason === "heartbeat" ? "heartbeat" : "regular";
      scanFailureBudget.recordFailure(lane);
      if (reason === "reviewed-control-recovery") {
        return { ok: false, reason: message };
      }
      if (lane === "heartbeat" && isRetriableReviewedControlError(error)) {
        return { ok: false, reason: message };
      }
      if (lane === "heartbeat" && isRefreshableInitialError(error)) {
        return {
          ok: false,
          reason: "INITIALIZING_HISTORY",
          initialization_error: message,
        };
      }
      if (lane === "regular") {
        scheduleRegularRetry(() => void requestScan("error-retry"));
      }
      return { ok: false, reason: message };
    }
  }

  let reviewedControlRecovery = null;
  const scanRunner = pageStability.createIndependentHeartbeatRouter(scanOnce, {
    mergeReason: (queued, incoming) => {
      if (queued === "reviewed-control-recovery" ||
          incoming === "reviewed-control-recovery") return "reviewed-control-recovery";
      if (queued === "manual" || incoming === "manual") return "manual";
      if (queued === "heartbeat" || incoming === "heartbeat") return "heartbeat";
      return incoming;
    },
  });
  function requestScan(reason) {
    const result = scanRunner.request(reason);
    if (reason !== "heartbeat") return result;
    return result.then((value) => {
      reviewedControlRecovery?.observeHeartbeatResult(value);
      return value;
    });
  }
  reviewedControlRecovery = pageStability.createReviewedControlRecoveryCoordinator({
    requestRecovery: () => scanRunner.request("reviewed-control-recovery"),
    requestImmediateHeartbeat: () => scanRunner.request("heartbeat"),
  });

  function scheduleScan() {
    if (!initialized || scheduled) return;
    scheduled = true;
    setTimeout(() => {
      scheduled = false;
      void requestScan("mutation");
    }, 3000);
  }

  const observer = new MutationObserver(scheduleScan);
  pageStability.installSourceHeartbeat(requestScan, {
    intervalMs: SCAN_HEARTBEAT_MS,
    scheduleEvery: setInterval,
  });
  chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
    if (message?.type === "GRIDEDGE_SOURCE_HEARTBEAT") {
      void requestScan("heartbeat").then(sendResponse);
      return true;
    }
    if (message?.type !== "GRIDEDGE_SCAN_NOW") return false;
    void requestScan("manual").then(sendResponse);
    return true;
  });
  void requestInitialization();
})();
