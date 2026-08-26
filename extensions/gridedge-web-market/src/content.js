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
  const MAX_SCAN_ERROR_RETRIES = 3;
  const SCAN_ERROR_RETRY_MS = 1500;
  const MAX_REVIEWED_SNAPSHOT_ATTEMPTS = 60;
  let initialized = false;
  let scheduled = false;
  let consecutiveScanFailures = 0;
  let lastObservedRowsetHash = null;
  let lastDeliveredRowsetHash = null;

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
      checkbox.click();
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
      message === "Eastmoney time-sales DOM order disagrees with its reviewed control";
  }

  async function readReviewedSnapshot() {
    return await pageStability.readCaptureWithRetry({
      readCapture: async () => provider.parseSnapshot(documentSnapshot()),
      isRetriableError: isRetriableReviewedControlError,
      delay: async () => await delay(100),
      maxAttempts: MAX_REVIEWED_SNAPSHOT_ATTEMPTS,
    });
  }

  async function refreshLatestFirst() {
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
        for (let attempt = 0; attempt < 20; attempt += 1) {
          await delay(250);
          try {
            const capture = provider.parseSnapshot(documentSnapshot());
            if (capture.rows.length === 0) continue;
            const currentRowsetHash = await rowsetHash(capture);
            if (previousRowsetHash === null || currentRowsetHash !== previousRowsetHash) return;
          } catch (_error) {
            // Eastmoney can replace the pagination token before its rows.
          }
        }
      },
      maxStateAttempts: 60,
    });
  }

  function stableCaptureValue(capture) {
    return { ...capture, captured_at_us: 0 };
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

  async function stablePageCapture(expectedPageIndex = null, forbiddenRowsetHash = null) {
    return await pageStability.captureStablePage({
      readCapture: readReviewedSnapshot,
      stableCaptureHash: async (capture) =>
        await core.sha256Hex(core.canonicalJson(stableCaptureValue(capture))),
      rowsetHash,
      delay: async () => await delay(500),
      expectedPageIndex,
      forbiddenRowsetHash,
      maxAttempts: MAX_STABILITY_ATTEMPTS,
    });
  }

  function historyLink(label) {
    return Array.from(document.querySelectorAll("a")).find((link) =>
      core.normalizeText(link.textContent) === label && link.getClientRects().length > 0,
    );
  }

  async function navigateHistory(label, expectedPageIndex) {
    const link = historyLink(label);
    if (!link) throw new Error(`Eastmoney history navigation link is missing: ${label}`);
    link.click();
    for (let attempt = 0; attempt < 20; attempt += 1) {
      await delay(250);
      try {
        const capture = provider.parseSnapshot(documentSnapshot());
        if (capture.completeness.page_index === expectedPageIndex) return;
      } catch (_error) {
        // The table can be transiently incomplete while Eastmoney redraws it.
      }
    }
    throw new Error(`Eastmoney history navigation did not reach page ${expectedPageIndex}`);
  }

  async function deliverCapture(capture, rowsetHash) {
    core.validateCaptureTiming(capture);
    const captureSha256 = await core.sha256Hex(core.canonicalJson(capture));
    const response = await chrome.runtime.sendMessage({
      type: "GRIDEDGE_CAPTURE_BATCH",
      capture,
      capture_sha256: captureSha256,
      rowset_hash: rowsetHash,
    });
    if (!response?.ok) throw new Error(response?.error ?? response?.reason ?? "capture delivery failed");
    return response;
  }

  async function crawlSessionHistory() {
    for (let restart = 0; restart < MAX_HISTORY_RESTARTS; restart += 1) {
      const current = await readReviewedSnapshot();
      let previousPageRowsetHash = null;
      if (current.completeness.page_index !== 1) {
        previousPageRowsetHash = await rowsetHash(current);
        await navigateHistory("首页", 1);
      }
      const pageCaptures = [];
      const pageHashes = [];
      let expectedPageCount = null;
      for (let pageIndex = 1; pageIndex <= (expectedPageCount ?? 1); pageIndex += 1) {
        const page = await stablePageCapture(pageIndex, previousPageRowsetHash);
        expectedPageCount = Math.max(expectedPageCount ?? 0, page.capture.completeness.page_count);
        if (expectedPageCount > MAX_HISTORY_PAGES) throw new Error("Eastmoney history exceeds the reviewed page bound");
        pageCaptures.push(page.capture);
        pageHashes.push(page.hash);
        previousPageRowsetHash = page.rowsetHash;
        if (pageIndex < expectedPageCount) await navigateHistory("下一页", pageIndex + 1);
      }
      let finalFirstPage;
      if (expectedPageCount === 1) {
        finalFirstPage = await stablePageCapture(1);
      } else {
        await navigateHistory("首页", 1);
        finalFirstPage = await stablePageCapture(1, previousPageRowsetHash);
      }
      try {
        const completeCapture = provider.assembleSessionHistory(
          pageCaptures,
          finalFirstPage.capture,
          pageHashes,
          finalFirstPage.hash,
        );
        await deliverCapture(completeCapture, finalFirstPage.hash);
        lastObservedRowsetHash = finalFirstPage.hash;
        lastDeliveredRowsetHash = finalFirstPage.hash;
        return completeCapture;
      } catch (error) {
        if (restart + 1 === MAX_HISTORY_RESTARTS) throw error;
      }
    }
    throw new Error("Eastmoney history crawl exhausted its restart bound");
  }

  async function establishResumeBoundary() {
    let current = await readReviewedSnapshot();
    let forbiddenRowsetHash = null;
    if (current.completeness.page_index !== 1) {
      forbiddenRowsetHash = await rowsetHash(current);
      await navigateHistory("首页", 1);
    }
    const stable = await stablePageCapture(1, forbiddenRowsetHash);
    core.validateCaptureTiming(stable.capture);
    const captureSha256 = await core.sha256Hex(core.canonicalJson(stable.capture));
    const response = await chrome.runtime.sendMessage({
      type: "GRIDEDGE_RESUME_BOUNDARY",
      capture: stable.capture,
      capture_sha256: captureSha256,
      rowset_hash: stable.rowsetHash,
    });
    if (!response?.ok) {
      throw new Error(response?.error ?? response?.reason ?? "resume boundary delivery failed");
    }
    lastObservedRowsetHash = stable.rowsetHash;
    lastDeliveredRowsetHash = stable.rowsetHash;
    return response;
  }

  async function initializeCollector() {
    while (!ensureLatestFirst()) await delay(750);
    let currentPage = await pageStability.captureInitialPageWithRefresh({
      captureStableFirstPage: async (forbiddenRowsetHash) =>
        await stablePageCapture(null, forbiddenRowsetHash),
      refreshLatestFirst,
      isRefreshableInitialError,
      validateCaptureTiming(capture) {
        if (capture.completeness.page_index === 1) core.validateCaptureTiming(capture);
      },
    });
    const stateResponse = await chrome.runtime.sendMessage({
      type: "GRIDEDGE_GET_CAPTURE_STATE",
      instrument: currentPage.capture.instrument,
    });
    if (!stateResponse?.ok) throw new Error(stateResponse?.error ?? "capture state query failed");
    if (stateResponse.state?.complete_session_date === currentPage.capture.session_date) {
      if (currentPage.capture.completeness.page_index !== 1) {
        await navigateHistory("首页", 1);
        currentPage = await stablePageCapture(1, currentPage.rowsetHash);
      }
    } else {
      try {
        await crawlSessionHistory();
      } catch (_historyError) {
        await establishResumeBoundary();
      }
    }
    return await pageStability.completeProvisionalInitialization({
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
  }

  const initializationRunner = pageStability.createRetriableInitializer(initializeCollector, {
    isInitialized: () => initialized,
    scheduleRetry(callback) {
      setTimeout(callback, 15_000);
    },
    async onError(error) {
      await chrome.runtime.sendMessage({
        type: "GRIDEDGE_CAPTURE_ERROR",
        provider: "eastmoney",
        page_url: location.href,
        message: String(error?.message ?? error),
      });
    },
  });

  function requestInitialization() {
    return initializationRunner.request();
  }

  function successfulScan(result) {
    consecutiveScanFailures = 0;
    return result;
  }

  async function scanOnce(reason) {
    try {
      if (!initialized) {
        void requestInitialization();
        return { ok: false, reason: "INITIALIZING_HISTORY" };
      }
      if (!ensureLatestFirst()) {
        setTimeout(() => void requestScan("latest-first"), 1500);
        return successfulScan({ ok: false, reason: "WAITING_FOR_LATEST_FIRST" });
      }
      let capture = await readReviewedSnapshot();
      if (capture.completeness.page_index !== 1) {
        const staleRowsetHash = await rowsetHash(capture);
        await navigateHistory("首页", 1);
        capture = (await stablePageCapture(1, staleRowsetHash)).capture;
      }
      if (capture.rows.length === 0) {
        return successfulScan({ ok: false, reason: "NO_TIME_SALES_ROWS" });
      }
      try {
        core.validateCaptureTiming(capture);
      } catch (error) {
        if (String(error?.message ?? error) !== "capture latest row is stale") throw error;
        capture = (await pageStability.refreshStaleFirstPage({
          staleCapture: capture,
          rowsetHash,
          refreshLatestFirst,
          captureStableFirstPage: async (forbiddenRowsetHash) =>
            await stablePageCapture(1, forbiddenRowsetHash),
          validateCaptureTiming: core.validateCaptureTiming,
          retryDelay: async () => await delay(500),
          maxRefreshAttempts: 3,
        })).capture;
      }
      const captureHash = await core.sha256Hex(core.canonicalJson(stableCaptureValue(capture)));
      if (captureHash !== lastObservedRowsetHash) {
        lastObservedRowsetHash = captureHash;
        setTimeout(() => void requestScan("stability"), 1000);
        return successfulScan({ ok: false, reason: "WAITING_FOR_STABLE_ROWSET" });
      }
      if (reason !== "manual" && captureHash === lastDeliveredRowsetHash) {
        return successfulScan({ ok: true, reason: "UNCHANGED" });
      }
      const response = await deliverCapture(capture, captureHash);
      lastDeliveredRowsetHash = captureHash;
      return successfulScan(response);
    } catch (error) {
      const message = String(error?.message ?? error);
      if (message.includes("live capture has no overlap with the prior durable watermark")) {
        observer.disconnect();
        initialized = false;
        try {
          await crawlSessionHistory();
          initialized = true;
          observer.observe(document.documentElement, {
            childList: true,
            characterData: true,
            subtree: true,
          });
          return successfulScan({ ok: true, reason: "HISTORY_RECOVERED" });
        } catch (recoveryError) {
          try {
            const boundary = await establishResumeBoundary();
            initialized = true;
            observer.observe(document.documentElement, {
              childList: true,
              characterData: true,
              subtree: true,
            });
            return successfulScan({
              ok: true,
              reason: "PARTIAL_SESSION_BOUNDARY_RECOVERED",
              boundary,
            });
          } catch (boundaryError) {
            await chrome.runtime.sendMessage({
              type: "GRIDEDGE_CAPTURE_ERROR",
              provider: "eastmoney",
              page_url: location.href,
              message: String(boundaryError?.message ?? boundaryError),
            });
            const boundaryMessage = String(boundaryError?.message ?? boundaryError);
            if (consecutiveScanFailures < MAX_SCAN_ERROR_RETRIES) {
              consecutiveScanFailures += 1;
              setTimeout(() => void requestInitialization(), SCAN_ERROR_RETRY_MS);
            }
            return { ok: false, reason: boundaryMessage };
          }
        }
      }
      await chrome.runtime.sendMessage({
        type: "GRIDEDGE_CAPTURE_ERROR",
        provider: "eastmoney",
        page_url: location.href,
        message,
      });
      if (consecutiveScanFailures < MAX_SCAN_ERROR_RETRIES) {
        consecutiveScanFailures += 1;
        setTimeout(() => void requestScan("error-retry"), SCAN_ERROR_RETRY_MS);
      }
      return { ok: false, reason: message };
    }
  }

  const scanRunner = pageStability.createSingleFlightRunner(scanOnce, {
    mergeReason: (queued, incoming) =>
      queued === "manual" || incoming === "manual" ? "manual" : incoming,
  });
  function requestScan(reason) {
    return scanRunner.request(reason);
  }

  function scheduleScan() {
    if (!initialized || scheduled) return;
    scheduled = true;
    setTimeout(() => {
      scheduled = false;
      void requestScan("mutation");
    }, 3000);
  }

  const observer = new MutationObserver(scheduleScan);
  chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
    if (message?.type !== "GRIDEDGE_SCAN_NOW") return false;
    void requestScan("manual").then(sendResponse);
    return true;
  });
  void requestInitialization();
})();
