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
  let initialized = false;
  let scheduled = false;
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
    const checkbox = Array.from(document.querySelectorAll('input[type="checkbox"]')).find((input) =>
      Array.from(input.labels ?? []).some((label) =>
        core.normalizeText(label.textContent) === "倒序",
      ),
    );
    if (!checkbox) throw new Error("Eastmoney reviewed row-order control is missing");
    return checkbox.checked ? "LATEST_FIRST" : "EARLIEST_FIRST";
  }

  function ensureLatestFirst() {
    const checkbox = Array.from(document.querySelectorAll('input[type="checkbox"]')).find((input) =>
      Array.from(input.labels ?? []).some((label) =>
        core.normalizeText(label.textContent) === "倒序",
      ),
    );
    if (!checkbox) return false;
    if (!checkbox.checked) {
      checkbox.click();
      return false;
    }
    return true;
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
      readCapture: async () => provider.parseSnapshot(documentSnapshot()),
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
      const current = provider.parseSnapshot(documentSnapshot());
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
    let current = provider.parseSnapshot(documentSnapshot());
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
    let currentPage = await stablePageCapture();
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
    initialized = true;
    observer.observe(document.documentElement, {
      childList: true,
      characterData: true,
      subtree: true,
    });
    return await scan("initial");
  }

  async function scan(reason) {
    scheduled = false;
    try {
      if (!initialized) return { ok: false, reason: "INITIALIZING_HISTORY" };
      if (!ensureLatestFirst()) {
        setTimeout(() => void scan("latest-first"), 1500);
        return { ok: false, reason: "WAITING_FOR_LATEST_FIRST" };
      }
      let capture = provider.parseSnapshot(documentSnapshot());
      if (capture.completeness.page_index !== 1) {
        const staleRowsetHash = await rowsetHash(capture);
        await navigateHistory("首页", 1);
        capture = (await stablePageCapture(1, staleRowsetHash)).capture;
      }
      if (capture.rows.length === 0) return { ok: false, reason: "NO_TIME_SALES_ROWS" };
      const rowsetHash = await core.sha256Hex(core.canonicalJson(stableCaptureValue(capture)));
      if (rowsetHash !== lastObservedRowsetHash) {
        lastObservedRowsetHash = rowsetHash;
        setTimeout(() => void scan("stability"), 1000);
        return { ok: false, reason: "WAITING_FOR_STABLE_ROWSET" };
      }
      if (reason !== "manual" && rowsetHash === lastDeliveredRowsetHash) {
        return { ok: true, reason: "UNCHANGED" };
      }
      const response = await deliverCapture(capture, rowsetHash);
      lastDeliveredRowsetHash = rowsetHash;
      return response;
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
          return { ok: true, reason: "HISTORY_RECOVERED" };
        } catch (recoveryError) {
          try {
            const boundary = await establishResumeBoundary();
            initialized = true;
            observer.observe(document.documentElement, {
              childList: true,
              characterData: true,
              subtree: true,
            });
            return { ok: true, reason: "PARTIAL_SESSION_BOUNDARY_RECOVERED", boundary };
          } catch (boundaryError) {
            await chrome.runtime.sendMessage({
              type: "GRIDEDGE_CAPTURE_ERROR",
              provider: "eastmoney",
              page_url: location.href,
              message: String(boundaryError?.message ?? boundaryError),
            });
            return { ok: false, reason: String(boundaryError?.message ?? boundaryError) };
          }
        }
      }
      await chrome.runtime.sendMessage({
        type: "GRIDEDGE_CAPTURE_ERROR",
        provider: "eastmoney",
        page_url: location.href,
        message,
      });
      return { ok: false, reason: message };
    }
  }

  function scheduleScan() {
    if (!initialized || scheduled) return;
    scheduled = true;
    setTimeout(() => void scan("mutation"), 3000);
  }

  const observer = new MutationObserver(scheduleScan);
  chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
    if (message?.type !== "GRIDEDGE_SCAN_NOW") return false;
    void scan("manual").then(sendResponse);
    return true;
  });
  const initializeWithRetry = () => void initializeCollector().catch(async (error) => {
    await chrome.runtime.sendMessage({
      type: "GRIDEDGE_CAPTURE_ERROR",
      provider: "eastmoney",
      page_url: location.href,
      message: String(error?.message ?? error),
    });
    setTimeout(initializeWithRetry, 15_000);
  });
  initializeWithRetry();
})();
