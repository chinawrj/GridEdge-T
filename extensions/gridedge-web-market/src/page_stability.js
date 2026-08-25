(function initializeGridEdgePageStability(root, factory) {
  const api = factory();
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  root.GridEdgePageStability = api;
})(typeof globalThis !== "undefined" ? globalThis : this, function pageStabilityFactory() {
  "use strict";

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

  return { captureStablePage };
});
