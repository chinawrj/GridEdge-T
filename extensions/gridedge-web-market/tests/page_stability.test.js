"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { captureStablePage } = require("../src/page_stability.js");

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
