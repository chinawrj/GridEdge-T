"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const core = require("../src/shared.js");
globalThis.GridEdgeMarket = core;
const eastmoney = require("../src/providers/eastmoney.js");
const fixture = JSON.parse(
  fs.readFileSync(path.join(__dirname, "../fixtures/eastmoney-time-sales-page1.json"), "utf8"),
);

test("Eastmoney adapter emits one source fact per displayed time-sales row", () => {
  const capture = eastmoney.parseSnapshot(fixture);
  assert.equal(capture.capture_spec, "gridedge.web-market.capture");
  assert.deepEqual(capture.instrument, {
    venue: "XSHE",
    symbol: "002256",
    asset_class: "EQUITY",
    currency: "CNY",
  });
  assert.equal(capture.completeness.page_index, 1);
  assert.equal(capture.completeness.page_count, 19);
  assert.equal(capture.completeness.session_complete, false);
  assert.equal(capture.rows.length, 4);
  assert.equal(capture.rows[1].quantity_hands, 2439);
  assert.equal(capture.rows[1].quantity, 243900);
  assert.notEqual(capture.rows[1].source_row_key, capture.rows[2].source_row_key);
  assert.equal(capture.rows[1].occurrence, 1);
  assert.equal(capture.rows[2].occurrence, 2);
  assert.equal(capture.rows[3].quantity, 10000);
  assert.equal(capture.rows[3].source_table_ordinal, 1);
  assert.equal(Object.hasOwn(capture, "account_marker"), false);
});

test("Eastmoney adapter canonicalizes descending DOM rows into chronological source order", () => {
  const descending = structuredClone(fixture);
  descending.tables.reverse();
  for (const table of descending.tables) {
    const [header, ...rows] = table;
    table.splice(0, table.length, header, ...rows.reverse());
  }
  descending.rowOrder = "LATEST_FIRST";

  const capture = eastmoney.parseSnapshot(descending);

  assert.deepEqual(
    capture.rows.map((row) => row.source_trade_time),
    ["09:30:00", "09:30:03", "09:30:03", "09:30:06"],
  );
});

test("Eastmoney adapter excludes pre-auction display rows before durable live identity is formed", () => {
  const mixed = snapshotPage(1, 1, [
    ["09:30:03", "3.35", "20"],
    ["09:25:00", "3.34", "10"],
    ["09:24:57", "3.33", "30"],
  ]);
  mixed.rowOrder = "LATEST_FIRST";

  const capture = eastmoney.parseSnapshot(mixed);

  assert.deepEqual(
    capture.rows.map((row) => row.source_trade_time),
    ["09:25:00", "09:30:03"],
  );
  assert.equal(capture.completeness.row_count, 2);
  assert.equal(capture.rows.some((row) => row.source_trade_time === "09:24:57"), false);
});

test("Eastmoney adapter preserves reviewed DOM order among same-second trades", () => {
  const descending = snapshotPage(1, 1, [
    ["09:35:00", "3.36", "20"],
    ["09:35:00", "3.34", "10"],
    ["09:34:59", "3.35", "30"],
  ]);
  descending.rowOrder = "LATEST_FIRST";
  const capture = eastmoney.parseSnapshot(descending);
  assert.deepEqual(
    capture.rows.map((row) => [row.source_trade_time, row.price, row.source_same_second_ordinal]),
    [
      ["09:34:59", "3.35", 1],
      ["09:35:00", "3.34", 1],
      ["09:35:00", "3.36", 2],
    ],
  );
  assert.equal(capture.completeness.identity_policy, "DOM_CHRONOLOGICAL_ORDER_V2");
});

test("Eastmoney adapter reads the live pagination token when it is adjacent to 尾页", () => {
  const livePagination = structuredClone(fixture);
  livePagination.bodyText = "首页上一页下一页尾页1/10页 青色的现手表示大额成交";

  const capture = eastmoney.parseSnapshot(livePagination);

  assert.equal(capture.completeness.page_index, 1);
  assert.equal(capture.completeness.page_count, 10);
});

test("Eastmoney adapter extracts exactly one reviewed source-page clock", () => {
  const live = structuredClone(fixture);
  live.bodyText = `${live.bodyText} （2026-08-27 星期四 10:31:24）`;
  assert.equal(
    eastmoney.sourcePageObservedAtUs(live),
    core.eventTimeUs("2026-08-27", "10:31:24"),
  );
  assert.throws(
    () => eastmoney.sourcePageObservedAtUs({ ...live, bodyText: live.bodyText.replace("10:31:24", "") }),
    /unique reviewed source clock/,
  );
  assert.throws(
    () => eastmoney.sourcePageObservedAtUs({ ...live, bodyText: `${live.bodyText} 2026-08-27 星期四 10:31:27` }),
    /unique reviewed source clock/,
  );
});

test("Eastmoney adapter obtains one reviewed HTTPS Date clock without trade-row liveness", async () => {
  const calls = [];
  const url = "https://quote.eastmoney.com/f1.html?newcode=0.002256";
  const observed = await eastmoney.sourceServerObservedAtUs(url, async (requested, options) => {
    calls.push({ requested, options });
    return {
      ok: true,
      status: 200,
      redirected: false,
      url,
      headers: { get: (name) => name.toLowerCase() === "date"
        ? "Thu, 27 Aug 2026 02:46:35 GMT"
        : null },
    };
  });
  assert.equal(observed, core.eventTimeUs("2026-08-27", "10:46:35"));
  assert.equal(calls.length, 1);
  assert.equal(calls[0].requested, url);
  assert.deepEqual(
    { ...calls[0].options, signal: undefined },
    {
      method: "HEAD",
      cache: "no-store",
      credentials: "omit",
      redirect: "error",
      signal: undefined,
    },
  );
  assert.equal(calls[0].options.signal instanceof AbortSignal, true);
  await assert.rejects(
    eastmoney.sourceServerObservedAtUs(url, async () => ({
      ok: true,
      status: 200,
      redirected: false,
      url,
      headers: { get: () => null },
    })),
    /Date header/,
  );
  await assert.rejects(
    eastmoney.sourceServerObservedAtUs(url, async (_requested, options) =>
      await new Promise((_resolve, reject) => {
        options.signal.addEventListener("abort", () => reject(new Error("bounded abort")), {
          once: true,
        });
      }), 1),
    /bounded abort/,
  );
  for (const response of [
    { ok: false, status: 503, redirected: false, url },
    { ok: true, status: 200, redirected: true, url },
    { ok: true, status: 200, redirected: false, url: `${url}&extra=1` },
  ]) {
    await assert.rejects(
      eastmoney.sourceServerObservedAtUs(url, async () => ({
        ...response,
        headers: { get: () => "Thu, 27 Aug 2026 02:46:35 GMT" },
      })),
      /HTTPS identity/,
    );
  }
});

test("clock-bound observation timing rejects stale future wrong-session and wrong-order proof", () => {
  const capture = eastmoney.parseSnapshot(fixture);
  capture.captured_at_us = core.eventTimeUs("2026-08-27", "10:31:30");
  capture.session_date = "2026-08-27";
  capture.source_row_order = "LATEST_FIRST";
  capture.source_page_observed_at_us = core.eventTimeUs("2026-08-27", "10:31:24");
  assert.equal(
    core.validateClockBoundSourceObservationTiming(capture, capture.captured_at_us),
    capture,
  );
  assert.throws(
    () => core.validateClockBoundSourceObservationTiming({
      ...capture,
      source_page_observed_at_us: core.eventTimeUs("2026-08-27", "10:31:14"),
    }, capture.captured_at_us),
    /source page clock is stale/,
  );
  assert.throws(
    () => core.validateClockBoundSourceObservationTiming({
      ...capture,
      source_page_observed_at_us: core.eventTimeUs("2026-08-27", "10:31:31"),
    }, capture.captured_at_us),
    /source page clock is in the future/,
  );
  assert.throws(
    () => core.validateClockBoundSourceObservationTiming({
      ...capture,
      source_page_observed_at_us: core.eventTimeUs("2026-08-26", "10:31:24"),
    }, capture.captured_at_us),
    /source page clock is stale|session date disagrees/,
  );
  assert.throws(
    () => core.validateClockBoundSourceObservationTiming({
      ...capture,
      source_row_order: "EARLIEST_FIRST",
    }, capture.captured_at_us),
    /page clock or order proof/,
  );
});

test("Eastmoney adapter rejects conflicting pagination tokens from one DOM snapshot", () => {
  const conflicting = structuredClone(fixture);
  conflicting.bodyText = "尾页1/10页 另一分页2/10页";

  assert.throws(
    () => eastmoney.parseSnapshot(conflicting),
    /conflicting pagination tokens/,
  );
});

test("Eastmoney adapter refuses pages and symbols outside its reviewed identity", () => {
  assert.equal(eastmoney.matches("https://quote.eastmoney.com/f1.html?newcode=0.002256"), true);
  assert.equal(eastmoney.matches("https://example.com/f1.html?newcode=0.002256"), false);
  assert.throws(
    () => eastmoney.parseSnapshot({ ...fixture, url: "https://quote.eastmoney.com/f1.html?newcode=9.002256" }),
    /reviewed A-share identity/,
  );
});

test("Eastmoney adapter fails closed on a partially rendered populated row", () => {
  const invalid = structuredClone(fixture);
  invalid.tables[0][1][1].text = "";
  assert.throws(() => eastmoney.parseSnapshot(invalid), /row is incomplete/);
});

test("Eastmoney adapter accepts reviewed trailing price direction arrows while preserving raw evidence", () => {
  const liveShape = structuredClone(fixture);
  liveShape.tables[0][1][1].text = "3.33↓";
  liveShape.tables[0][2][1].text = "3.34↑";

  const capture = eastmoney.parseSnapshot(liveShape);

  assert.equal(capture.rows[0].price, "3.33");
  assert.equal(capture.rows[0].raw_cells[1], "3.33↓");
  assert.equal(capture.rows[1].price, "3.34");
  assert.equal(capture.rows[1].raw_cells[1], "3.34↑");
});

test("Eastmoney adapter rejects unreviewed price-cell suffixes", () => {
  const invalid = structuredClone(fixture);
  invalid.tables[0][1][1].text = "3.33*";
  assert.throws(() => eastmoney.parseSnapshot(invalid), /row is incomplete/);
});

test("canonical JSON and normalized price evidence are deterministic", async () => {
  const first = eastmoney.parseSnapshot(fixture);
  const reordered = { ...first, rows: first.rows.map((row) => ({ ...row })) };
  assert.equal(core.canonicalJson(first), core.canonicalJson(reordered));
  assert.equal(
    await core.sha256Hex(core.canonicalJson(first)),
    await core.sha256Hex(core.canonicalJson(reordered)),
  );
});

function snapshotPage(pageIndex, pageCount, rows) {
  return {
    ...structuredClone(fixture),
    bodyText: `时间 成交价 手数 ${pageIndex}/${pageCount}页`,
    rowOrder: "LATEST_FIRST",
    tables: [[
      [
        { text: "时间", class_name: "" },
        { text: "成交价", class_name: "" },
        { text: "手数", class_name: "" },
      ],
      ...rows.map(([time, price, hands]) => [
        { text: time, class_name: "" },
        { text: price, class_name: "" },
        { text: hands, class_name: "" },
      ]),
    ]],
  };
}

test("history assembly covers every page, bridges the live first page, and emits chronological rows", () => {
  const firstPage = eastmoney.parseSnapshot(snapshotPage(1, 2, [
    ["09:35:00", "3.34", "10"],
    ["09:34:57", "3.33", "20"],
  ]));
  const secondPage = eastmoney.parseSnapshot(snapshotPage(2, 2, [
    ["09:34:54", "3.32", "30"],
    ["09:34:51", "3.31", "40"],
  ]));
  const finalFirstPage = eastmoney.parseSnapshot(snapshotPage(1, 2, [
    ["09:35:03", "3.35", "50"],
    ["09:35:00", "3.34", "10"],
  ]));

  const capture = eastmoney.assembleSessionHistory(
    [firstPage, secondPage],
    finalFirstPage,
    ["1".repeat(64), "2".repeat(64)],
    "3".repeat(64),
  );

  assert.equal(capture.page_kind, "TIME_SALES_SESSION");
  assert.equal(capture.completeness.session_complete, true);
  assert.deepEqual(capture.completeness.pages_captured, [1, 2]);
  assert.equal(capture.completeness.live_page_overlap, 1);
  assert.deepEqual(
    capture.rows.map((row) => row.source_trade_time),
    ["09:34:51", "09:34:54", "09:34:57", "09:35:00", "09:35:03"],
  );
});

test("history assembly rejects a missing page or a first-page live-window gap", () => {
  const firstPage = eastmoney.parseSnapshot(snapshotPage(1, 2, [["09:35:00", "3.34", "10"]]));
  const secondPage = eastmoney.parseSnapshot(snapshotPage(2, 2, [["09:34:57", "3.33", "20"]]));
  const disconnectedFirstPage = eastmoney.parseSnapshot(snapshotPage(1, 2, [["10:35:00", "3.34", "10"]]));

  assert.throws(
    () => eastmoney.assembleSessionHistory(
      [firstPage], firstPage, ["1".repeat(64)], "2".repeat(64),
    ),
    /every history page/,
  );
  assert.throws(
    () => eastmoney.assembleSessionHistory(
      [firstPage, secondPage], disconnectedFirstPage,
      ["1".repeat(64), "2".repeat(64)], "3".repeat(64),
    ),
    /live page has no overlap/,
  );
});

test("history assembly rejects a page token that advanced before the time-sales rows", () => {
  const staleRows = [
    ["09:35:00", "3.34", "10"],
    ["09:34:57", "3.33", "20"],
  ];
  const firstPage = eastmoney.parseSnapshot(snapshotPage(1, 2, staleRows));
  const tokenOnlySecondPage = eastmoney.parseSnapshot(snapshotPage(2, 2, staleRows));

  assert.throws(
    () => eastmoney.assembleSessionHistory(
      [firstPage, tokenOnlySecondPage], firstPage,
      ["1".repeat(64), "2".repeat(64)], "3".repeat(64),
    ),
    /duplicate time-sales rowset/,
  );
});

test("history assembly rejects a changing live window mislabeled as an older page", () => {
  const firstPage = eastmoney.parseSnapshot(snapshotPage(1, 2, [
    ["09:35:03", "3.35", "20"],
    ["09:35:00", "3.34", "10"],
  ]));
  const shiftedLiveWindow = eastmoney.parseSnapshot(snapshotPage(2, 2, [
    ["09:35:06", "3.36", "30"],
    ["09:35:03", "3.35", "20"],
  ]));

  assert.throws(
    () => eastmoney.assembleSessionHistory(
      [firstPage, shiftedLiveWindow], firstPage,
      ["1".repeat(64), "2".repeat(64)], "3".repeat(64),
    ),
    /does not move backward/,
  );
});

test("history assembly fails closed when adjacent pages overlap one row identity", () => {
  const firstPage = eastmoney.parseSnapshot(snapshotPage(1, 2, [
    ["09:35:00", "3.34", "10"],
    ["09:34:57", "3.33", "20"],
  ]));
  const secondPage = eastmoney.parseSnapshot(snapshotPage(2, 2, [
    ["09:34:57", "3.33", "20"],
    ["09:34:54", "3.32", "30"],
  ]));

  assert.throws(
    () => eastmoney.assembleSessionHistory(
      [firstPage, secondPage], firstPage,
      ["1".repeat(64), "2".repeat(64)], "3".repeat(64),
    ),
    /overlap at an unprovable source row identity/,
  );
});
