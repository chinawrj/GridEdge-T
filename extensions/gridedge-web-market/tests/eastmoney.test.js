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
