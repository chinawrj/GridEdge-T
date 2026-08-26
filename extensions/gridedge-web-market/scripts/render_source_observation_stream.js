"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { indexedDB } = require("fake-indexeddb");

globalThis.indexedDB = indexedDB;
require("../src/shared.js");
const provider = require("../src/providers/eastmoney.js");
const durable = require("../src/durable.js");

async function digest(value) {
  return await GridEdgeMarket.sha256Hex(GridEdgeMarket.canonicalJson(value));
}

async function main() {
  const fixture = JSON.parse(fs.readFileSync(
    path.join(__dirname, "../fixtures/eastmoney-time-sales-page1.json"),
    "utf8",
  ));
  const initial = provider.parseSnapshot(fixture);
  const history = structuredClone(initial);
  history.page_kind = "TIME_SALES_SESSION";
  history.completeness = {
    ...history.completeness,
    session_complete: true,
    pages_captured: [1, 2],
    page_count: 2,
    history_page_sha256: ["1".repeat(64), "2".repeat(64)],
    final_live_page_sha256: "3".repeat(64),
    live_page_overlap: 1,
    covered_from_us: GridEdgeMarket.eventTimeUs(
      history.session_date,
      history.rows[0].source_trade_time,
    ),
    covered_through_us: GridEdgeMarket.eventTimeUs(
      history.session_date,
      history.rows.at(-1).source_trade_time,
    ),
  };

  const database = await durable.openDatabase(
    indexedDB,
    `gridedge-cross-runtime-${crypto.randomUUID()}`,
  );
  await durable.ingestCapture(database, history, await digest(history));
  const observation = structuredClone(initial);
  observation.captured_at_us = history.completeness.covered_through_us + 30_000_000;
  await durable.ingestCapture(database, observation, await digest(observation), {
    sourceObservationPolicy: "ACTIVE_REVIEWED_LATEST_FIRST_CYCLE_V1",
  });
  observation.captured_at_us += 30_000_000;
  await durable.ingestCapture(database, observation, await digest(observation), {
    sourceObservationPolicy: "ACTIVE_REVIEWED_LATEST_FIRST_CYCLE_V1",
  });

  for (const event of await durable.pendingEvents(database, 20)) {
    process.stdout.write(`${JSON.stringify({
      topic: event.mqtt_topic,
      payload_hex: Buffer.from(event.payload, "utf8").toString("hex"),
    })}\n`);
  }
  database.close();
}

void main().catch((error) => {
  process.stderr.write(`${error?.stack ?? error}\n`);
  process.exitCode = 1;
});
