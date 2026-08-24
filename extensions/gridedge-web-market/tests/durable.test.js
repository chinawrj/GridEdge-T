"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const { indexedDB } = require("fake-indexeddb");

globalThis.indexedDB = indexedDB;
require("../src/shared.js");
const provider = require("../src/providers/eastmoney.js");
const durable = require("../src/durable.js");

const fixture = JSON.parse(fs.readFileSync(path.join(__dirname, "../fixtures/eastmoney-time-sales-page1.json"), "utf8"));

async function capture() {
  const value = provider.parseSnapshot(fixture);
  const sha = await GridEdgeMarket.sha256Hex(GridEdgeMarket.canonicalJson(value));
  return { value, sha };
}

async function completeSessionCapture() {
  const input = await capture();
  const value = structuredClone(input.value);
  value.page_kind = "TIME_SALES_SESSION";
  value.completeness = {
    ...value.completeness,
    session_complete: true,
    pages_captured: [1, 2],
    page_count: 2,
    history_page_sha256: ["1".repeat(64), "2".repeat(64)],
    final_live_page_sha256: "3".repeat(64),
    live_page_overlap: 1,
    covered_from_us: GridEdgeMarket.eventTimeUs(value.session_date, value.rows[0].source_trade_time),
    covered_through_us: GridEdgeMarket.eventTimeUs(value.session_date, value.rows.at(-1).source_trade_time),
  };
  const sha = await GridEdgeMarket.sha256Hex(GridEdgeMarket.canonicalJson(value));
  return { value, sha };
}

async function laterLiveCapture() {
  const input = await capture();
  const value = structuredClone(input.value);
  const row = structuredClone(value.rows.at(-1));
  row.source_trade_time = "09:30:09";
  row.price = "3.35";
  row.raw_cells = ["09:30:09", "3.35", String(row.quantity_hands)];
  row.source_row_key = `${value.session_date}|09:30:09|3.35|${row.quantity_hands}|${row.side}|1`;
  row.occurrence = 1;
  value.rows.push(row);
  value.completeness.row_count = value.rows.length;
  value.captured_at_us += 3_000_000;
  const sha = await GridEdgeMarket.sha256Hex(GridEdgeMarket.canonicalJson(value));
  return { value, sha };
}

async function allOutboxEvents(database) {
  const transaction = database.transaction("market_event_outbox", "readonly");
  const request = transaction.objectStore("market_event_outbox").getAll();
  const events = await new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
  await new Promise((resolve, reject) => {
    transaction.oncomplete = resolve;
    transaction.onabort = () => reject(transaction.error);
    transaction.onerror = () => reject(transaction.error);
  });
  return events.sort((left, right) => left.source_sequence - right.source_sequence);
}

async function replaceOutboxEvent(database, previousEventId, event) {
  const transaction = database.transaction("market_event_outbox", "readwrite");
  const store = transaction.objectStore("market_event_outbox");
  if (previousEventId !== event.event_id) store.delete(previousEventId);
  store.put(event);
  await new Promise((resolve, reject) => {
    transaction.oncomplete = resolve;
    transaction.onabort = () => reject(transaction.error);
    transaction.onerror = () => reject(transaction.error);
  });
}

async function deleteOutboxEvent(database, eventId) {
  const transaction = database.transaction("market_event_outbox", "readwrite");
  transaction.objectStore("market_event_outbox").delete(eventId);
  await new Promise((resolve, reject) => {
    transaction.oncomplete = resolve;
    transaction.onabort = () => reject(transaction.error);
    transaction.onerror = () => reject(transaction.error);
  });
}

test("IndexedDB owns raw row identity, source sequence, and a persistent MQTT outbox", async () => {
  const name = `gridedge-test-${crypto.randomUUID()}`;
  let database = await durable.openDatabase(indexedDB, name);
  const input = await capture();
  const first = await durable.ingestCapture(database, input.value, input.sha);
  assert.deepEqual({ accepted: first.accepted, duplicates: first.duplicates, conflicts: first.conflicts }, { accepted: 4, duplicates: 0, conflicts: 0 });
  assert.equal(new Set(first.event_ids).size, 4);
  assert.deepEqual(await durable.status(database), { accepted_rows: 4, conflicts: 0, pending_events: 4, acknowledged_events: 0 });

  const pending = await durable.pendingEvents(database);
  assert.deepEqual(pending.map((event) => event.source_sequence), [1, 2, 3, 4]);
  for (const event of pending) {
    const document = JSON.parse(event.payload);
    assert.equal(event.payload, GridEdgeMarket.canonicalJson(document));
    assert.equal(document.source.source_id, "eastmoney-web-time-sales");
    assert.equal(document.event_id, event.event_id);
    assert.equal(event.mqtt_topic, "gridedge/market/v1/XSHE/002256/trade");
    const { event_id: _eventId, recv_us: _receivedAt, ...identity } = document;
    assert.equal(await GridEdgeMarket.sha256Hex(GridEdgeMarket.canonicalJson(identity)), document.event_id);
  }

  database.close();
  database = await durable.openDatabase(indexedDB, name);
  assert.equal((await durable.pendingEvents(database)).length, 4, "worker restart must not lose unacknowledged events");
  for (const event of await durable.pendingEvents(database)) await durable.acknowledge(database, event.event_id);
  assert.deepEqual(await durable.status(database), { accepted_rows: 4, conflicts: 0, pending_events: 0, acknowledged_events: 4 });
  database.close();
});

test("exact re-capture is idempotent while changed evidence is durably rejected", async () => {
  const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
  const input = await capture();
  await durable.ingestCapture(database, input.value, input.sha);
  const duplicate = await durable.ingestCapture(database, input.value, input.sha);
  assert.deepEqual({ accepted: duplicate.accepted, duplicates: duplicate.duplicates, conflicts: duplicate.conflicts }, { accepted: 0, duplicates: 4, conflicts: 0 });

  const changed = structuredClone(input.value);
  changed.rows[0].price = "3.32";
  const changedSha = await GridEdgeMarket.sha256Hex(GridEdgeMarket.canonicalJson(changed));
  const conflict = await durable.ingestCapture(database, changed, changedSha);
  assert.deepEqual({ accepted: conflict.accepted, duplicates: conflict.duplicates, conflicts: conflict.conflicts }, { accepted: 0, duplicates: 3, conflicts: 1 });
  assert.deepEqual(await durable.status(database), { accepted_rows: 4, conflicts: 1, pending_events: 4, acknowledged_events: 0 });
  database.close();
});

test("rolling-table relocation is duplicate delivery rather than a market-data conflict", async () => {
  const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
  const input = await capture();
  await durable.ingestCapture(database, input.value, input.sha);

  const relocated = structuredClone(input.value);
  relocated.captured_at_us += 3_000_000;
  for (const row of relocated.rows) {
    row.source_table_ordinal += 1;
    row.source_row_ordinal += 7;
  }
  const relocatedSha = await GridEdgeMarket.sha256Hex(GridEdgeMarket.canonicalJson(relocated));
  const result = await durable.ingestCapture(database, relocated, relocatedSha);

  assert.deepEqual(
    { accepted: result.accepted, duplicates: result.duplicates, conflicts: result.conflicts },
    { accepted: 0, duplicates: 4, conflicts: 0 },
  );
  assert.deepEqual(await durable.status(database), {
    accepted_rows: 4,
    conflicts: 0,
    pending_events: 4,
    acknowledged_events: 0,
  });
  database.close();
});

test("late Eastmoney direction decoration is duplicate presentation rather than a market-data conflict", async () => {
  const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
  const input = await capture();
  await durable.ingestCapture(database, input.value, input.sha);

  const decorated = structuredClone(input.value);
  decorated.captured_at_us += 3_000_000;
  decorated.rows[0].raw_cells[1] = `${decorated.rows[0].price}↓`;
  const decoratedSha = await GridEdgeMarket.sha256Hex(GridEdgeMarket.canonicalJson(decorated));
  const result = await durable.ingestCapture(database, decorated, decoratedSha);

  assert.deepEqual(
    { accepted: result.accepted, duplicates: result.duplicates, conflicts: result.conflicts },
    { accepted: 0, duplicates: 4, conflicts: 0 },
  );
  assert.deepEqual(await durable.status(database), {
    accepted_rows: 4,
    conflicts: 0,
    pending_events: 4,
    acknowledged_events: 0,
  });
  database.close();
});

test("complete history appends ticks first and one durable SOURCE_STATUS watermark last", async () => {
  const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
  const input = await completeSessionCapture();
  const result = await durable.ingestCapture(database, input.value, input.sha);

  assert.equal(result.accepted, 4);
  assert.equal(result.status_events, 1);
  const pending = await durable.pendingEvents(database, 20);
  assert.equal(pending.length, 5);
  const documents = pending.map((event) => JSON.parse(event.payload));
  assert.deepEqual(documents.slice(0, 4).map((event) => event.event_type), Array(4).fill("TRADE_TICK"));
  assert.equal(documents[4].event_type, "SOURCE_STATUS");
  assert.equal(documents[4].payload.status, "SESSION_HISTORY_COMPLETE");
  assert.equal(documents[4].payload.covered_through_us, input.value.completeness.covered_through_us);
  assert.deepEqual(documents.map((event) => event.source_sequence), [1, 2, 3, 4, 5]);
  assert.deepEqual(await durable.status(database), {
    accepted_rows: 4,
    conflicts: 0,
    pending_events: 5,
    acknowledged_events: 0,
  });
  database.close();
});

test("completed-session state is durable and scoped to one reviewed source and instrument", async () => {
  const name = `gridedge-test-${crypto.randomUUID()}`;
  let database = await durable.openDatabase(indexedDB, name);
  const input = await completeSessionCapture();
  await durable.ingestCapture(database, input.value, input.sha);
  database.close();

  database = await durable.openDatabase(indexedDB, name);
  const state = await durable.sourceState(database, input.value.instrument);
  assert.equal(state.complete_session_date, input.value.session_date);
  assert.equal(state.complete_capture_sha256, input.sha);
  assert.equal(state.covered_through_us, input.value.completeness.covered_through_us);
  assert.equal(typeof state.source_instance_id, "string");
  assert.equal(state.next_sequence, 6);
  await assert.rejects(
    () => durable.sourceState(database, { ...input.value.instrument, symbol: "000001" }),
    /source state does not exist/,
  );
  database.close();
});

test("an overlapping live page advances one durable continuity watermark after its new ticks", async () => {
  const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
  const history = await completeSessionCapture();
  await durable.ingestCapture(database, history.value, history.sha);
  const live = await laterLiveCapture();
  const result = await durable.ingestCapture(database, live.value, live.sha);

  assert.equal(result.accepted, 1);
  assert.equal(result.duplicates, 4);
  assert.equal(result.status_events, 1);
  const events = await durable.pendingEvents(database, 20);
  const documents = events.map((event) => JSON.parse(event.payload));
  assert.deepEqual(documents.map((event) => event.source_sequence), [1, 2, 3, 4, 5, 6, 7]);
  assert.equal(documents[5].event_type, "TRADE_TICK");
  assert.equal(documents[6].event_type, "SOURCE_STATUS");
  assert.equal(documents[6].payload.status, "LIVE_CONTIGUOUS");
  assert.equal(documents[6].payload.previous_covered_through_us, history.value.completeness.covered_through_us);
  assert.equal(documents[6].payload.live_page_overlap, 4);
  const state = await durable.sourceState(database, live.value.instrument);
  assert.equal(state.covered_through_us, documents[5].ts_us);
  database.close();
});

test("replay export preserves exact durable MQTT bytes for one requested session", async () => {
  const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
  const history = await completeSessionCapture();
  await durable.ingestCapture(database, history.value, history.sha);
  const live = await laterLiveCapture();
  await durable.ingestCapture(database, live.value, live.sha);
  const pending = await durable.pendingEvents(database, 20);
  await durable.acknowledge(database, pending[0].event_id);

  const exported = await durable.replayExport(database, history.value.session_date);
  assert.equal(exported.record_count, 7);
  assert.equal(exported.first_source_sequence, 1);
  assert.equal(exported.last_source_sequence, 7);
  assert.equal(exported.acknowledged_count, 1);
  assert.equal(exported.pending_count, 6);
  assert.deepEqual(exported.provider_versions, ["eastmoney-time-sales-dom-v6"]);
  const decoded = exported.records.map((line) => {
    const record = JSON.parse(line);
    return {
      topic: record.topic,
      document: JSON.parse(Buffer.from(record.payload_hex, "hex").toString("utf8")),
    };
  });
  assert.deepEqual(decoded.map(({ document }) => document.source_sequence), [1, 2, 3, 4, 5, 6, 7]);
  assert.ok(decoded.every(({ topic }) => topic.startsWith("gridedge/market/v1/XSHE/002256/")));
  assert.deepEqual((await durable.replayExport(database, "2026-08-19")).records, []);
  await assert.rejects(() => durable.replayExport(database, "2026/08/20"), /YYYY-MM-DD/);
  database.close();
});

test("replay export rejects a missing middle sequence instead of manufacturing completeness", async () => {
  const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
  const history = await completeSessionCapture();
  await durable.ingestCapture(database, history.value, history.sha);
  const events = await allOutboxEvents(database);
  await deleteOutboxEvent(database, events[2].event_id);
  await assert.rejects(
    () => durable.replayExport(database, history.value.session_date),
    /contiguous/,
  );
  database.close();
});

test("replay export validates state, topic, source key, and cryptographic identity before date filtering", async () => {
  const mutations = [
    { label: "unknown state", mutate(event) { event.state = "DELIVERED"; }, error: /state/ },
    { label: "topic", mutate(event) { event.mqtt_topic = "gridedge/market/v1/XSHE/000001/trade"; }, error: /topic/ },
    { label: "source key", mutate(event) { event.source_key = "forged|XSHE|002256"; }, error: /source key/ },
    {
      label: "event id",
      mutate(event) {
        const document = JSON.parse(event.payload);
        document.payload.quantity += 100;
        event.payload = GridEdgeMarket.canonicalJson(document);
      },
      error: /event id/,
    },
    {
      label: "hidden other day",
      mutate(event) {
        const document = JSON.parse(event.payload);
        document.payload.source_row_key = document.payload.source_row_key.replace("2026-08-20", "2026-08-19");
        event.payload = GridEdgeMarket.canonicalJson(document);
      },
      error: /event id|session date/,
    },
  ];
  for (const mutation of mutations) {
    const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
    const input = await capture();
    await durable.ingestCapture(database, input.value, input.sha);
    const event = (await allOutboxEvents(database))[0];
    mutation.mutate(event);
    await replaceOutboxEvent(database, event.event_id, event);
    await assert.rejects(
      () => durable.replayExport(database, "2026-08-19"),
      mutation.error,
      mutation.label,
    );
    database.close();
  }
});

test("replay export rejects source-instance mixing even when each payload is self-consistent", async () => {
  const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
  const input = await capture();
  await durable.ingestCapture(database, input.value, input.sha);
  const events = await allOutboxEvents(database);
  const event = events[1];
  const oldId = event.event_id;
  const document = JSON.parse(event.payload);
  document.source.source_instance_id = crypto.randomUUID();
  const { event_id: _eventId, recv_us: _recvUs, ...identity } = document;
  document.event_id = await GridEdgeMarket.sha256Hex(GridEdgeMarket.canonicalJson(identity));
  event.event_id = document.event_id;
  event.payload = GridEdgeMarket.canonicalJson(document);
  await replaceOutboxEvent(database, oldId, event);
  await assert.rejects(() => durable.replayExport(database, input.value.session_date), /single source identity/);
  database.close();
});

test("replay export takes one atomic snapshot while ACK state changes concurrently", async () => {
  const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
  const input = await capture();
  await durable.ingestCapture(database, input.value, input.sha);
  const first = (await durable.pendingEvents(database))[0];
  const [exported] = await Promise.all([
    durable.replayExport(database, input.value.session_date),
    durable.acknowledge(database, first.event_id),
  ]);
  assert.equal(exported.record_count, 4);
  assert.equal(exported.pending_count + exported.acknowledged_count, 4);
  database.close();
});

test("a live page without a row at or before the prior watermark is rejected without mutation", async () => {
  const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
  const history = await completeSessionCapture();
  await durable.ingestCapture(database, history.value, history.sha);
  const live = await laterLiveCapture();
  for (let index = 0; index < live.value.rows.length; index += 1) {
    const row = live.value.rows[index];
    row.source_trade_time = `10:00:${String(index).padStart(2, "0")}`;
    row.source_row_key = `${live.value.session_date}|${row.source_trade_time}|${row.price}|${row.quantity_hands}|${row.side}|1`;
    row.raw_cells[0] = row.source_trade_time;
  }
  live.value.captured_at_us = GridEdgeMarket.eventTimeUs(live.value.session_date, "10:00:05");
  live.sha = await GridEdgeMarket.sha256Hex(GridEdgeMarket.canonicalJson(live.value));

  await assert.rejects(
    () => durable.ingestCapture(database, live.value, live.sha),
    /live capture has no overlap with the prior durable watermark/,
  );
  const state = await durable.sourceState(database, live.value.instrument);
  assert.equal(state.covered_through_us, history.value.completeness.covered_through_us);
  assert.deepEqual(await durable.status(database), {
    accepted_rows: 4,
    conflicts: 0,
    pending_events: 5,
    acknowledged_events: 0,
  });
  database.close();
});

test("background validation rejects trading identity and a forged capture digest", async () => {
  const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
  const input = await capture();
  const withAccount = { ...input.value, account_marker: "模拟练习" };
  await assert.rejects(() => durable.ingestCapture(database, withAccount, input.sha), /account_marker is not market data/);
  await assert.rejects(() => durable.ingestCapture(database, input.value, "0".repeat(64)), /SHA-256 disagrees/);
  assert.deepEqual(await durable.status(database), { accepted_rows: 0, conflicts: 0, pending_events: 0, acknowledged_events: 0 });
  database.close();
});

test("non-trading-day, closed-session, future-row, and stale-row captures fail before durable mutation", async () => {
  const cases = [
    {
      label: "weekend",
      mutate(value) {
        value.session_date = "2026-08-22";
        value.captured_at_us = GridEdgeMarket.eventTimeUs(value.session_date, "09:30:08");
        for (const row of value.rows) {
          row.source_row_key = row.source_row_key.replace("2026-08-20", value.session_date);
        }
      },
      error: /non-trading day/,
    },
    {
      label: "after close",
      mutate(value) {
        value.captured_at_us = GridEdgeMarket.eventTimeUs(value.session_date, "15:05:00");
      },
      error: /outside the reviewed collection window/,
    },
    {
      label: "official weekday holiday",
      mutate(value) {
        value.session_date = "2026-01-02";
        value.captured_at_us = GridEdgeMarket.eventTimeUs(value.session_date, "09:30:08");
        for (const row of value.rows) {
          row.source_row_key = row.source_row_key.replace("2026-08-20", value.session_date);
        }
      },
      error: /non-trading day/,
    },
    {
      label: "future row",
      mutate(value) {
        value.captured_at_us = GridEdgeMarket.eventTimeUs(value.session_date, "09:30:06") - 1;
      },
      error: /later than its capture clock/,
    },
    {
      label: "stale row",
      mutate(value) {
        value.captured_at_us = GridEdgeMarket.eventTimeUs(value.session_date, "09:32:00");
      },
      error: /latest row is stale/,
    },
  ];
  for (const scenario of cases) {
    const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
    const input = await capture();
    scenario.mutate(input.value);
    input.sha = await GridEdgeMarket.sha256Hex(GridEdgeMarket.canonicalJson(input.value));
    await assert.rejects(
      () => durable.ingestCapture(database, input.value, input.sha),
      scenario.error,
      scenario.label,
    );
    assert.deepEqual(await durable.status(database), {
      accepted_rows: 0,
      conflicts: 0,
      pending_events: 0,
      acknowledged_events: 0,
    });
    database.close();
  }
});

test("capture and complete-history watermarks reject one-microsecond future or bound drift", async () => {
  const input = await capture();
  assert.throws(
    () => GridEdgeMarket.validateCaptureTiming(input.value, input.value.captured_at_us - 1),
    /capture clock is in the future/,
  );

  const database = await durable.openDatabase(indexedDB, `gridedge-test-${crypto.randomUUID()}`);
  const complete = await completeSessionCapture();
  complete.value.completeness.covered_through_us += 1;
  complete.sha = await GridEdgeMarket.sha256Hex(GridEdgeMarket.canonicalJson(complete.value));
  await assert.rejects(
    () => durable.ingestCapture(database, complete.value, complete.sha),
    /watermark disagrees with exact row bounds/,
  );
  assert.deepEqual(await durable.status(database), {
    accepted_rows: 0,
    conflicts: 0,
    pending_events: 0,
    acknowledged_events: 0,
  });
  database.close();
});
