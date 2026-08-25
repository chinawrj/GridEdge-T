"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const { EventEmitter } = require("node:events");
const mqttAck = require("../src/mqtt_ack.js");
const delivery = require("../src/outbox_delivery.js");

function pendingEvent() {
  return {
    event_id: "d".repeat(64),
    source_sequence: 2764,
    mqtt_topic: "gridedge/market/v1/XSHE/002256/trade",
    payload: JSON.stringify({ source: {
      source_id: "eastmoney-web-time-sales",
      source_instance_id: "8101d65c-bdba-4de3-83e0-8983506f159e",
    } }),
  };
}

function committedReceipt(event, overrides = {}) {
  return Buffer.from(JSON.stringify({
    event_id: event.event_id,
    result: "COMMITTED",
    schema_version: 1,
    source_id: "eastmoney-web-time-sales",
    source_instance_id: "8101d65c-bdba-4de3-83e0-8983506f159e",
    source_sequence: event.source_sequence,
    spec: "gridedge.market.ack",
    ...overrides,
  }));
}

function harness() {
  const event = pendingEvent();
  const pending = [event];
  const acknowledged = [];
  return {
    event,
    pending,
    acknowledged,
    client: new EventEmitter(),
    durable: {
      pendingEvents: async () => [...pending],
      acknowledge: async (_database, eventId, reason) => {
        acknowledged.push({ eventId, reason });
        pending.splice(0, pending.length);
      },
    },
  };
}

test("outbox remains PENDING after broker PUBACK when database receipt times out", async () => {
  const state = harness();
  await assert.rejects(
    delivery.flushPending({
      database: {},
      client: state.client,
      durable: state.durable,
      mqttAck,
      publishWithPuback: async () => undefined,
      ackTimeoutMs: 5,
    }),
    /timed out/,
  );
  assert.equal(state.pending.length, 1);
  assert.deepEqual(state.acknowledged, []);
});

test("forged database receipt leaves the durable outbox PENDING", async () => {
  const state = harness();
  const topic = `${mqttAck.ACK_PREFIX}/${state.event.event_id}`;
  await assert.rejects(
    delivery.flushPending({
      database: {},
      client: state.client,
      durable: state.durable,
      mqttAck,
      publishWithPuback: async () => {
        queueMicrotask(() => state.client.emit(
          "message", topic, committedReceipt(state.event, { source_sequence: 2765 }),
        ));
      },
      ackTimeoutMs: 50,
    }),
    /does not bind/,
  );
  assert.equal(state.pending.length, 1);
  assert.deepEqual(state.acknowledged, []);
});

test("exact database COMMITTED receipt is the only path to ACKNOWLEDGED", async () => {
  const state = harness();
  const topic = `${mqttAck.ACK_PREFIX}/${state.event.event_id}`;
  const published = await delivery.flushPending({
    database: {},
    client: state.client,
    durable: state.durable,
    mqttAck,
    publishWithPuback: async () => {
      queueMicrotask(() => state.client.emit(
        "message", topic, committedReceipt(state.event),
      ));
    },
    ackTimeoutMs: 50,
  });
  assert.equal(published, 1);
  assert.equal(state.pending.length, 0);
  assert.deepEqual(state.acknowledged, [{
    eventId: state.event.event_id,
    reason: "DB_COMMIT_ACK",
  }]);
});
