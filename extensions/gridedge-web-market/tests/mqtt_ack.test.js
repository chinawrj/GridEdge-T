"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const { EventEmitter } = require("node:events");
const ack = require("../src/mqtt_ack.js");

test("database commit ACK must bind the exact event and source sequence", () => {
  const event = {
    event_id: "a".repeat(64),
    source_sequence: 2764,
    payload: JSON.stringify({ source: {
      source_id: "eastmoney-web-time-sales",
      source_instance_id: "8101d65c-bdba-4de3-83e0-8983506f159e",
    } }),
  };
  const topic = `gridedge/market-ack/v1/${event.event_id}`;
  const payload = Buffer.from(JSON.stringify({
    event_id: event.event_id,
    result: "COMMITTED",
    schema_version: 1,
    source_id: "eastmoney-web-time-sales",
    source_instance_id: "8101d65c-bdba-4de3-83e0-8983506f159e",
    source_sequence: event.source_sequence,
    spec: "gridedge.market.ack",
  }));
  assert.deepEqual(ack.validateCommittedAck(topic, payload, event), { ok: true });
  const forged = JSON.parse(payload);
  forged.source_sequence += 1;
  assert.throws(
    () => ack.validateCommittedAck(topic, Buffer.from(JSON.stringify(forged)), event),
    /does not bind/,
  );
});

test("broker PUBACK alone cannot complete delivery; database ACK is required", async () => {
  const client = new EventEmitter();
  const event = {
    event_id: "b".repeat(64),
    source_sequence: 2764,
    payload: JSON.stringify({ source: {
      source_id: "eastmoney-web-time-sales",
      source_instance_id: "8101d65c-bdba-4de3-83e0-8983506f159e",
    } }),
  };
  await assert.rejects(
    () => ack.waitForCommittedAck(client, event, async () => undefined, 5),
    /timed out/,
  );

  const topic = `gridedge/market-ack/v1/${event.event_id}`;
  const payload = Buffer.from(JSON.stringify({
    event_id: event.event_id,
    result: "COMMITTED",
    schema_version: 1,
    source_id: "eastmoney-web-time-sales",
    source_instance_id: "8101d65c-bdba-4de3-83e0-8983506f159e",
    source_sequence: 2764,
    spec: "gridedge.market.ack",
  }));
  await ack.waitForCommittedAck(client, event, async () => {
    queueMicrotask(() => client.emit("message", topic, payload));
  }, 50);
});
