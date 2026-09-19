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

function pendingEvents(count) {
  return Array.from({ length: count }, (_value, index) => ({
    ...pendingEvent(),
    event_id: String(index + 1).padStart(64, "0"),
    source_sequence: 2764 + index,
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

test("ordered outbox delivery pipelines a bounded window before waiting for database ACKs", async () => {
  const pending = pendingEvents(4);
  const published = [];
  const acknowledged = [];
  const client = new EventEmitter();
  const durable = {
    pendingEvents: async () => [...pending],
    acknowledge: async (_database, eventId, reason) => {
      const index = pending.findIndex((event) => event.event_id === eventId);
      assert.notEqual(index, -1);
      acknowledged.push({ eventId, reason });
      pending.splice(index, 1);
    },
  };

  const delivered = await delivery.flushPending({
    database: {},
    client,
    durable,
    mqttAck,
    maxInFlight: 4,
    publishWithPuback: async (_client, event) => {
      published.push(event.source_sequence);
      if (published.length === 4) {
        queueMicrotask(() => {
          for (const pendingEvent of [...pending]) {
            client.emit(
              "message",
              `${mqttAck.ACK_PREFIX}/${pendingEvent.event_id}`,
              committedReceipt(pendingEvent),
            );
          }
        });
      }
    },
    ackTimeoutMs: 50,
  });

  assert.equal(delivered, 4);
  assert.deepEqual(published, [2764, 2765, 2766, 2767]);
  assert.deepEqual(
    acknowledged.map(({ eventId }) => eventId),
    pendingEvents(4).map(({ event_id }) => event_id),
  );
  assert.equal(pending.length, 0);
});

test("out-of-order committed receipts still acknowledge the durable prefix in source order", async () => {
  const pending = pendingEvents(4);
  const acknowledged = [];
  const client = new EventEmitter();
  const durable = {
    pendingEvents: async () => [...pending],
    acknowledge: async (_database, eventId) => {
      const index = pending.findIndex((event) => event.event_id === eventId);
      acknowledged.push(pending[index].source_sequence);
      pending.splice(index, 1);
    },
  };
  let publishes = 0;
  await delivery.flushPending({
    database: {}, client, durable, mqttAck, maxInFlight: 4, ackTimeoutMs: 50,
    publishWithPuback: async () => {
      publishes += 1;
      if (publishes === 4) {
        queueMicrotask(() => {
          for (const event of [...pending].reverse()) {
            client.emit("message", `${mqttAck.ACK_PREFIX}/${event.event_id}`,
              committedReceipt(event));
          }
        });
      }
    },
  });
  assert.deepEqual(acknowledged, [2764, 2765, 2766, 2767]);
  assert.equal(pending.length, 0);
});

test("a partial committed-ACK window advances only its exact durable prefix", async () => {
  const pending = pendingEvents(3);
  const acknowledged = [];
  const original = [...pending];
  const client = new EventEmitter();
  const durable = {
    pendingEvents: async () => [...pending],
    acknowledge: async (_database, eventId) => {
      const index = pending.findIndex((event) => event.event_id === eventId);
      acknowledged.push(pending[index].source_sequence);
      pending.splice(index, 1);
    },
  };
  let publishes = 0;
  await assert.rejects(delivery.flushPending({
    database: {}, client, durable, mqttAck, maxInFlight: 3, ackTimeoutMs: 10,
    publishWithPuback: async () => {
      publishes += 1;
      if (publishes === 3) {
        queueMicrotask(() => {
          for (const event of [original[2], original[0]]) {
            client.emit("message", `${mqttAck.ACK_PREFIX}/${event.event_id}`,
              committedReceipt(event));
          }
        });
      }
    },
  }), /timed out/);
  assert.deepEqual(acknowledged, [2764]);
  assert.deepEqual(pending.map((event) => event.source_sequence), [2765, 2766]);
});
