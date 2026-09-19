(function initGridEdgeMqttAck(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  root.GridEdgeMqttAck = api;
})(typeof globalThis === "object" ? globalThis : this, function buildMqttAck() {
  "use strict";

  const ACK_PREFIX = "gridedge/market-ack/v1";

  function expectedIdentity(event) {
    const document = JSON.parse(event.payload);
    return {
      event_id: event.event_id,
      source_id: document.source.source_id,
      source_instance_id: document.source.source_instance_id,
      source_sequence: event.source_sequence,
    };
  }

  function validateCommittedAck(topic, payload, event, ackRoot = ACK_PREFIX) {
    const expected = expectedIdentity(event);
    if (topic !== `${ackRoot}/${expected.event_id}`) {
      throw new Error("database ACK topic does not bind the pending event");
    }
    let receipt;
    try {
      receipt = JSON.parse(new TextDecoder().decode(payload));
    } catch (_error) {
      throw new Error("database ACK payload is not JSON");
    }
    const keys = Object.keys(receipt).sort().join(",");
    if (keys !== "event_id,result,schema_version,source_id,source_instance_id,source_sequence,spec" ||
        receipt.spec !== "gridedge.market.ack" || receipt.schema_version !== 1 ||
        receipt.result !== "COMMITTED" || receipt.event_id !== expected.event_id ||
        receipt.source_id !== expected.source_id ||
        receipt.source_instance_id !== expected.source_instance_id ||
        receipt.source_sequence !== expected.source_sequence) {
      throw new Error("database ACK does not bind the exact committed market event");
    }
    return { ok: true };
  }

  function subscribe(client, ackRoot = ACK_PREFIX, timeoutMs = 10_000) {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
      return Promise.reject(new Error("database ACK subscription timeout is invalid"));
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("database ACK subscription timed out")), timeoutMs);
      try {
        client.subscribe(`${ackRoot}/#`, { qos: 1 }, (error, granted) => {
          clearTimeout(timer);
          if (error) return reject(error);
          if (!Array.isArray(granted) || granted.length !== 1 || granted[0].qos !== 1) {
            return reject(new Error("database ACK subscription was not granted at QoS 1"));
          }
          resolve();
        });
      } catch (error) {
        clearTimeout(timer);
        reject(error);
      }
    });
  }

  async function waitForCommittedAck(client, event, publish, timeoutMs = 15_000, ackRoot = ACK_PREFIX) {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
      throw new Error("database commit ACK timeout is invalid");
    }
    const target = `${ackRoot}/${event.event_id}`;
    let timer;
    let settle;
    const receipt = new Promise((resolve, reject) => { settle = { resolve, reject }; });
    const deadline = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error("database commit ACK or PUBACK timed out")), timeoutMs);
    });
    const onMessage = (topic, payload) => {
      if (topic !== target) return;
      try {
        validateCommittedAck(topic, payload, event, ackRoot);
        settle.resolve();
      } catch (error) {
        settle.reject(error);
      }
    };
    client.on("message", onMessage);
    try {
      // Attach both rejection handlers before publishing. Either acknowledgement
      // can arrive first, but both must complete within the same bounded attempt.
      // A resolved COMMITTED receipt must not disable a missing-PUBACK deadline.
      await Promise.race([
        Promise.all([receipt, Promise.resolve().then(publish)]),
        deadline,
      ]);
    } finally {
      clearTimeout(timer);
      client.removeListener("message", onMessage);
    }
  }

  function withAckRoot(ackRoot) {
    return {
      subscribe: (client) => subscribe(client, ackRoot),
      validateCommittedAck: (topic, payload, event) =>
        validateCommittedAck(topic, payload, event, ackRoot),
      waitForCommittedAck: (client, event, publish, timeoutMs) =>
        waitForCommittedAck(client, event, publish, timeoutMs, ackRoot),
    };
  }

  return { ACK_PREFIX, subscribe, validateCommittedAck, waitForCommittedAck, withAckRoot };
});
