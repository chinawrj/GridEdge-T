(function initGridEdgeOutboxDelivery(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  root.GridEdgeOutboxDelivery = api;
})(typeof globalThis === "object" ? globalThis : this, function buildOutboxDelivery() {
  "use strict";

  async function flushPending({
    database,
    client,
    durable,
    mqttAck,
    publishWithPuback,
    ackTimeoutMs = 15_000,
    maxInFlight = 16,
  }) {
    if (!Number.isSafeInteger(ackTimeoutMs) || ackTimeoutMs <= 0) {
      throw new Error("database ACK timeout must be a positive safe integer");
    }
    if (!Number.isSafeInteger(maxInFlight) || maxInFlight <= 0 || maxInFlight > 64) {
      throw new Error("outbox in-flight window must be within 1..=64");
    }
    let published = 0;
    let pending = await durable.pendingEvents(database);
    while (pending.length > 0) {
      const window = pending.slice(0, maxInFlight);
      const results = await Promise.allSettled(window.map((event) =>
        mqttAck.waitForCommittedAck(
          client,
          event,
          () => publishWithPuback(client, event),
          ackTimeoutMs,
        ),
      ));
      for (let index = 0; index < window.length; index += 1) {
        const result = results[index];
        if (result.status === "rejected") throw result.reason;
        const event = window[index];
        await durable.acknowledge(database, event.event_id, "DB_COMMIT_ACK");
        published += 1;
      }
      pending = await durable.pendingEvents(database);
    }
    return published;
  }

  return { flushPending };
});
