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
  }) {
    if (!Number.isSafeInteger(ackTimeoutMs) || ackTimeoutMs <= 0) {
      throw new Error("database ACK timeout must be a positive safe integer");
    }
    let published = 0;
    let pending = await durable.pendingEvents(database);
    while (pending.length > 0) {
      for (const event of pending) {
        await mqttAck.waitForCommittedAck(
          client,
          event,
          () => publishWithPuback(client, event),
          ackTimeoutMs,
        );
        await durable.acknowledge(database, event.event_id, "DB_COMMIT_ACK");
        published += 1;
      }
      pending = await durable.pendingEvents(database);
    }
    return published;
  }

  return { flushPending };
});
