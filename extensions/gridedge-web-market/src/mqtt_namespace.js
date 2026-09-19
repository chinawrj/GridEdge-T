(function initGridEdgeMqttNamespace(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  root.GridEdgeMqttNamespace = api;
})(typeof globalThis === "object" ? globalThis : this, function buildMqttNamespace() {
  "use strict";

  const PRODUCTION = "PRODUCTION";
  const ISOLATED = "ISOLATED_READ_ONLY_E2E";
  const PRODUCTION_INITIALIZATION_POLICY = "FULL_HISTORY_OR_REVIEWED_FALLBACK_V1";
  const ISOLATED_INITIALIZATION_POLICY = "ISOLATED_EMPTY_STATE_RESUME_BOUNDARY_V1";
  const E2E_NAMESPACE = /^gridedge-e2e\/(e2e-0629-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$/;

  function validateSettings(settings) {
    if (!settings || typeof settings !== "object") throw new Error("MQTT settings are required");
    if (!settings.mqtt_password) throw new Error("MQTT publisher credentials are incomplete");
    let url;
    try {
      url = new URL(settings.mqtt_url);
    } catch (_error) {
      throw new Error("MQTT endpoint is invalid");
    }
    const mode = settings.deployment_mode ?? PRODUCTION;
    let namespace;
    if (mode === PRODUCTION) {
      if (settings.initialization_policy !== PRODUCTION_INITIALIZATION_POLICY) {
        throw new Error("production initialization policy is invalid");
      }
      if (url.protocol !== "ws:" || url.hostname !== "192.168.1.201" ||
          url.port !== "9001" || url.pathname !== "/mqtt") {
        throw new Error("production MQTT endpoint must be ws://192.168.1.201:9001/mqtt");
      }
      if (settings.mqtt_username !== "gridedge-publisher") {
        throw new Error("production MQTT publisher identity is invalid");
      }
      if ((settings.mqtt_namespace ?? "gridedge") !== "gridedge") {
        throw new Error("production MQTT namespace must be gridedge");
      }
      if (!/^gridedge-web-market-[a-z0-9._-]+$/i.test(settings.mqtt_client_id ?? "")) {
        throw new Error("production MQTT client identity is invalid");
      }
      namespace = "gridedge";
    } else if (mode === ISOLATED) {
      if (settings.initialization_policy !== ISOLATED_INITIALIZATION_POLICY) {
        throw new Error("isolated E2E initialization policy is invalid");
      }
      if (url.protocol !== "ws:" || !["127.0.0.1", "localhost"].includes(url.hostname) ||
          url.port !== "19001" || url.pathname !== "/mqtt") {
        throw new Error("isolated E2E MQTT endpoint must be loopback port 19001");
      }
      if (settings.mqtt_username !== "gridedge-e2e-publisher") {
        throw new Error("isolated E2E MQTT publisher identity is invalid");
      }
      const match = E2E_NAMESPACE.exec(settings.mqtt_namespace ?? "");
      if (!match) throw new Error("isolated E2E namespace is invalid");
      if (settings.mqtt_client_id !== `${match[1]}-publisher`) {
        throw new Error("isolated E2E requires a nonce-bound client identity");
      }
      namespace = settings.mqtt_namespace;
    } else {
      throw new Error("MQTT deployment mode is invalid");
    }
    return Object.freeze({
      mode,
      namespace,
      url: url.href,
      clientId: settings.mqtt_client_id,
      topicRoot: `${namespace}/market/v1`,
      ackRoot: `${namespace}/market-ack/v1`,
      committedRoot: `${namespace}/market-committed/v1`,
      databaseName: mode === PRODUCTION ? "gridedge-web-market-v6" :
        `gridedge-web-market-v6-${namespace.split("/")[1]}`,
      initializationPolicy: settings.initialization_policy,
    });
  }

  function marketTopic(resolved, venue, symbol, stream) {
    return `${resolved.topicRoot}/${venue}/${symbol}/${stream}`;
  }
  function ackTopic(resolved, eventId) {
    return `${resolved.ackRoot}/${eventId}`;
  }
  function committedTopic(resolved, suffix) {
    return `${resolved.committedRoot}/${suffix}`;
  }

  return {
    ISOLATED,
    PRODUCTION,
    PRODUCTION_INITIALIZATION_POLICY,
    ISOLATED_INITIALIZATION_POLICY,
    ackTopic,
    committedTopic,
    marketTopic,
    validateSettings,
  };
});
