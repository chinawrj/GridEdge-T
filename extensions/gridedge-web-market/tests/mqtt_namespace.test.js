"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const namespace = require("../src/mqtt_namespace.js");

const production = {
  deployment_mode: "PRODUCTION",
  mqtt_url: "ws://192.168.1.201:9001/mqtt",
  mqtt_username: "gridedge-publisher",
  mqtt_password: "secret",
  mqtt_namespace: "gridedge",
  mqtt_client_id: "gridedge-web-market-extension",
  initialization_policy: "FULL_HISTORY_OR_REVIEWED_FALLBACK_V1",
};
const isolated = {
  deployment_mode: "ISOLATED_READ_ONLY_E2E",
  mqtt_url: "ws://127.0.0.1:19001/mqtt",
  mqtt_username: "gridedge-e2e-publisher",
  mqtt_password: "secret",
  mqtt_namespace: "gridedge-e2e/e2e-0629-123e4567-e89b-42d3-a456-426614174000",
  mqtt_client_id: "e2e-0629-123e4567-e89b-42d3-a456-426614174000-publisher",
  initialization_policy: "ISOLATED_EMPTY_STATE_RESUME_BOUNDARY_V1",
};

test("production and isolated E2E settings are mutually exclusive", () => {
  assert.equal(namespace.validateSettings(production).topicRoot, "gridedge/market/v1");
  assert.equal(namespace.validateSettings(isolated).topicRoot,
    "gridedge-e2e/e2e-0629-123e4567-e89b-42d3-a456-426614174000/market/v1");
  assert.throws(() => namespace.validateSettings({ ...production, mqtt_url: isolated.mqtt_url }),
    /production MQTT endpoint/);
  assert.throws(() => namespace.validateSettings({ ...isolated, mqtt_url: production.mqtt_url }),
    /isolated E2E MQTT endpoint/);
  assert.throws(() => namespace.validateSettings({ ...isolated, mqtt_namespace: "gridedge" }),
    /isolated E2E namespace/);
  assert.throws(() => namespace.validateSettings({ ...production, mqtt_namespace: isolated.mqtt_namespace }),
    /production MQTT namespace/);
});

test("all market, ACK, and committed topics remain inside one validated namespace", () => {
  const resolved = namespace.validateSettings(isolated);
  assert.equal(namespace.marketTopic(resolved, "XSHE", "002256", "trade"),
    `${resolved.topicRoot}/XSHE/002256/trade`);
  assert.equal(namespace.ackTopic(resolved, "a".repeat(64)),
    `${resolved.ackRoot}/${"a".repeat(64)}`);
  assert.equal(namespace.committedTopic(resolved, "XSHE/002256/trade"),
    `${resolved.committedRoot}/XSHE/002256/trade`);
  assert.equal(resolved.topicRoot.startsWith("gridedge/market/"), false);
});

test("isolated mode requires an exact nonce-bound client identity", () => {
  assert.throws(() => namespace.validateSettings({ ...isolated, mqtt_client_id: "gridedge-publisher" }),
    /nonce-bound client identity/);
  assert.throws(() => namespace.validateSettings({ ...isolated,
    mqtt_namespace: "gridedge-e2e/not-reviewed" }), /isolated E2E namespace/);
});

test("isolated empty-state boundary policy cannot enter production or be omitted", () => {
  assert.throws(() => namespace.validateSettings({
    ...production,
    initialization_policy: isolated.initialization_policy,
  }), /production initialization policy/);
  assert.throws(() => namespace.validateSettings({
    ...isolated,
    initialization_policy: production.initialization_policy,
  }), /isolated E2E initialization policy/);
  assert.throws(() => namespace.validateSettings({
    ...isolated,
    initialization_policy: undefined,
  }), /isolated E2E initialization policy/);
});
