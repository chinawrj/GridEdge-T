"use strict";

importScripts(
  "../vendor/mqtt.min.js",
  "shared.js",
  "mqtt_namespace.js",
  "durable.js",
  "mqtt_ack.js",
  "outbox_delivery.js",
  "flush_coordinator.js",
  "source_heartbeat.js",
);

const DEFAULT_SETTINGS = Object.freeze({
  enabled: false,
  mqtt_url: "ws://192.168.1.201:9001/mqtt",
  mqtt_username: "gridedge-publisher",
  mqtt_password: "",
  deployment_mode: "PRODUCTION",
  mqtt_namespace: "gridedge",
  mqtt_client_id: "",
  initialization_policy: "FULL_HISTORY_OR_REVIEWED_FALLBACK_V1",
});

let flushCoordinator = null;
const OUTBOX_ALARM = "gridedge-mqtt-outbox";
const SOURCE_HEARTBEAT_TIMEOUT_MS = 10_000;
const REVIEWED_BUILD_ID = "collector-0.6.51-stale-trade-coverage-only-v1";

async function ensurePeriodicAlarms() {
  await chrome.alarms.create(OUTBOX_ALARM, { periodInMinutes: 1 });
}

function runtimeIdentity() {
  return {
    manifest_version: chrome.runtime.getManifest().version,
    reviewed_build_id: REVIEWED_BUILD_ID,
  };
}

const reviewedTabRecovery = GridEdgeSourceHeartbeat.createReviewedTabRecovery({
  getTab: async (tabId) => await chrome.tabs.get(tabId),
  reloadTab: async (tabId, options) => await chrome.tabs.reload(tabId, options),
  loadCooldown: async (key) => {
    const stored = await chrome.storage.session.get(key);
    return stored[key] ?? null;
  },
  saveCooldown: async (key, value) => await chrome.storage.session.set({ [key]: value }),
});

async function dispatchSourceHeartbeat() {
  return await GridEdgeSourceHeartbeat.dispatchSourceHeartbeat({
    queryTabs: async () => await chrome.tabs.query({
      url: "https://quote.eastmoney.com/f1.html*",
    }),
    sendMessage: async (tabId, message) => await chrome.tabs.sendMessage(tabId, message),
    recoverTab: async (tab, response) => response.reason === "STALE_TRADE_COVERAGE" &&
      await reviewedTabRecovery.request(tab, response),
    timeoutMs: SOURCE_HEARTBEAT_TIMEOUT_MS,
  });
}

const sourceHeartbeatCoordinator =
  GridEdgeSourceHeartbeat.createSourceHeartbeatCoordinator(dispatchSourceHeartbeat);
GridEdgeSourceHeartbeat.installSourceHeartbeatAlarmWatchdog({
  alarms: chrome.alarms,
  runtime: chrome.runtime,
  coordinator: sourceHeartbeatCoordinator,
});

async function settings() {
  const stored = await chrome.storage.local.get(Object.keys(DEFAULT_SETTINGS));
  const current = { ...DEFAULT_SETTINGS, ...stored };
  if (!current.mqtt_client_id && current.deployment_mode === "PRODUCTION") {
    current.mqtt_client_id = `gridedge-web-market-${chrome.runtime.id}`;
  }
  return current;
}

async function setStatus(status, storeGeneration = GridEdgeDurable.DATABASE_NAME) {
  await chrome.storage.local.set({
    last_status: {
      ...status,
      store_generation: storeGeneration,
      at: Date.now(),
    },
  });
}

function allowedSender(sender) {
  try {
    const url = new URL(sender.tab?.url ?? "");
    return url.protocol === "https:" && url.hostname === "quote.eastmoney.com";
  } catch (_error) {
    return false;
  }
}

function validateMqttSettings(current) {
  return GridEdgeMqttNamespace.validateSettings(current);
}

function connectMqtt(current) {
  const resolved = validateMqttSettings(current);
  return new Promise((resolve, reject) => {
    const client = mqtt.connect(resolved.url, {
      protocolVersion: 5,
      clean: true,
      clientId: resolved.clientId,
      username: current.mqtt_username,
      password: current.mqtt_password,
      keepalive: 20,
      connectTimeout: 10000,
      reconnectPeriod: 0,
      forceNativeWebSocket: true,
    });
    let settled = false;
    client.once("connect", () => {
      void GridEdgeMqttAck.subscribe(client, resolved.ackRoot).then(() => {
        settled = true;
        resolve(client);
      }).catch((error) => {
        settled = true;
        client.end(true);
        reject(error);
      });
    });
    client.once("error", (error) => {
      if (!settled) {
        settled = true;
        client.end(true);
        reject(error);
      }
    });
    client.once("close", () => {
      if (!settled) {
        settled = true;
        reject(new Error("MQTT WebSocket closed before CONNACK"));
      }
    });
  });
}

function publishWithPuback(client, event) {
  return new Promise((resolve, reject) => {
    client.publish(
      event.mqtt_topic,
      event.payload,
      { qos: 1, retain: false, properties: { contentType: "application/json" } },
      (error) => error ? reject(error) : resolve(),
    );
  });
}

async function doFlushOutbox() {
  const current = await settings();
  if (!current.enabled) return { ok: false, reason: "COLLECTOR_DISABLED" };
  const resolved = validateMqttSettings(current);
  const database = await GridEdgeDurable.openDatabase(indexedDB, resolved.databaseName);
  let pending = await GridEdgeDurable.pendingEvents(database);
  if (pending.length === 0) {
    return { ok: true, published: 0, store: await GridEdgeDurable.status(database) };
  }
  const client = await connectMqtt(current);
  let published;
  try {
    published = await GridEdgeOutboxDelivery.flushPending({
      database,
      client,
      durable: GridEdgeDurable,
      mqttAck: GridEdgeMqttAck.withAckRoot(resolved.ackRoot),
      publishWithPuback,
      maxInFlight: 16,
    });
  } finally {
    client.end(true);
  }
  const store = await GridEdgeDurable.status(database);
  await setStatus({ ok: true, kind: "DATABASE_COMMIT_ACK", published, store },
    resolved.databaseName);
  return { ok: true, published, store };
}

function flushOutbox() {
  if (!flushCoordinator) {
    flushCoordinator = GridEdgeFlushCoordinator.createFlushCoordinator(doFlushOutbox, {
      async onError(error) {
        const current = await settings();
        const resolved = validateMqttSettings(current);
        const database = await GridEdgeDurable.openDatabase(indexedDB, resolved.databaseName);
        const store = await GridEdgeDurable.status(database);
        await setStatus({
          ok: false,
          kind: "MQTT_QUEUED",
          error: String(error?.message ?? error),
          store,
        }, resolved.databaseName);
      },
    });
  }
  return flushCoordinator.request();
}

async function deliverCapture(
  message,
  sender,
  { resumeBoundary = false, discontinuityBoundary = false, completeHistoryBridge = false } = {},
) {
  if (!allowedSender(sender)) throw new Error("capture sender is outside the reviewed page allowlist");
  const current = await settings();
  if (!current.enabled) return { ok: false, reason: "COLLECTOR_DISABLED" };
  const resolved = validateMqttSettings(current);
  const database = await GridEdgeDurable.openDatabase(indexedDB, resolved.databaseName);
  if ([resumeBoundary, discontinuityBoundary, completeHistoryBridge].filter(Boolean).length > 1) {
    throw new Error("capture delivery cannot select multiple recovery policies");
  }
  const stored = discontinuityBoundary
    ? await GridEdgeDurable.ingestDiscontinuityBoundary(
      database,
      message.capture,
      message.capture_sha256,
      { mqttTopicRoot: resolved.topicRoot, deadlineAtMs: message.deadline_at_ms },
    )
    : resumeBoundary
    ? await GridEdgeDurable.ingestResumeBoundary(
      database,
      message.capture,
      message.capture_sha256,
      { mqttTopicRoot: resolved.topicRoot },
    )
    : await GridEdgeDurable.ingestCapture(database, message.capture, message.capture_sha256, {
        requireCompleteHistoryBridge: completeHistoryBridge,
        sourceObservationPolicy: message.source_observation_policy ?? null,
        sourceObservationOnly: message.source_observation_policy != null,
        mqttTopicRoot: resolved.topicRoot,
      });
  const store = await GridEdgeDurable.status(database);
  void flushOutbox().catch(async (error) => {
    const failedStore = await GridEdgeDurable.status(database);
    await setStatus({
      ok: false,
      kind: "MQTT_QUEUED",
      error: String(error?.message ?? error),
      store: failedStore,
    }, resolved.databaseName);
  });
  return { ok: true, stored, delivery: { ok: false, queued: store.pending_events } };
}

async function scanActiveTab() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab?.id || !tab.url?.startsWith("https://quote.eastmoney.com/")) {
    throw new Error("当前标签页不是受支持的东方财富行情页");
  }
  return await chrome.tabs.sendMessage(tab.id, { type: "GRIDEDGE_SCAN_NOW" });
}

chrome.runtime.onInstalled.addListener(async () => {
  const current = await chrome.storage.local.get(Object.keys(DEFAULT_SETTINGS));
  const missing = Object.fromEntries(Object.entries(DEFAULT_SETTINGS).filter(([key]) => current[key] === undefined));
  if (Object.keys(missing).length > 0) await chrome.storage.local.set(missing);
  await ensurePeriodicAlarms();
});

chrome.runtime.onStartup.addListener(() => {
  void ensurePeriodicAlarms();
  void flushOutbox().catch((error) =>
    setStatus({ ok: false, kind: "MQTT_RETRY_ERROR", error: String(error?.message ?? error) }));
});
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === OUTBOX_ALARM) {
    void flushOutbox().catch((error) =>
      setStatus({ ok: false, kind: "MQTT_RETRY_ERROR", error: String(error?.message ?? error) }));
  }
});

// Alarm persistence is not guaranteed across browser restarts or extension
// updates. Reassert both schedules whenever the service worker is evaluated.
void ensurePeriodicAlarms();

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  const run = async () => {
    switch (message?.type) {
      case "GRIDEDGE_CAPTURE_BATCH": return await deliverCapture(message, sender);
      case "GRIDEDGE_COMPLETE_HISTORY_BRIDGE":
        return await deliverCapture(message, sender, { completeHistoryBridge: true });
      case "GRIDEDGE_DISCONTINUITY_BOUNDARY":
        return await deliverCapture(message, sender, { discontinuityBoundary: true });
      case "GRIDEDGE_RESUME_BOUNDARY":
        return await deliverCapture(message, sender, { resumeBoundary: true });
      case "GRIDEDGE_CAPTURE_ERROR":
        await setStatus({ ok: false, kind: "CAPTURE_ERROR", provider: message.provider, page_url: message.page_url, error: message.message });
        return { ok: true };
      case "GRIDEDGE_GET_STATUS": {
        const current = await settings();
        const resolved = validateMqttSettings(current);
        const { last_status: lastStatus = null } = await chrome.storage.local.get("last_status");
        const database = await GridEdgeDurable.openDatabase(indexedDB, resolved.databaseName);
        const effectiveStatus = lastStatus?.store_generation === resolved.databaseName
          ? lastStatus
          : { ok: false, kind: "STORE_GENERATION_CHANGED", store_generation: resolved.databaseName };
        return {
          runtime_identity: runtimeIdentity(),
          settings: { ...current, mqtt_password: current.mqtt_password ? "***" : "" },
          store: await GridEdgeDurable.status(database),
          last_status: effectiveStatus,
        };
      }
      case "GRIDEDGE_GET_CAPTURE_STATE": {
        if (!allowedSender(sender)) throw new Error("capture-state sender is outside the reviewed page allowlist");
        const current = await settings();
        const resolved = validateMqttSettings(current);
        const database = await GridEdgeDurable.openDatabase(indexedDB, resolved.databaseName);
        try {
          return {
            ok: true,
            state: await GridEdgeDurable.sourceState(database, message.instrument),
            initialization_policy: resolved.initializationPolicy,
          };
        } catch (error) {
          if (String(error?.message ?? error) === "source state does not exist") {
            return { ok: true, state: null, initialization_policy: resolved.initializationPolicy };
          }
          throw error;
        }
      }
      case "GRIDEDGE_FLUSH_OUTBOX": return await flushOutbox();
      case "GRIDEDGE_SCAN_ACTIVE_TAB": return await scanActiveTab();
      case "GRIDEDGE_RELOAD_EXTENSION": setTimeout(() => chrome.runtime.reload(), 50); return { ok: true };
      default: throw new Error("unsupported extension message");
    }
  };
  void run().then(sendResponse).catch(async (error) => {
    const messageText = String(error?.message ?? error);
    await setStatus({ ok: false, kind: "BACKGROUND_ERROR", error: messageText });
    sendResponse({ ok: false, error: messageText });
  });
  return true;
});
