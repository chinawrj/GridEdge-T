"use strict";

importScripts(
  "../vendor/mqtt.min.js",
  "shared.js",
  "durable.js",
  "mqtt_ack.js",
  "outbox_delivery.js",
);

const DEFAULT_SETTINGS = Object.freeze({
  enabled: false,
  mqtt_url: "ws://192.168.1.201:9001/mqtt",
  mqtt_username: "gridedge-publisher",
  mqtt_password: "",
});

let flushInFlight = null;

async function settings() {
  const stored = await chrome.storage.local.get(Object.keys(DEFAULT_SETTINGS));
  return { ...DEFAULT_SETTINGS, ...stored };
}

async function setStatus(status) {
  await chrome.storage.local.set({
    last_status: {
      ...status,
      store_generation: GridEdgeDurable.DATABASE_NAME,
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
  const url = new URL(current.mqtt_url);
  if (url.protocol !== "ws:" || url.hostname !== "192.168.1.201" ||
      url.port !== "9001" || url.pathname !== "/mqtt") {
    throw new Error("MQTT WebSocket must be ws://192.168.1.201:9001/mqtt");
  }
  if (current.mqtt_username !== "gridedge-publisher" || !current.mqtt_password) {
    throw new Error("MQTT publisher credentials are incomplete");
  }
  return url.href;
}

function connectMqtt(current) {
  const url = validateMqttSettings(current);
  return new Promise((resolve, reject) => {
    const client = mqtt.connect(url, {
      protocolVersion: 5,
      clean: true,
      clientId: `gridedge-web-market-${chrome.runtime.id}`,
      username: current.mqtt_username,
      password: current.mqtt_password,
      keepalive: 20,
      connectTimeout: 10000,
      reconnectPeriod: 0,
      forceNativeWebSocket: true,
    });
    let settled = false;
    client.once("connect", () => {
      void GridEdgeMqttAck.subscribe(client).then(() => {
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
  const database = await GridEdgeDurable.openDatabase();
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
      mqttAck: GridEdgeMqttAck,
      publishWithPuback,
    });
  } finally {
    client.end(true);
  }
  const store = await GridEdgeDurable.status(database);
  await setStatus({ ok: true, kind: "DATABASE_COMMIT_ACK", published, store });
  return { ok: true, published, store };
}

function flushOutbox() {
  if (!flushInFlight) {
    flushInFlight = doFlushOutbox().finally(() => { flushInFlight = null; });
  }
  return flushInFlight;
}

async function deliverCapture(message, sender, resumeBoundary = false) {
  if (!allowedSender(sender)) throw new Error("capture sender is outside the reviewed page allowlist");
  const current = await settings();
  if (!current.enabled) return { ok: false, reason: "COLLECTOR_DISABLED" };
  validateMqttSettings(current);
  const database = await GridEdgeDurable.openDatabase();
  const stored = resumeBoundary
    ? await GridEdgeDurable.ingestResumeBoundary(
      database,
      message.capture,
      message.capture_sha256,
    )
    : await GridEdgeDurable.ingestCapture(database, message.capture, message.capture_sha256);
  try {
    const delivery = await flushOutbox();
    return { ok: true, stored, delivery };
  } catch (error) {
    const store = await GridEdgeDurable.status(database);
    await setStatus({ ok: false, kind: "MQTT_QUEUED", error: String(error?.message ?? error), store });
    return { ok: true, stored, delivery: { ok: false, queued: store.pending_events } };
  }
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
  await chrome.alarms.create("gridedge-mqtt-outbox", { periodInMinutes: 1 });
});

chrome.runtime.onStartup.addListener(() => void flushOutbox().catch((error) =>
  setStatus({ ok: false, kind: "MQTT_RETRY_ERROR", error: String(error?.message ?? error) })));
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === "gridedge-mqtt-outbox") {
    void flushOutbox().catch((error) =>
      setStatus({ ok: false, kind: "MQTT_RETRY_ERROR", error: String(error?.message ?? error) }));
  }
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  const run = async () => {
    switch (message?.type) {
      case "GRIDEDGE_CAPTURE_BATCH": return await deliverCapture(message, sender);
      case "GRIDEDGE_RESUME_BOUNDARY": return await deliverCapture(message, sender, true);
      case "GRIDEDGE_CAPTURE_ERROR":
        await setStatus({ ok: false, kind: "CAPTURE_ERROR", provider: message.provider, page_url: message.page_url, error: message.message });
        return { ok: true };
      case "GRIDEDGE_GET_STATUS": {
        const current = await settings();
        const { last_status: lastStatus = null } = await chrome.storage.local.get("last_status");
        const database = await GridEdgeDurable.openDatabase();
        const effectiveStatus = lastStatus?.store_generation === GridEdgeDurable.DATABASE_NAME
          ? lastStatus
          : { ok: false, kind: "STORE_GENERATION_CHANGED", store_generation: GridEdgeDurable.DATABASE_NAME };
        return { settings: { ...current, mqtt_password: current.mqtt_password ? "***" : "" }, store: await GridEdgeDurable.status(database), last_status: effectiveStatus };
      }
      case "GRIDEDGE_GET_CAPTURE_STATE": {
        if (!allowedSender(sender)) throw new Error("capture-state sender is outside the reviewed page allowlist");
        const database = await GridEdgeDurable.openDatabase();
        try {
          return { ok: true, state: await GridEdgeDurable.sourceState(database, message.instrument) };
        } catch (error) {
          if (String(error?.message ?? error) === "source state does not exist") {
            return { ok: true, state: null };
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
