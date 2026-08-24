"use strict";

const enabled = document.querySelector("#enabled");
const mqttUrl = document.querySelector("#mqtt-url");
const mqttUsername = document.querySelector("#mqtt-username");
const mqttPassword = document.querySelector("#mqtt-password");
const result = document.querySelector("#result");

async function load() {
  const stored = await chrome.storage.local.get(["enabled", "mqtt_url", "mqtt_username"]);
  enabled.checked = stored.enabled === true;
  mqttUrl.value = stored.mqtt_url ?? "ws://192.168.1.201:9001/mqtt";
  mqttUsername.value = stored.mqtt_username ?? "gridedge-publisher";
}

document.querySelector("#save").addEventListener("click", async () => {
  const parsed = new URL(mqttUrl.value);
  if (parsed.protocol !== "ws:" || parsed.hostname !== "192.168.1.201" ||
      parsed.port !== "9001" || parsed.pathname !== "/mqtt") {
    result.textContent = "地址必须是 ws://192.168.1.201:9001/mqtt";
    return;
  }
  if (mqttUsername.value !== "gridedge-publisher") {
    result.textContent = "当前仅允许 gridedge-publisher";
    return;
  }
  const update = {
    enabled: enabled.checked,
    mqtt_url: parsed.href,
    mqtt_username: mqttUsername.value,
  };
  if (mqttPassword.value) update.mqtt_password = mqttPassword.value;
  await chrome.storage.local.set(update);
  mqttPassword.value = "";
  result.textContent = "已保存";
});
void load();
