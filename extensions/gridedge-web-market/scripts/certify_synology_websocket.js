#!/usr/bin/env node
"use strict";

const { execFileSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const mqtt = require("mqtt");

require("../src/shared.js");
const provider = require("../src/providers/eastmoney.js");
const durable = require("../src/durable.js");

const root = path.join(__dirname, "..");
const fixture = JSON.parse(fs.readFileSync(path.join(root, "fixtures/eastmoney-time-sales-page1.json"), "utf8"));
const passwordPath = path.join(os.homedir(), "Library/Application Support/GridEdge-T/market-mqtt/publisher.password");

function publish(client, event) {
  return new Promise((resolve, reject) => {
    client.publish(
      event.mqtt_topic,
      event.payload,
      { qos: 1, retain: false, properties: { contentType: "application/json" } },
      (error) => error ? reject(error) : resolve(),
    );
  });
}

function presentEventIds(eventIds) {
  const quoted = eventIds.map((eventId) => `'${eventId}'`).join(",");
  const sql = `SELECT encode(event_id,'hex') FROM market_events WHERE encode(event_id,'hex') IN (${quoted}) ORDER BY 1`;
  const shellSql = `'${sql.replaceAll("'", `'"'"'`)}'`;
  const output = execFileSync("ssh", [
    "192.168.1.201",
    `/usr/local/bin/docker exec gridedge-market-postgres psql -U gridedge_market -d gridedge_market -Atqc ${shellSql}`,
  ], { encoding: "utf8" });
  return output.trim().split("\n").filter(Boolean);
}

async function main() {
  const capture = provider.parseSnapshot(fixture);
  const sourceInstanceId = crypto.randomUUID();
  const events = [];
  for (let index = 0; index < capture.rows.length; index += 1) {
    events.push(await durable.canonicalEvent(
      capture,
      capture.rows[index],
      sourceInstanceId,
      index + 1,
      "eastmoney-web-time-sales-cert",
    ));
  }
  const client = await mqtt.connectAsync("ws://192.168.1.201:9001/mqtt", {
    protocolVersion: 5,
    clean: true,
    clientId: `gridedge-web-cert-${crypto.randomUUID()}`,
    username: "gridedge-publisher",
    password: fs.readFileSync(passwordPath, "utf8").trim(),
    reconnectPeriod: 0,
    connectTimeout: 10000,
  });
  try {
    for (const event of events) await publish(client, event);
  } finally {
    await client.endAsync();
  }
  const expected = events.map((event) => event.event_id).sort();
  let actual = [];
  for (let attempt = 0; attempt < 20; attempt += 1) {
    actual = presentEventIds(expected);
    if (actual.length === expected.length) break;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`PostgreSQL event IDs differ: expected=${expected} actual=${actual}`);
  }
  process.stdout.write(`${JSON.stringify({ ok: true, source_instance_id: sourceInstanceId, event_ids: expected }, null, 2)}\n`);
}

main().catch((error) => {
  process.stderr.write(`${error.stack ?? error}\n`);
  process.exitCode = 1;
});
