#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const core = require("../src/shared.js");
globalThis.GridEdgeMarket = core;
const eastmoney = require("../src/providers/eastmoney.js");

const fixture = JSON.parse(
  fs.readFileSync(path.join(__dirname, "../fixtures/eastmoney-time-sales-page1.json"), "utf8"),
);
process.stdout.write(core.canonicalJson(eastmoney.parseSnapshot(fixture)));
