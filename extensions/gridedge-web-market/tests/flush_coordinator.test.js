"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const { createFlushCoordinator } = require("../src/flush_coordinator.js");

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

test("a capture arriving at the tail of an in-flight flush forces a second drain", async () => {
  const first = deferred();
  const calls = [];
  const coordinator = createFlushCoordinator(async () => {
    calls.push(calls.length + 1);
    if (calls.length === 1) return await first.promise;
    return { ok: true, published: 1 };
  });

  const a = coordinator.request();
  const b = coordinator.request();
  first.resolve({ ok: true, published: 1 });

  assert.deepEqual(await a, { ok: true, published: 1 });
  assert.equal(await b, await a);
  assert.deepEqual(calls, [1, 2]);
});

test("a requested trailing drain survives one in-flight failure without waiting for an alarm", async () => {
  const first = deferred();
  const calls = [];
  const errors = [];
  const coordinator = createFlushCoordinator(async () => {
    calls.push(calls.length + 1);
    if (calls.length === 1) return await first.promise;
    return { ok: true, published: 1 };
  }, {
    async onError(error) {
      errors.push(error.message);
    },
  });

  const a = coordinator.request();
  const b = coordinator.request();
  first.reject(new Error("database ACK timeout"));

  assert.deepEqual(await a, { ok: true, published: 1 });
  assert.equal(await b, await a);
  assert.deepEqual(calls, [1, 2]);
  assert.deepEqual(errors, ["database ACK timeout"]);
});

test("an isolated flush failure remains visible and a later request creates a fresh runner", async () => {
  let calls = 0;
  const coordinator = createFlushCoordinator(async () => {
    calls += 1;
    if (calls === 1) throw new Error("database ACK timeout");
    return { ok: true, published: 1 };
  });

  await assert.rejects(coordinator.request(), /database ACK timeout/);
  assert.deepEqual(await coordinator.request(), { ok: true, published: 1 });
  assert.equal(calls, 2);
});
