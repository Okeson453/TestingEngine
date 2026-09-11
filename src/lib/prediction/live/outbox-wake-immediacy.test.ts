/**
 * Deterministic latency tests — outbox wake immediacy (pure, no DB).
 *
 * The dispatcher must react to a newly durable outbox row IMMEDIATELY via
 * the wake channel, never by waiting out the recovery tick. These tests pin
 * the contract:
 *
 *   1. a notify with no waiter latches — the NEXT wait returns immediately
 *      (a wake is never lost between the durable commit and the dispatcher
 *      entering its wait);
 *   2. a notify while a waiter is pending resolves that waiter within one
 *      microtask burst (wake→claim latency is not a timer);
 *   3. bursts coalesce — N notifies resolve the pending waiter exactly once;
 *   4. the timeout is a RECOVERY fallback only (a healthy wake beats it);
 *   5. lane kinds are carried faithfully (prediction vs normal).
 *
 * Pure JS — runs in any test runner (node --test, vitest, bun).
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  notifyOutbox,
  waitForOutboxWake,
  getWakeStats,
  _resetOutboxWakeForTests,
} from "./outbox-wake.ts";

test("wake: notify with no waiter latches — next wait resolves immediately", () => {
  _resetOutboxWakeForTests();
  notifyOutbox("prediction");
  const t0 = Date.now();
  // No timeout: if the latch were lost this would hang — guard with a timer
  // and assert the resolved kinds.
  return Promise.race([
    waitForOutboxWake().then((k) => {
      assert.equal(k.prediction, true);
      assert.equal(k.normal, false);
      assert.ok(Date.now() - t0 < 50, "latched wake must resolve immediately");
    }),
    new Promise<never>((_, rej) =>
      setTimeout(() => rej(new Error("latched wake lost — wait hung")), 1_000),
    ),
  ]).then(() => undefined);
});

test("wake: notify resolves a pending waiter within one microtask burst", async () => {
  _resetOutboxWakeForTests();
  const waitP = waitForOutboxWake();
  // The dispatcher is now parked in its wait. A producer commit lands.
  await new Promise((r) => setTimeout(r, 10));
  const t0 = Date.now();
  notifyOutbox("prediction");
  const kinds = await waitP;
  const elapsed = Date.now() - t0;
  assert.equal(kinds.prediction, true);
  assert.ok(elapsed < 50, `wake→waiter must be immediate, took ${elapsed}ms`);
});

test("wake: a burst of notifies coalesces into one wake", async () => {
  _resetOutboxWakeForTests();
  let resolved = 0;
  const waitP = waitForOutboxWake().then((k) => {
    resolved += 1;
    return k;
  });
  await new Promise((r) => setTimeout(r, 5));
  notifyOutbox("prediction");
  notifyOutbox("prediction");
  notifyOutbox("prediction");
  const kinds = await waitP;
  assert.equal(kinds.prediction, true);
  // Give any straggler microtasks a chance to run — only one wake total.
  await new Promise((r) => setTimeout(r, 10));
  assert.equal(resolved, 1, "bursts must coalesce into a single drain");
});

test("wake: timeout is the recovery fallback, a wake beats it", async () => {
  _resetOutboxWakeForTests();
  const waitP = waitForOutboxWake(30_000);
  await new Promise((r) => setTimeout(r, 5));
  notifyOutbox("normal");
  const t0 = Date.now();
  const kinds = await waitP;
  assert.ok(Date.now() - t0 < 50, "wake must beat the 30s fallback timer");
  assert.equal(kinds.normal, true);
  assert.equal(kinds.prediction, false);
});

test("wake: pure timeout resolves with no kinds latched", async () => {
  _resetOutboxWakeForTests();
  const t0 = Date.now();
  const kinds = await waitForOutboxWake(30);
  assert.ok(Date.now() - t0 >= 25, "timeout wait must actually wait");
  assert.equal(kinds.prediction, false);
  assert.equal(kinds.normal, false);
});

test("wake: stats count per lane", () => {
  _resetOutboxWakeForTests();
  const before = getWakeStats();
  notifyOutbox("prediction");
  notifyOutbox("normal");
  const after = getWakeStats();
  assert.equal(after.predictionWakeCount, before.predictionWakeCount + 1);
  assert.equal(after.normalWakeCount, before.normalWakeCount + 1);
  assert.ok(after.lastPredictionNotifyAt != null);
  assert.ok(after.lastNormalNotifyAt != null);
});
