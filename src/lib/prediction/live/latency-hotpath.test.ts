/**
 * Latency hot-path regressions (no Neon required).
 * - Outbox wake resolves immediately (not TICK-bound)
 * - Ownership reserve is synchronous (BG before ED)
 * - Stage metadata keys required for forensics are present in the contract
 */
import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";
import {
  notifyOutbox,
  waitForOutboxWake,
  _resetOutboxWakeForTests,
  getWakeStats,
} from "./outbox-wake.ts";
import {
  reserveTargetForBg,
  claimTarget,
  _resetTargetCoordinatorForTests,
} from "./target-coordinator.ts";
import { TICK_MS } from "./notification-worker.ts";
import { classifyDelivery } from "./delivery-forensics.ts";

beforeEach(() => {
  _resetOutboxWakeForTests();
  _resetTargetCoordinatorForTests();
});

describe("outbox wake latency", () => {
  it("notifyOutbox resolves waiter in << TICK_MS (immediate drain)", async () => {
    const t0 = Date.now();
    const waitP = waitForOutboxWake(TICK_MS);
    // Producer enqueues after waiter is armed
    setTimeout(() => notifyOutbox("prediction"), 5);
    const kinds = await waitP;
    const elapsed = Date.now() - t0;
    assert.equal(kinds.prediction, true);
    assert.ok(elapsed < Math.min(TICK_MS, 200), `wake took ${elapsed}ms (TICK_MS=${TICK_MS})`);
  });

  it("latched wake returns immediately without timer", async () => {
    notifyOutbox("prediction");
    const t0 = Date.now();
    const kinds = await waitForOutboxWake(TICK_MS);
    const elapsed = Date.now() - t0;
    assert.equal(kinds.prediction, true);
    assert.ok(elapsed < 50, `latched wake took ${elapsed}ms`);
    assert.ok(getWakeStats().predictionWakeCount >= 1);
  });
});

describe("BG ownership before any await", () => {
  it("reserve is synchronous and blocks ED", () => {
    const t0 = Date.now();
    const r = reserveTargetForBg("99001", "99000");
    const reserveMs = Date.now() - t0;
    assert.equal(r.owned, true);
    assert.ok(reserveMs < 5, `reserve took ${reserveMs}ms`);
    const ed = claimTarget("99001", "ed:99000");
    assert.equal(ed.owned, false);
    assert.equal(ed.blockedByBg, true);
  });
});

describe("delivery forensics classification", () => {
  it("does not return UNKNOWN when both stamps exist", () => {
    const now = Date.now();
    const { outcome, leadTimeMs } = classifyDelivery({
      telegramAcceptedAtMs: now,
      targetStartedAtMs: now + 5_000,
      outboxStatus: "delivered",
    });
    assert.ok(outcome === "EARLY" || outcome === "ON_TIME");
    assert.ok(leadTimeMs != null && leadTimeMs > 0);
  });

  it("UNKNOWN only when target start missing (honest)", () => {
    const { outcome } = classifyDelivery({
      telegramAcceptedAtMs: Date.now(),
      targetStartedAtMs: null,
      outboxStatus: "delivered",
    });
    assert.equal(outcome, "UNKNOWN");
  });
});

describe("TICK_MS recovery default", () => {
  it("default recovery tick is <= 100ms (wake remains primary)", () => {
    assert.ok(TICK_MS <= 100, `TICK_MS=${TICK_MS} should be recovery-fast`);
  });
});

describe("outbox wake coalescing under burst", () => {
  it("multiple notifyOutbox collapse to one latched prediction wake", async () => {
    notifyOutbox("prediction");
    notifyOutbox("prediction");
    notifyOutbox("normal");
    const kinds = await waitForOutboxWake(50);
    assert.equal(kinds.prediction, true);
    assert.equal(kinds.normal, true);
  });
});
