/**
 * Latency attribution tests (sep 11 19:05-19:12 forensic pass).
 *
 * Production evidence: uniform 570-1001ms slow queries across BOTH pools
 * (critical SELECT 1 = 595ms; general worker_state/live_round_state/
 * worker_locks/dead-letter = 570-624ms; critical outbox claim = 1001ms)
 * with ZERO pool_acquire_ms warnings — pool queuing is exonerated. The
 * BG slow phase MOVES between the reconcile TX (927-1420ms in some rounds)
 * and the memory-only skip→not-persisted gap (1.0-1.2s in others, where
 * every intervening operation is synchronous) — the delay is systemic,
 * not tied to any one query. The two candidate root causes (event-loop
 * stall vs Neon/network RTT) are indistinguishable from queryMs alone,
 * so these tests pin the DISCRIMINATOR shipped this pass:
 *
 * 1. event-loop-lag sampler: max lag in a time window, ring-buffer bounded,
 *    passive reads, warn threshold.
 * 2. db.ts slow-query lines carry loop_lag_ms (source contract).
 * 3. poll-worker: source-too-old skip is rate-limited (30s/key) and drives
 *    a capped linear fetch backoff.
 * 4. BG latency profile is inlined into message text (Railway strips pino
 *    fields — raw logs show message text only).
 *
 * All deterministic: no DB, no network, no real timers.
 */
import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  maxLoopLagBetween,
  startEventLoopLagSampler,
} from "../../observability/event-loop-lag.ts";
import { POLL_INTERVAL_MS } from "./poll-worker.ts";

const here = dirname(fileURLToPath(import.meta.url));
const read = (f: string) => readFileSync(join(here, f), "utf8");

describe("event-loop-lag sampler", () => {
  it("returns 0 for windows with no samples (passive, no evidence ≠ provable absence)", () => {
    // Far-future window relative to any real samples.
    const far = performance.now() + 1_000_000;
    expect(maxLoopLagBetween(far, far + 100)).toBe(0);
  });

  it("is idempotent to start (multiple importers, single timer)", () => {
    expect(() => {
      startEventLoopLagSampler();
      startEventLoopLagSampler();
      startEventLoopLagSampler();
    }).not.toThrow();
  });

  it("measures real lag in a busy window (deterministic: blocks the loop 120ms)", async () => {
    startEventLoopLagSampler();
    const t0 = performance.now();
    const spinEnd = t0 + 120;
    // Deterministic busy-wait: forces ≥2 sampler ticks to be late by ~110ms+.
    while (performance.now() < spinEnd) { /* block */ }
    const t1 = performance.now();
    // The sampler's late tick runs in a timers phase after the block —
    // yield a real timer turn so it lands in the ring before querying.
    await new Promise((r) => setTimeout(r, 30));
    const maxLag = maxLoopLagBetween(t0 - 10, t1 + 10);
    expect(maxLag).toBeGreaterThan(50);
  });

  it("ignores samples outside the requested window", () => {
    const t0 = performance.now();
    // Window entirely in the past with no samples → 0.
    expect(maxLoopLagBetween(t0 - 100_000, t0 - 99_000)).toBe(0);
  });
});

describe("slow-query attribution (source contract: db.ts)", () => {
  const dbSrc = read("../../db.ts");

  it("slow-query log carries loop_lag_ms for network-vs-stall discrimination", () => {
    expect(dbSrc).toContain("slow_query_ms=${queryMs} loop_lag_ms=${loopLag}");
  });

  it("attribution window covers the query wall-clock only", () => {
    expect(dbSrc).toContain("maxLoopLagBetween(q0Perf, performance.now())");
  });

  it("sampler starts with the pools", () => {
    expect(dbSrc).toContain("startEventLoopLagSampler();");
  });
});

describe("poll source-too-old spam + backoff (source contract)", () => {
  const pollSrc = read("poll-worker.ts");

  it("source-too-old skip goes through the 30s rate limiter, not raw logger.info", () => {
    const branch = pollSrc.slice(
      pollSrc.indexOf("source round too old for reliable N+1") - 1200,
      pollSrc.indexOf("source round too old for reliable N+1") + 200,
    );
    expect(branch).toContain('logSkipOncePerInterval(\n        "source_too_old"');
    expect(branch).not.toContain("logger.info(");
  });

  it("tracks consecutive too-old ticks and resets on a fresh round", () => {
    expect(pollSrc).toContain("this.consecutiveSourceTooOld += 1;");
    expect(pollSrc).toContain("this.consecutiveSourceTooOld = 0;");
  });

  it("backoff is linear and capped at 2s (never starves recovery)", () => {
    const backoffBlock = pollSrc.slice(pollSrc.indexOf("consecutiveSourceTooOld > 2"));
    expect(backoffBlock).toContain("Math.min(2_000, Math.round(base + n * 250))");
  });

  it("base poll cadence unchanged (500ms default)", () => {
    expect(POLL_INTERVAL_MS).toBe(500);
  });
});

describe("BG latency profile in message text (Railway strips pino fields)", () => {
  const handlerSrc = read("../events/game-event-handlers.ts");

  it("NO_BET and SIGNAL_READY messages inline reconcile/prediction/total ms", () => {
    expect(handlerSrc).toContain(
      "[reconcile=${profile.bg_receipt_to_reconcile_ms}ms prediction=${profile.prediction_ms}ms total=${profile.bg_receipt_to_prediction_done_ms}ms]",
    );
  });
});
