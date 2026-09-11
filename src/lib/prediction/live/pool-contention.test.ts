/**
 * General-pool contention regression tests (sep 11 pool pass 2).
 *
 * Production evidence (17:33:00-17:35:26, general max=6): waiting=1-3 queue
 * blips every ~30s, each ~1 RTT (~170ms), plus POOL PRESSURE at 17:34:54.
 * Traced to the invariant monitor: five concurrent read probes + heartbeat
 * CTE + event-log inserts = 6+ simultaneous slot demands = the pool max.
 *
 * Fix: invariant probes run under a concurrency cap of 2 (worst-case monitor
 * occupancy ~2-3 slots for ~450ms per 30s cycle) and the predictor's
 * live_event_log audit insert moved off the critical pool to general.
 *
 * All tests here are deterministic (no DB, no network): the runner is
 * exercised with fake tasks that track in-flight concurrency.
 */
import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  runWithConcurrency,
  INVARIANT_PROBE_CONCURRENCY,
} from "./invariants.ts";

const predictorSrc = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "predictor.ts"),
  "utf8",
);

describe("runWithConcurrency (bounded fan-out)", () => {
  it("never exceeds the concurrency limit", async () => {
    let inflight = 0;
    let peak = 0;
    const tasks = Array.from({ length: 20 }, () => async () => {
      inflight += 1;
      peak = Math.max(peak, inflight);
      await new Promise((r) => setTimeout(r, 5));
      inflight -= 1;
    });
    await runWithConcurrency(tasks, 2);
    expect(peak).toBeLessThanOrEqual(2);
  });

  it("runs every task exactly once", async () => {
    const ran: number[] = [];
    const tasks = Array.from({ length: 9 }, (_, i) => async () => {
      ran.push(i);
    });
    await runWithConcurrency(tasks, 3);
    expect(ran.sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8]);
  });

  it("uses fewer workers than the limit when tasks are few", async () => {
    let started = 0;
    await runWithConcurrency(
      [async () => { started += 1; }],
      5,
    );
    expect(started).toBe(1);
  });

  it("keeps a task's rejection isolated to its own slot (later tasks still run)", async () => {
    const ran: number[] = [];
    const tasks = Array.from({ length: 4 }, (_, i) => async () => {
      ran.push(i);
      if (i === 1) throw new Error("probe 1 failed");
    });
    await expect(runWithConcurrency(tasks, 2)).rejects.toThrow("probe 1 failed");
    // Workers keep pulling until their own await rejects; the failed worker
    // dies but the other continues draining the queue.
    expect(ran.length).toBeGreaterThanOrEqual(3);
  });
});

describe("invariant monitor configuration", () => {
  it("probe concurrency is capped at 2 (evidence: 30s waiting=1-3 blips at general max=6)", () => {
    expect(INVARIANT_PROBE_CONCURRENCY).toBe(2);
  });

  it("sampleProductionInvariants uses the capped runner, not unbounded Promise.all", () => {
    const src = readFileSync(
      join(dirname(fileURLToPath(import.meta.url)), "invariants.ts"),
      "utf8",
    );
    const body = src.slice(src.indexOf("export async function sampleProductionInvariants"));
    expect(body).toContain("runWithConcurrency(probes, INVARIANT_PROBE_CONCURRENCY)");
    expect(body).not.toContain("await Promise.all(probes)");
  });
});

describe("realtime path telemetry isolation (source contract)", () => {
  it("predictor live_event_log audit insert runs on the general pool, not critical", () => {
    // The durable-handoff block's `sql` is critical; the audit insert must
    // fetch its own general-pool handle.
    const anchor = predictorSrc.indexOf("TELEMETRY POOL FIX");
    expect(anchor).toBeGreaterThan(-1);
    const block = predictorSrc.slice(anchor, anchor + 2000);
    expect(block).toContain("await getSql()");
    expect(block).not.toContain("void sql`");
  });
});
