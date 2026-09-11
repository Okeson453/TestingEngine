/**
 * Pool priority/isolation regression tests (sep 11 pool pass).
 *
 * Locks the measured evidence behind the 1.5-1.8s NO_BET result lag and the
 * general pool pinning at total=max with ~1.0-1.16s acquires:
 *
 *   1. Pool sizing math — critical >= 4, general >= 8, cap 12, min warm 3.
 *   2. Source contract — the BG reconcile CTE (began_at stamp + round state +
 *      temporal kill) runs on the CRITICAL pool; forensic reclassify stays on
 *      the GENERAL pool and commits all row outcomes in ONE batched statement.
 *   3. Invariant probes fan out concurrently (Promise.all), not sequentially.
 *
 * All assertions are pure (env math + source contract). No DB, no network —
 * runs under any runner, including bun.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  readTotalMax,
  readCriticalMax,
  readGeneralMax,
} from "@/lib/db";

const handlersSrc = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "../events/game-event-handlers.ts"),
  "utf8",
);
const forensicsSrc = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "delivery-forensics.ts"),
  "utf8",
);
const invariantsSrc = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "invariants.ts"),
  "utf8",
);

function withEnv(vars: Record<string, string>, fn: () => void): void {
  const saved: Record<string, string | undefined> = {};
  for (const k of Object.keys(vars)) {
    saved[k] = process.env[k];
    process.env[k] = vars[k];
  }
  try {
    fn();
  } finally {
    for (const [k, v] of Object.entries(saved)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
}

describe("pool sizing (sep 11 pass)", () => {
  beforeEach(() => {
    delete process.env.PG_POOL_MAX;
    delete process.env.PG_CRITICAL_POOL_MAX;
    delete process.env.PG_POOL_MIN_IDLE;
  });

  it("defaults: total 12 split critical 4 / general 8", () => {
    expect(readTotalMax()).toBe(12);
    expect(readCriticalMax()).toBe(4);
    expect(readGeneralMax()).toBe(8);
  });

  it("critical always keeps at least one slot and never exceeds total-1", () => {
    withEnv({ PG_CRITICAL_POOL_MAX: "99", PG_POOL_MAX: "4" }, () => {
      expect(readCriticalMax()).toBeLessThan(readTotalMax());
      expect(readCriticalMax()).toBeGreaterThanOrEqual(1);
    });
  });

  it("hard cap 12 protects the Neon connection budget", () => {
    withEnv({ PG_POOL_MAX: "999" }, () => {
      expect(readTotalMax()).toBe(12);
    });
  });

  it("general = total - critical always", () => {
    for (const total of [2, 6, 12]) {
      withEnv({ PG_POOL_MAX: String(total) }, () => {
        expect(readGeneralMax() + readCriticalMax()).toBe(readTotalMax());
      });
    }
  });
});

describe("realtime vs background pool isolation (source contract)", () => {
  it("BG reconcile CTE runs on the critical pool", () => {
    expect(handlersSrc).toContain("await getCriticalSql()");
    // The reconcile comment anchors the critical-pool acquire to the BG CTE.
    expect(handlersSrc).toMatch(/PRIORITY-ISOLATION FIX[\s\S]*?const sql = await getCriticalSql\(\)/);
  });

  it("forensic reclassify stays on the general pool", () => {
    expect(handlersSrc).toMatch(/Telemetry, not realtime[\s\S]*?const generalSql = await getSql\(\)/);
  });

  it("reclassify commits outcomes in ONE batched statement, not per-row awaits", () => {
    expect(forensicsSrc).toContain("UPDATE notification_outbox o");
    expect(forensicsSrc).toContain("AS v(notification_id, outcome, lead_time_ms)");
    // The old per-row loop must be gone: no persistDeliveryOutcome call left
    // inside reclassifyOnTargetStart's delivered-rows loop.
    const reclassifyBody = forensicsSrc.slice(
      forensicsSrc.indexOf("export async function reclassifyOnTargetStart"),
    );
    expect(reclassifyBody).not.toContain("await persistDeliveryOutcome");
    // The write is skipped entirely when nothing was delivered.
    expect(reclassifyBody).toContain("if (classified.length > 0)");
  });

  it("invariant probes fan out concurrently via Promise.all", () => {
    expect(invariantsSrc).toContain("const probes: Promise<void>[] = [];");
    expect(invariantsSrc).toContain("await Promise.all(probes);");
  });
});
