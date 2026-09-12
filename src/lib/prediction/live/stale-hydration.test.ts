/**
 * STALE_REJECTED cold-start race — reproduction + fix verification.
 *
 * Production 15:46:56, target 9603950: a crash (round 9603949) landed while
 * the worker had no wired handlers (deploy / lease wait). The ACIE snapshot
 * restored lastSourceGameId = an older round, so the first BG prediction's
 * freshness source (the crash_rounds tail) mismatched and BG rejected to ED.
 *
 * Fix under test (guard NOT weakened):
 *   1. hydrateAcieFreshnessSource — observe the EXACT missing source round
 *      from its authoritative crash_rounds row, then re-assert. Refuses to
 *      hydrate a source OLDER than the last observation (would drag
 *      provenance backwards).
 *   2. hydrateAcieTailFromCrashRounds — boot-time hydration of every
 *      crash_rounds row newer than the snapshot's last-observed game,
 *      oldest→newest, ending with the newest as lastObservedGameId.
 *
 * Emission invariant: assertFreshAcieState(source) is ok:true ONLY after
 * ACIE observed the true source crash — hydration adds the missing real
 * observation, it never bypasses the check.
 */

import { describe, it, expect, beforeEach } from "vitest";
import {
  recordAcieObservation,
  assertFreshAcieState,
  resetStaleGuardForTests,
  getLastAcieObservation,
} from "@/lib/prediction/acie/stale-guard";
import {
  hydrateAcieFreshnessSource,
  hydrateAcieTailFromCrashRounds,
} from "@/lib/prediction/live/predictor";
import { getSharedACIEEngine, setSharedACIEEngineForTests } from "@/lib/prediction/acie/shared-engine";
import type { Sql } from "@/lib/db";

type CrashRow = { game_id: string; multiplier: string | null; crashed_at: string | Date };

/** Fake sql: a template-tag callable returning canned rows per call index. */
function fakeSql(pages: CrashRow[][]): Sql & { calls: number } {
  let calls = 0;
  const fn = (() => {
    const rows = pages[Math.min(calls, pages.length - 1)] ?? [];
    calls += 1;
    return Promise.resolve(rows);
  }) as unknown as Sql & { calls: number };
  fn.calls = 0;
  Object.defineProperty(fn, "calls", { get: () => calls });
  return fn;
}

const ROW = (id: string, mult: string): CrashRow => ({
  game_id: id,
  multiplier: mult,
  crashed_at: new Date("2026-09-12T15:46:00Z"),
});

describe("STALE_REJECTED cold-start race (9603950)", () => {
  beforeEach(() => {
    setSharedACIEEngineForTests(null);
    resetStaleGuardForTests();
    getSharedACIEEngine(); // fresh shared engine with empty history
  });

  it("reproduces the production failure: restored snapshot game ≠ first live source", () => {
    // Boot seeded provenance from the (older) ACIE snapshot…
    recordAcieObservation("9603948", 4);
    // …but the crash of 9603949 happened before handlers were wired, so the
    // first BG prediction for target 9603950 asserts freshness on 9603949.
    const check = assertFreshAcieState("9603949");
    expect(check.ok).toBe(false);
    if (!check.ok) expect(check.reason).toBe("source_mismatch");
  });

  it("hydrates the exact missing source from crash_rounds and re-proves freshness", async () => {
    recordAcieObservation("9603948", 4);
    const sql = fakeSql([[ROW("9603949", "1.4210")]]);

    const ok = await hydrateAcieFreshnessSource("9603949", async () => sql);

    expect(ok).toBe(true);
    expect(getLastAcieObservation().gameId).toBe("9603949");
    // The invariant the guard exists for: N+1 may now emit ONLY because the
    // true source crash was observed in-process.
    const check = assertFreshAcieState("9603949");
    expect(check.ok).toBe(true);
    if (check.ok) expect(check.observationCount).toBeGreaterThan(0);
  });

  it("refuses to hydrate a source OLDER than the last observation (no backwards drift)", async () => {
    recordAcieObservation("9603960", 9);
    const sql = fakeSql([[ROW("9603949", "1.4210")]]);

    const ok = await hydrateAcieFreshnessSource("9603949", async () => sql);

    expect(ok).toBe(false);
    expect(getLastAcieObservation().gameId).toBe("9603960");
    // Freshness for the CURRENT source is untouched — still proven fresh.
    expect(assertFreshAcieState("9603960").ok).toBe(true);
  });

  it("returns false and leaves state untouched when crash_rounds has no such row", async () => {
    recordAcieObservation("9603948", 4);
    const sql = fakeSql([[]]);

    const ok = await hydrateAcieFreshnessSource("9603949", async () => sql);

    expect(ok).toBe(false);
    expect(getLastAcieObservation().gameId).toBe("9603948");
    expect(assertFreshAcieState("9603949").ok).toBe(false);
  });

  it("boot tail hydration observes every missed round oldest→newest (newest wins)", async () => {
    recordAcieObservation("9603948", 4);
    const sql = fakeSql([[ROW("9603950", "1.9000"), ROW("9603949", "1.4210")]]);

    const n = await hydrateAcieTailFromCrashRounds(sql as unknown as Sql);

    expect(n).toBe(2);
    // Last-observed must be the NEWEST round, not the last row returned.
    expect(getLastAcieObservation().gameId).toBe("9603950");
    expect(assertFreshAcieState("9603950").ok).toBe(true);
  });
});
