/**
 * §9.7 — cold-start recovery assertion.
 *
 * On a freshly migrated empty database, the ColdStartSeeder populates ≥
 * 100 crash_rounds rows within 10 seconds of boot.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { getSql } from "@/lib/db";
import { runColdStartSeeder } from "@/lib/prediction/live/cold-start-seeder";
import type { FetchedRound } from "@/lib/crash/fetch-bc";

const fixedNow = Date.parse("2026-09-03T12:00:00.000Z");

function makeFakeHistory(pages: number): FetchedRound[] {
  const rows: FetchedRound[] = [];
  for (let i = 0; i < pages * 20; i += 1) {
    const crashedAt = new Date(fixedNow - (i + 1) * 4_000);
    rows.push({
      gameId: String(7000 + i),
      multiplier: 1 + (i % 13) * 0.13,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  return rows;
}

test("§9.7: cold-start seeder populates ≥ 100 rows in ≤ 10s on empty DB", async () => {
  const sql = await getSql();
  // Wipe and re-seed.
  await sql`delete from pending_predictions`;
  await sql`delete from crash_rounds`;

  const before = await sql<{ count: number }>`select count(*)::int as count from crash_rounds`;
  assert.equal(before[0]?.count, 0, "expected empty crash_rounds before seed");

  const result = await runColdStartSeeder({
    fetchHistory: async (_pages: number) => makeFakeHistory(5),
    now: () => Date.now(),
  });

  assert.equal(result.alreadySeeded, false);
  assert.ok(result.elapsedMs <= 10_000, `elapsed ${result.elapsedMs}ms > 10000ms`);
  assert.ok(result.finalCount >= 100, `expected ≥ 100 rows, got ${result.finalCount}`);

  const after = await sql<{ count: number }>`select count(*)::int as count from crash_rounds`;
  assert.ok((after[0]?.count ?? 0) >= 100);
});

test("§9.7: cold-start seeder is a no-op when history is sufficient", async () => {
  const sql = await getSql();
  await sql`delete from crash_rounds`;

  // Pre-populate to satisfy MIN_HISTORY without invoking fetch.
  const seeds: FetchedRound[] = [];
  for (let i = 0; i < 100; i += 1) {
    const crashedAt = new Date(Date.now() - (i + 1) * 4_000);
    seeds.push({
      gameId: String(8000 + i),
      multiplier: 1.5,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  const { insertNewRounds } = await import("@/lib/crash/ingest");
  await insertNewRounds(seeds);

  const fetchCalls: number[] = [];
  const result = await runColdStartSeeder({
    fetchHistory: async (pages: number) => {
      fetchCalls.push(pages);
      return [];
    },
    now: () => Date.now(),
  });
  assert.equal(result.alreadySeeded, true);
  assert.equal(fetchCalls.length, 0, "no fetch should occur when history is sufficient");
});
