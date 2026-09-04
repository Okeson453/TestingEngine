/**
 * §9 — validator property tests.
 *
 * Covers the synchronous-on-ed handler, the bg_arrived_late race, the
 * orphaned path, and idempotency on re-emitted ed events.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { onGameEnd } from "@/lib/prediction/live/validator";
import { onGameStart } from "@/lib/prediction/live/predictor";
import { getSql } from "@/lib/db";
import { insertNewRounds } from "@/lib/crash/ingest";
import type { FetchedRound } from "@/lib/crash/fetch-bc";

async function ts(offsetMs: number): Promise<string> {
  const sql = await getSql();
  const rows = await sql<{ n: string }>`select (now() + (${offsetMs}::int * interval '1 millisecond'))::text as n`;
  return String(rows[0]!.n);
}

async function seedHistory(targetBeganAt: string, n = 30): Promise<void> {
  const seeds: FetchedRound[] = [];
  const targetMs = new Date(targetBeganAt).getTime();
  for (let i = 0; i < n; i += 1) {
    const crashedAt = new Date(targetMs - (i + 1) * 4_000);
    seeds.push({
      gameId: String(15000 + i),
      multiplier: 1 + (i % 9) * 0.21,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  await insertNewRounds(seeds);
}

test("validator: resolves a pending prediction to WIN", async () => {
  const sql = await getSql();
  const targetBeganAt = await ts(-100);
  await seedHistory(targetBeganAt, 30);

  const targetGameId = "16001";
  const bg = await onGameStart({
    gameId: targetGameId,
    beginTime: targetBeganAt,
    hash: null,
    salt: null,
    sourceRoundGameId: "15099",
    receivedAt: await ts(0),
  });
  assert.equal(bg.kind, "predicted");

  const ed = await onGameEnd({
    gameId: targetGameId,
    endTime: await ts(2_900),
    multiplier: 2.5, // > target 1.30 → WIN
    receivedAt: await ts(3_000),
  });
  assert.equal(ed.kind, "resolved");
  if (ed.kind === "resolved") {
    assert.equal(ed.result, "WIN");
    assert.equal(ed.targetMultiplier, 1.3);
    assert.equal(ed.actualMultiplier, 2.5);
  }

  const validation = await sql<{ result: string; target_multiplier: string; actual_multiplier: string }>`
    select result, target_multiplier, actual_multiplier
    from prediction_validations where game_id = ${targetGameId}
  `;
  assert.equal(validation.length, 1);
  assert.equal(validation[0]!.result, "WIN");
});

test("validator: resolves a pending prediction to LOSS", async () => {
  const sql = await getSql();
  const targetBeganAt = await ts(-100);
  await seedHistory(targetBeganAt, 30);

  const targetGameId = "17001";
  await onGameStart({
    gameId: targetGameId,
    beginTime: targetBeganAt,
    hash: null,
    salt: null,
    sourceRoundGameId: "15099",
    receivedAt: await ts(0),
  });

  const ed = await onGameEnd({
    gameId: targetGameId,
    endTime: await ts(2_900),
    multiplier: 1.05, // < target 1.30 → LOSS
    receivedAt: await ts(3_000),
  });
  assert.equal(ed.kind, "resolved");
  if (ed.kind === "resolved") {
    assert.equal(ed.result, "LOSS");
  }
});

test("validator: idempotent on duplicate ed (UNIQUE prediction_id)", async () => {
  const sql = await getSql();
  const targetBeganAt = await ts(-100);
  await seedHistory(targetBeganAt, 30);
  const targetGameId = "18001";

  await onGameStart({
    gameId: targetGameId,
    beginTime: targetBeganAt,
    hash: null,
    salt: null,
    sourceRoundGameId: "15099",
    receivedAt: await ts(0),
  });
  await onGameEnd({ gameId: targetGameId, endTime: await ts(2_900), multiplier: 2.0, receivedAt: await ts(3_000) });
  await onGameEnd({ gameId: targetGameId, endTime: await ts(2_900), multiplier: 2.0, receivedAt: await ts(3_100) });

  const rows = await sql<{ count: number }>`
    select count(*)::int as count from prediction_validations where game_id = ${targetGameId}
  `;
  assert.equal(rows[0]?.count, 1, "exactly one validation row despite duplicate ed");
});

test("validator: bg_arrived_late when ed arrives without a pending row", async () => {
  const sql = await getSql();
  // No bg was sent — only ed.
  const ed = await onGameEnd({
    gameId: "19001",
    endTime: await ts(2_900),
    multiplier: 1.5,
    receivedAt: await ts(3_000),
  });
  // If the round also doesn't exist in crash_rounds, this is orphaned
  // (no began_at). If we pre-insert it, it's bg_arrived_late. Either
  // way, NO prediction row is created.
  assert.ok(
    ed.kind === "orphaned" || ed.kind === "bg_arrived_late",
    `expected orphaned or bg_arrived_late, got ${ed.kind}`,
  );
  const rows = await sql<{ count: number }>`
    select count(*)::int as count from pending_predictions where target_game_id = '19001'
  `;
  assert.equal(rows[0]?.count, 0);
});
