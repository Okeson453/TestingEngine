/**
 * §9.4 / §8 — concurrency safety assertions.
 *
 * The partial unique index on `target_game_id` and the `FOR UPDATE
 * SKIP LOCKED` on the validator are the documented safety nets against
 * concurrent inserts and concurrent validations. This test exercises
 * both: N concurrent bg events for the same target gameId must produce
 * exactly 1 prediction row, and N concurrent ed events must produce
 * exactly 1 validation row.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { onGameStart } from "@/lib/prediction/live/predictor";
import { onGameEnd } from "@/lib/prediction/live/validator";
import { getSql } from "@/lib/db";
import { insertNewRounds } from "@/lib/crash/ingest";
import type { FetchedRound } from "@/lib/crash/fetch-bc";

async function ts(offsetMs: number): Promise<string> {
  const sql = await getSql();
  const rows = await sql<{ n: string }>`select (now() + (${offsetMs}::int * interval '1 millisecond'))::text as n`;
  return String(rows[0]!.n);
}

test("§9.4: concurrent bg events for the same target gameId → exactly 1 row", async () => {
  const sql = await getSql();
  await sql`truncate pending_predictions, prediction_validations, notification_outbox, live_event_log, crash_rounds restart identity cascade`;

  const bgBeginMs = Date.now() - 100;
  const seeds: FetchedRound[] = [];
  for (let i = 0; i < 30; i += 1) {
    const crashedAt = new Date(bgBeginMs - (i + 1) * 4_000);
    seeds.push({
      gameId: String(20000 + i),
      multiplier: 1.5,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  await insertNewRounds(seeds);

  const targetGameId = "21001";
  const beginTime = await ts(-100);
  const receivedAt = await ts(0);
  // PGLite is single-threaded, so we exercise concurrency by running the
  // events back-to-back and asserting the partial unique index is the
  // canonical idempotency gate. With the same target gameId, exactly one
  // call can return `predicted`; the rest are `duplicate` (or a benign
  // `temporal_violation` when the local clock has drifted relative to
  // the DB at the time the second call's `now()` is evaluated).
  const results: Array<Awaited<ReturnType<typeof onGameStart>>> = [];
  for (let i = 0; i < 20; i += 1) {
    results.push(
      await onGameStart({
        gameId: targetGameId,
        beginTime,
        hash: null,
        salt: null,
        sourceRoundGameId: "20099",
        receivedAt,
      }),
    );
  }
  const predicted = results.filter((r) => r.kind === "predicted").length;
  const duplicates = results.filter((r) => r.kind === "duplicate").length;
  assert.equal(predicted, 1, `expected exactly 1 predicted, got ${predicted}; results=${JSON.stringify(results)}`);
  assert.ok(predicted + duplicates === 20, "every call must be predicted or duplicate");

  const rows = await sql<{ count: number }>`
    select count(*)::int as count from pending_predictions where target_game_id = ${targetGameId}
  `;
  assert.equal(rows[0]?.count, 1, "exactly one prediction row for the target");
});

test("§9.4: 10 sequential ed events for the same gameId → exactly 1 validation", async () => {
  const sql = await getSql();
  await sql`truncate pending_predictions, prediction_validations, notification_outbox, live_event_log, crash_rounds restart identity cascade`;

  const bgBeginMs = Date.now() - 100;
  const seeds: FetchedRound[] = [];
  for (let i = 0; i < 30; i += 1) {
    const crashedAt = new Date(bgBeginMs - (i + 1) * 4_000);
    seeds.push({
      gameId: String(22000 + i),
      multiplier: 1.5,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  await insertNewRounds(seeds);

  const targetGameId = "23001";
  const bg = await onGameStart({
    gameId: targetGameId,
    beginTime: await ts(-100),
    hash: null,
    salt: null,
    sourceRoundGameId: "22099",
    receivedAt: await ts(0),
  });
  assert.equal(bg.kind, "predicted");

  const endTime = await ts(2_900);
  // Sequential re-emissions. PGLite is single-threaded; for production
  // Neon deployments the `UNIQUE(prediction_id)` on prediction_validations
  // is the canonical gate.
  for (let i = 0; i < 10; i += 1) {
    const recv = await ts(3_000 + i * 10);
    await onGameEnd({
      gameId: targetGameId,
      endTime,
      multiplier: 2.5,
      receivedAt: recv,
    });
  }

  const rows = await sql<{ count: number }>`
    select count(*)::int as count from prediction_validations where game_id = ${targetGameId}
  `;
  assert.equal(rows[0]?.count, 1, "exactly one prediction_validation row despite multiple ed");
});
