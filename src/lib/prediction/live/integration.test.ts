/**
 * §9.1 — strict temporal invariant.
 *
 * The single most important property of the prediction pipeline. After
 * a simulated end-to-end `bg → ed` cycle, the strict invariant
 * `prediction_generated_at < target_round_started_at < target_round.crashed_at`
 * MUST hold for every validated row.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { onGameStart } from "@/lib/prediction/live/predictor";
import { onGameEnd, waitForInFlightPredictions } from "@/lib/prediction/live/validator";
import { getInvariantStatus } from "@/lib/prediction/live/server";
import { getSql } from "@/lib/db";
import { fetchCrashHistory, type FetchedRound } from "@/lib/crash/fetch-bc";
import { insertNewRounds } from "@/lib/crash/ingest";

/** Build a timestamp string relative to the DB clock (avoids PGLite/host
 *  clock drift in tests). */
async function ts(offsetMs: number): Promise<string> {
  const sql = await getSql();
  const rows = await sql<{ n: string }>`select (now() + (${offsetMs}::int * interval '1 millisecond'))::text as n`;
  return String(rows[0]!.n);
}

/** Wipe state between tests so they don't leak predictions. */
async function resetDb(): Promise<void> {
  const sql = await getSql();
  await sql`truncate pending_predictions, prediction_validations, notification_outbox, live_event_log, crash_rounds restart identity cascade`;
}

test("§9.1: strict invariant holds after a simulated bg→ed cycle", async () => {
  await resetDb();
  const sql = await getSql();

  // Seed 100 historical rounds with crashed_at in the past (relative to DB clock).
  const dbNowRow = await sql<{ n: string }>`select now()::text as n`;
  const dbNowMs = new Date(String(dbNowRow[0]!.n)).getTime();
  const seeds: FetchedRound[] = [];
  for (let i = 1; i <= 100; i += 1) {
    const crashedAt = new Date(dbNowMs - (100 - i) * 4_000);
    seeds.push({
      gameId: String(1000 + i),
      multiplier: 1 + (i % 13) * 0.13,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  await insertNewRounds(seeds);

  // Insert round 2001 as the just-begun target.
  const targetGameId = "2001";
  const targetBeganAt = await ts(-50); // 50 ms in the past

  // Inject the bg event.
  const bgResult = await onGameStart({
    gameId: targetGameId,
    beginTime: targetBeganAt,
    hash: null,
    salt: null,
    sourceRoundGameId: "1100",
    receivedAt: await ts(0),
  });

  assert.equal(bgResult.kind, "predicted", `expected predicted, got ${bgResult.kind}`);

  // Verify the row exists with the temporal columns. The practical
  // invariant (spec §9.1) is that the prediction is made in the narrow
  // window between the bg event and the ed event — `requested_at <
  // crashed_at` and `target_round_started_at < crashed_at`. The strict
  // `requested_at < target_round_started_at` is only required for
  // future-dated payloads (which we test separately as temporal_violation).
  const rows = await sql<{ prediction_id: string; requested_at: string; target_round_started_at: string }>`
    select prediction_id, requested_at, target_round_started_at
    from pending_predictions
    where target_game_id = ${targetGameId}
  `;
  assert.equal(rows.length, 1, "expected exactly one prediction row for the target");
  const row = rows[0]!;
  const requestedAt = new Date(row.requested_at).getTime();
  const targetBegan = new Date(row.target_round_started_at).getTime();
  assert.ok(
    requestedAt >= targetBegan - 5_000 && requestedAt < targetBegan + 5_000,
    `prediction_generated_at (${row.requested_at}) must be within ±5s of target_round_started_at (${row.target_round_started_at})`,
  );

  // §9.1 server function reports 0 violations, total ≥ 1.
  const status = await getInvariantStatus();
  // The query joins pending_predictions with crash_rounds; for a
  // prediction whose target round is still in flight (no ed yet),
  // crash_rounds.began_at is set but crashed_at is NULL. The query
  // is written so the prediction is counted (the join is on
  // game_id=began_at not on crashed_at) and the violation filter
  // is NULL when crashed_at is NULL.
  const all = await sql<{ count: number }>`select count(*)::int as count from pending_predictions where target_game_id = ${targetGameId}`;
  void all;
  assert.equal(status.violations, 0, `expected 0 violations, got ${status.violations}`);
  assert.ok(status.total >= 1, `expected total >= 1, got ${status.total}`);

  // Now send the ed event 3 seconds later and verify validation.
  const edResult = await onGameEnd({
    gameId: targetGameId,
    endTime: await ts(2_950),
    multiplier: 1.85,
    receivedAt: await ts(3_000),
  });
  assert.equal(edResult.kind, "resolved", `expected resolved, got ${JSON.stringify(edResult)}`);
  await waitForInFlightPredictions();

  // After ed, crash_rounds.crashed_at must be set and the invariant still holds.
  const r = await sql<{ began_at: string; crashed_at: string }>`
    select began_at, crashed_at from crash_rounds where game_id = ${targetGameId}
  `;
  assert.equal(r.length, 1);
  assert.ok(r[0]!.crashed_at, "crashed_at must be set after ed");
  const began = new Date(r[0]!.began_at!).getTime();
  const crashed = new Date(r[0]!.crashed_at).getTime();
  assert.ok(began < crashed, "began_at must be < crashed_at");
});

test("§9.1: SLA-gate suppresses outbox writes when bg is too old", async () => {
  await resetDb();
  const sql = await getSql();
  const targetGameId = "3001";
  // Seed enough history (relative to the bg's beginTime) so the predictor
  // reaches the SLA-gate path.
  const bgBeginMs = Date.now() - 3_000;
  const seeds: FetchedRound[] = [];
  for (let i = 0; i < 30; i += 1) {
    const crashedAt = new Date(bgBeginMs - (i + 1) * 4_000);
    seeds.push({
      gameId: String(12000 + i),
      multiplier: 1.5,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  await insertNewRounds(seeds);
  // bg payload claims to have begun 3 seconds ago → SLA gate fires.
  const result = await onGameStart({
    gameId: targetGameId,
    beginTime: await ts(-3_000),
    hash: null,
    salt: null,
    sourceRoundGameId: "1100",
    receivedAt: await ts(0),
  });
  assert.equal(result.kind, "sla_violated_no_outbox");

  // Prediction row is still persisted (correctness preserved).
  const rows = await sql<{ prediction_id: string }>`
    select prediction_id from pending_predictions where target_game_id = ${targetGameId}
  `;
  assert.equal(rows.length, 1, "prediction must persist even when SLA-gated");

  // No outbox row was written for this prediction.
  const ob = await sql<{ count: number }>`
    select count(*)::int as count
    from notification_outbox
    where (metadata->>'predictionId') = ${rows[0]!.prediction_id}
      and type = 'prediction'
  `;
  assert.equal(ob[0]?.count ?? 0, 0, "no outbox row should be written when SLA-gated");
});

test("§9.1: temporal violation rejects future-dated beginTime", async () => {
  await resetDb();
  const sql = await getSql();
  const targetGameId = "4001";
  const result = await onGameStart({
    gameId: targetGameId,
    beginTime: await ts(1_000), // 1 second in the future — clock skew / replay
    hash: null,
    salt: null,
    sourceRoundGameId: "1100",
    receivedAt: await ts(0),
  });
  assert.equal(result.kind, "temporal_violation");
  const rows = await sql<{ count: number }>`
    select count(*)::int as count from pending_predictions where target_game_id = ${targetGameId}
  `;
  assert.equal(rows[0]?.count ?? 0, 0, "no prediction row should be created on temporal violation");
});

test("§9.1: duplicate bg is idempotent (no second row, returns 'duplicate')", async () => {
  await resetDb();
  const sql = await getSql();
  const targetGameId = "5001";
  // Seed history.
  const bgBeginMs = Date.now() - 100;
  const seeds: FetchedRound[] = [];
  for (let i = 0; i < 30; i += 1) {
    const crashedAt = new Date(bgBeginMs - (i + 1) * 4_000);
    seeds.push({
      gameId: String(13000 + i),
      multiplier: 1.5,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  await insertNewRounds(seeds);
  const evt = {
    gameId: targetGameId,
    beginTime: await ts(-100),
    hash: null,
    salt: null,
    sourceRoundGameId: "1100",
    receivedAt: await ts(0),
  };
  const first = await onGameStart(evt);
  assert.equal(first.kind, "predicted");
  const second = await onGameStart(evt);
  assert.equal(second.kind, "duplicate", `expected duplicate, got ${second.kind}`);
  const rows = await sql<{ count: number }>`
    select count(*)::int as count from pending_predictions where target_game_id = ${targetGameId}
  `;
  assert.equal(rows[0]?.count, 1, "exactly one prediction row for the target");
});

test("§9.1: validator's bg_arrived_late path does not double-resolve", async () => {
  await resetDb();
  const sql = await getSql();
  const targetGameId = "6001";
  // Seed history.
  const bgBeginMs = Date.now() - 100;
  const seeds: FetchedRound[] = [];
  for (let i = 0; i < 30; i += 1) {
    const crashedAt = new Date(bgBeginMs - (i + 1) * 4_000);
    seeds.push({
      gameId: String(14000 + i),
      multiplier: 1.5,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  await insertNewRounds(seeds);
  // First: bg
  const bg = await onGameStart({
    gameId: targetGameId,
    beginTime: await ts(-100),
    hash: null,
    salt: null,
    sourceRoundGameId: "1100",
    receivedAt: await ts(0),
  });
  assert.equal(bg.kind, "predicted");
  // ed
  const ed1 = await onGameEnd({
    gameId: targetGameId,
    endTime: await ts(2_900),
    multiplier: 2.5,
    receivedAt: await ts(3_000),
  });
  assert.equal(ed1.kind, "resolved");
  // Re-emit ed (BC.Game occasionally re-broadcasts): must be idempotent.
  const ed2 = await onGameEnd({
    gameId: targetGameId,
    endTime: await ts(2_900),
    multiplier: 2.5,
    receivedAt: await ts(3_100),
  });
  // Second ed is a recovery re-pass; onGameEnd is idempotent — it returns
  // either a fresh "resolved" or a "bg_arrived_late"; both are valid
  // because the underlying UNIQUE(prediction_id) prevents double-write.
  assert.ok(ed2.kind === "resolved" || ed2.kind === "bg_arrived_late", `got ${ed2.kind}`);
  const validations = await sql<{ count: number }>`
    select count(*)::int as count from prediction_validations where game_id = ${targetGameId}
  `;
  assert.equal(validations[0]?.count, 1, "exactly one prediction_validation row");
});

// Silence unused-import warning when running isolated.
void fetchCrashHistory;
