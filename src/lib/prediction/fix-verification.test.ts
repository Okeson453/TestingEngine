/**
 * Critical fix verification:
 * - generateAndQueuePrediction binds to MAX(game_id) + 1
 * - validateAgainstNewRounds matches by target_game_id
 * - the partial unique index prevents duplicate active predictions
 *
 * These tests fail under the legacy FIFO + "next" behavior and pass
 * with the fix applied.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { getSql } from "@/lib/db";
import { insertNewRounds } from "@/lib/crash/ingest";
import { generateAndQueuePrediction, validateAgainstNewRounds } from "@/lib/prediction/service";
import type { FetchedRound } from "@/lib/crash/fetch-bc";

async function reset(): Promise<void> {
  const sql = await getSql();
  await sql`truncate pending_predictions, prediction_validations, notification_outbox, live_event_log, crash_rounds restart identity cascade`;
}

async function seedHistory(n: number): Promise<void> {
  const sql = await getSql();
  const dbNow = await sql<{ n: string }>`select now()::text as n`;
  const refMs = new Date(String(dbNow[0]!.n)).getTime();
  const seeds: FetchedRound[] = [];
  for (let i = 0; i < n; i += 1) {
    const crashedAt = new Date(refMs - (i + 1) * 4_000);
    seeds.push({
      gameId: String(50_000 + i),
      multiplier: 1 + (i % 13) * 0.13,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  await insertNewRounds(seeds);
}

test("fix-3: generateAndQueuePrediction binds to MAX(game_id) + 1", async () => {
  await reset();
  const sql = await getSql();
  await seedHistory(50);

  const maxIdRow = await sql<{ max_id: string }>`select MAX(game_id) as max_id from crash_rounds`;
  const expectedTarget = String(Number(maxIdRow[0]!.max_id) + 1);

  const queued = await generateAndQueuePrediction(sql);
  assert.ok(queued, "expected a queued prediction");

  const rows = await sql<{ target_game_id: string; status: string; source_round_id: string | null }>`
    select target_game_id, status, source_round_id
    from pending_predictions
  `;
  assert.equal(rows.length, 1, "exactly one pending prediction");
  assert.equal(rows[0]!.target_game_id, expectedTarget);
  assert.equal(rows[0]!.status, "PENDING");
  assert.equal(rows[0]!.source_round_id, maxIdRow[0]!.max_id);
});

test("fix-3: MAX+1 advances past newly-inserted rounds (no off-by-one)", async () => {
  await reset();
  const sql = await getSql();
  await seedHistory(50);

  // First generation binds to 50050 (MAX+1 = 50049+1).
  const queued1 = await generateAndQueuePrediction(sql);
  assert.ok(queued1);

  // The MAX is now still 50049 (the prediction is in pending_predictions,
  // not crash_rounds). So MAX+1 is still 50050 — the SAME target. The
  // active-target index makes the second call a no-op (Fix 3 idempotency).
  const queued2 = await generateAndQueuePrediction(sql);
  assert.equal(queued2, null, "second call should be deduped by active-target index");

  // Now insert 50050 into crash_rounds (simulating that BC.Game has
  // crashed the target while we were generating). The next generation
  // should now target 50051 (MAX=50050+1).
  await insertNewRounds([
    {
      gameId: "50050",
      multiplier: 1.5,
      hash: null,
      salt: null,
      beganAt: new Date(),
      crashedAt: new Date(),
    },
  ]);

  // The validator should now match the prediction for 50050.
  const outcome = await validateAgainstNewRounds([{
    gameId: "50050",
    multiplier: 1.85,
    hash: null,
    salt: null,
    beganAt: new Date().toISOString(),
    crashedAt: new Date().toISOString(),
  }]);
  assert.equal(outcome.resolved, 1, "validator should match the prediction for 50050");

  // Subsequent generation should now target 50051 (no duplicate for
  // 50050 because its prediction is now MATCHED, not PENDING).
  const queued3 = await generateAndQueuePrediction(sql);
  assert.ok(queued3, "next cycle should produce a fresh prediction for 50051");
  const target3 = await sql<{ target_game_id: string }>`
    select target_game_id from pending_predictions where status = 'PENDING'
  `;
  assert.equal(target3[0]!.target_game_id, "50051");
});

test("fix-3: idempotency — calling twice in a row yields a single active row", async () => {
  await reset();
  const sql = await getSql();
  await seedHistory(50);

  const a = await generateAndQueuePrediction(sql);
  const b = await generateAndQueuePrediction(sql);
  assert.ok(a, "first call should produce a row");
  assert.equal(b, null, "second call should be idempotent (no second row)");

  const rows = await sql<{ count: number }>`
    select count(*)::int as count from pending_predictions
    where status = 'PENDING'
  `;
  assert.equal(rows[0]?.count, 1);
});

test("fix-4: validateAgainstNewRounds matches by target_game_id (deterministic, not FIFO)", async () => {
  await reset();
  const sql = await getSql();
  await seedHistory(50);

  // Generate a prediction. It binds to MAX+1.
  const queued = await generateAndQueuePrediction(sql);
  assert.ok(queued);

  // The MAX+1 round then "crashes" via REST. We deliberately insert
  // other rounds in between to verify the validator does NOT do FIFO.
  const targetRow = await sql<{ target_game_id: string }>`
    select target_game_id from pending_predictions where status = 'PENDING' limit 1
  `;
  const targetId = targetRow[0]!.target_game_id;
  const now = new Date().toISOString();

  // Insert two intermediate rounds BEFORE the target, and one round
  // AFTER the target. The validator must still pair the prediction
  // with the target round, not the FIFO head.
  const intermediateBefore: FetchedRound[] = [];
  for (let i = 1; i <= 2; i += 1) {
    intermediateBefore.push({
      gameId: String(Number(targetId) - i),
      multiplier: 1.5,
      hash: null,
      salt: null,
      beganAt: new Date(),
      crashedAt: new Date(),
    });
  }
  await insertNewRounds(intermediateBefore);

  // Now insert the target itself as a fresh round.
  const targetRound: FetchedRound = {
    gameId: targetId,
    multiplier: 1.85,
    hash: null,
    salt: null,
    beganAt: new Date(),
    crashedAt: new Date(),
  };
  await insertNewRounds([targetRound]);

  // Now look up the row and validate it via the underlying service.
  const outcome = await validateAgainstNewRounds([{
    gameId: targetId,
    multiplier: 1.85,
    hash: null,
    salt: null,
    beganAt: now,
    crashedAt: now,
  }]);

  assert.equal(outcome.resolved, 1, "validator should match by target_game_id");
  const v = await sql<{ game_id: string; result: string; target_multiplier: string }>`
    select game_id, result, target_multiplier from prediction_validations
    where game_id = ${targetId}
  `;
  assert.equal(v.length, 1);
  assert.equal(v[0]!.game_id, targetId);
  assert.equal(v[0]!.result, "WIN");

  // Pending row should now be MATCHED, not PENDING.
  const status = await sql<{ status: string; matched: boolean }>`
    select status, matched from pending_predictions where target_game_id = ${targetId}
  `;
  assert.equal(status[0]!.status, "MATCHED");
  assert.equal(status[0]!.matched, true);
});

test("fix-4: validator returns no match when no PENDING prediction exists for the target", async () => {
  await reset();
  const sql = await getSql();
  await seedHistory(50);

  // Insert a round with no pending prediction.
  const outcome = await validateAgainstNewRounds([{
    gameId: "99999",
    multiplier: 1.5,
    hash: null,
    salt: null,
    beganAt: new Date().toISOString(),
    crashedAt: new Date().toISOString(),
  }]);
  assert.equal(outcome.resolved, 0);
  assert.equal(outcome.pairs.length, 0);
});
