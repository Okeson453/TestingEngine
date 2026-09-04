/**
 * Temporal invariant tests for the diagnosis architecture:
 *   prediction_generated_at < target_round_started_at < target_round_crashed_at
 *
 * Flow under test:
 *   ed(N) → validate + onGameEndPredict(N+1) → bg(N+1) backfills started_at
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { getSql } from "@/lib/db";
import { insertNewRounds } from "@/lib/crash/ingest";
import { onGameEndPredict } from "@/lib/prediction/live/predictor";
import { onGameEnd } from "@/lib/prediction/live/validator";
import type { FetchedRound } from "@/lib/crash/fetch-bc";

async function resetDb(): Promise<void> {
  const sql = await getSql();
  await sql`truncate pending_predictions, prediction_validations, notification_outbox, live_event_log, crash_rounds restart identity cascade`;
}

async function seed(n: number, refMs: number): Promise<void> {
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

test("temporal invariant: onGameEndPredict sets generated_at with NULL started_at", async () => {
  await resetDb();
  const sql = await getSql();
  const dbNow = await sql<{ n: string }>`select now()::text as n`;
  const refMs = new Date(String(dbNow[0]!.n)).getTime();
  await seed(30, refMs);

  // Anchor source round N
  const gameId = "60000";
  const crashedAt = new Date(refMs - 1_000).toISOString();
  await sql`
    insert into crash_rounds (game_id, multiplier, began_at, crashed_at)
    values (${gameId}, 1.5, ${new Date(refMs - 4_000).toISOString()}::timestamptz, ${crashedAt}::timestamptz)
    on conflict (game_id) do nothing
  `;

  const result = await onGameEndPredict(gameId, crashedAt, 1.5, randomUUID());
  assert.ok(
    result.kind === "predicted" || result.kind === "duplicate",
    `expected predicted, got ${result.kind}`,
  );
  assert.equal(result.targetGameId, "60001");

  const rows = await sql<{
    generated_at: string | Date | null;
    target_round_started_at: string | Date | null;
    status: string;
  }>`
    select generated_at, target_round_started_at, status
    from pending_predictions
    where target_game_id = '60001' and status = 'PENDING'
    limit 1
  `;
  assert.equal(rows.length, 1, "prediction row must exist");
  assert.ok(rows[0]!.generated_at, "generated_at must be set");
  assert.equal(rows[0]!.target_round_started_at, null, "started_at must be NULL until bg");
});

test("temporal invariant: bg backfill leaves generated_at < started_at", async () => {
  await resetDb();
  const sql = await getSql();
  const dbNow = await sql<{ n: string }>`select now()::text as n`;
  const refMs = new Date(String(dbNow[0]!.n)).getTime();
  await seed(30, refMs);

  const gameId = "61000";
  const crashedAt = new Date(refMs - 2_000).toISOString();
  await sql`
    insert into crash_rounds (game_id, multiplier, began_at, crashed_at)
    values (${gameId}, 2.0, ${new Date(refMs - 5_000).toISOString()}::timestamptz, ${crashedAt}::timestamptz)
    on conflict (game_id) do nothing
  `;

  const pred = await onGameEndPredict(gameId, crashedAt, 2.0, randomUUID());
  assert.ok(pred.kind === "predicted" || pred.kind === "duplicate");

  // Simulate bg for N+1 arriving later
  const beganAt = new Date(refMs + 500).toISOString();
  await sql`
    UPDATE pending_predictions
    SET target_round_started_at = ${beganAt}::timestamptz
    WHERE target_game_id = ${pred.targetGameId}
      AND status = 'PENDING'
      AND target_round_started_at IS NULL
  `;

  const violations = await sql<{ prediction_id: string }>`
    SELECT prediction_id
    FROM pending_predictions
    WHERE target_game_id = ${pred.targetGameId}
      AND target_round_started_at IS NOT NULL
      AND generated_at IS NOT NULL
      AND generated_at >= target_round_started_at
  `;
  assert.equal(violations.length, 0, "must have zero temporal violations");
});

test("idempotency: duplicate onGameEndPredict returns existing", async () => {
  await resetDb();
  const sql = await getSql();
  const dbNow = await sql<{ n: string }>`select now()::text as n`;
  const refMs = new Date(String(dbNow[0]!.n)).getTime();
  await seed(30, refMs);

  const gameId = "62000";
  const crashedAt = new Date(refMs - 1_000).toISOString();
  await sql`
    insert into crash_rounds (game_id, multiplier, began_at, crashed_at)
    values (${gameId}, 1.2, ${new Date(refMs - 4_000).toISOString()}::timestamptz, ${crashedAt}::timestamptz)
    on conflict (game_id) do nothing
  `;

  const first = await onGameEndPredict(gameId, crashedAt, 1.2, randomUUID());
  const second = await onGameEndPredict(gameId, crashedAt, 1.2, randomUUID());
  assert.ok(first.kind === "predicted" || first.kind === "duplicate");
  assert.equal(second.kind, "duplicate");
  assert.equal(first.targetGameId, second.targetGameId);

  const count = await sql<{ c: number }>`
    select count(*)::int as c from pending_predictions
    where target_game_id = ${first.targetGameId} and status = 'PENDING'
  `;
  assert.equal(count[0]!.c, 1, "only one PENDING row for target");
});
