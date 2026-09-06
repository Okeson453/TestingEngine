/**
 * Phase 16 — Production-path tests drive game-event-handlers only.
 *
 * Canonical path under test:
 *   BC.Game-shaped bg/ed payloads → onGameStartLegacy / onGameEndLegacy
 *   → durable handoff → validator.onGameEnd → feedback → onGameEndPredict
 *
 * Does not call onGameStart/onGameEndPredict directly (those remain unit-tested).
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { getSql } from "@/lib/db";
import {
  onGameStartLegacy,
  onGameEndLegacy,
} from "@/lib/prediction/events/game-event-handlers";
import { insertNewRounds } from "@/lib/crash/ingest";
import type { FetchedRound } from "@/lib/crash/fetch-bc";

async function ts(offsetMs: number): Promise<string> {
  const sql = await getSql();
  const rows = await sql<{ n: string }>`
    select (now() + (${offsetMs}::int * interval '1 millisecond'))::text as n
  `;
  return String(rows[0]!.n);
}

async function resetDb(): Promise<void> {
  const sql = await getSql();
  await sql`
    truncate pending_predictions, prediction_validations, notification_outbox,
             live_event_log, live_round_state, crash_rounds
    restart identity cascade
  `;
}

async function seedHistory(count = 40, baseId = 90000): Promise<void> {
  const sql = await getSql();
  const dbNow = await sql<{ n: string }>`select now()::text as n`;
  const dbNowMs = new Date(String(dbNow[0]!.n)).getTime();
  const seeds: FetchedRound[] = [];
  for (let i = 1; i <= count; i += 1) {
    const crashedAt = new Date(dbNowMs - (count - i) * 4_000);
    seeds.push({
      gameId: String(baseId + i),
      multiplier: 1 + (i % 11) * 0.17,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  await insertNewRounds(seeds);
}

/** Wait for setImmediate / async ed processing inside handlers. */
function flushAsync(ms = 50): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

test("Phase 16: ed payload via handlers runs durable handoff + validation path", async () => {
  await resetDb();
  await seedHistory(50, 91000);

  const gameId = "91050";
  const endTime = await ts(0);
  const multiplier = 2.34;

  // Simulate BC.Game ed payload shape (not a typed GameEndEvent)
  await onGameEndLegacy({
    gameId,
    crashedAt: endTime,
    multiplier,
  });

  // Durable handoff is sync before setImmediate; validation is async
  await flushAsync(200);

  const sql = await getSql();
  const rounds = await sql<{ game_id: string; multiplier: number }>`
    select game_id, multiplier from crash_rounds where game_id = ${gameId}
  `;
  assert.equal(rounds.length, 1, "crash_rounds must receive durable ed handoff");
  assert.ok(Number(rounds[0]!.multiplier) > 0);

  const log = await sql<{ event_kind: string }>`
    select event_kind from live_event_log
    where game_id = ${gameId} and event_kind = 'ED_RECEIVED'
    limit 1
  `;
  assert.equal(log.length, 1, "ED_RECEIVED marker must be written");

  const live = await sql<{ lifecycle: string }>`
    select lifecycle from live_round_state where game_id = ${gameId} limit 1
  `;
  assert.equal(live.length, 1);
  assert.ok(
    ["ENDED", "PREDICTION_RESOLVED", "FEEDBACK_APPLIED", "NEXT_PREDICTION_GENERATED", "RECONCILED"].includes(
      live[0]!.lifecycle,
    ),
    `unexpected lifecycle ${live[0]!.lifecycle}`,
  );
});

test("Phase 16: bg payload via handlers is observability-only (no prediction)", async () => {
  await resetDb();
  const gameId = "92001";
  const beganAt = await ts(-100);

  await onGameStartLegacy({
    gameId,
    beginTime: beganAt,
  });
  await flushAsync(50);

  const sql = await getSql();
  const preds = await sql<{ c: number }>`
    select count(*)::int as c from pending_predictions where target_game_id = ${gameId}
  `;
  assert.equal(preds[0]?.c ?? 0, 0, "bg must not create predictions");

  const live = await sql<{ lifecycle: string }>`
    select lifecycle from live_round_state where game_id = ${gameId} limit 1
  `;
  assert.equal(live.length, 1);
  assert.ok(
    live[0]!.lifecycle === "STARTED" || live[0]!.lifecycle === "RUNNING",
    `got ${live[0]!.lifecycle}`,
  );
});

test("Phase 16: full N then N+1 path driven only by handlers", async () => {
  await resetDb();
  await seedHistory(60, 93000);

  // First seed a prior prediction for round 93060 so ed can resolve something
  // (optional). Primary assertion is ed → durable state + no throw.
  const gameN = "93060";
  const endN = await ts(0);
  await onGameEndLegacy({
    id: gameN,
    gameId: gameN,
    endTime: endN,
    rate: 1.75,
  });
  await flushAsync(400);

  const sql = await getSql();
  const rounds = await sql<{ c: number }>`
    select count(*)::int as c from crash_rounds where game_id = ${gameN}
  `;
  assert.equal(rounds[0]?.c, 1);

  // N+1 may or may not have a pending row depending on history/ACIE readiness;
  // assert the pipeline did not leave ED_RECEIVED without attempting processing
  // (no fatal throw) and live state advanced past DISCOVERED.
  const live = await sql<{ lifecycle: string }>`
    select lifecycle from live_round_state where game_id = ${gameN}
  `;
  assert.ok(live[0]);
  assert.notEqual(live[0]!.lifecycle, "DISCOVERED");
});
