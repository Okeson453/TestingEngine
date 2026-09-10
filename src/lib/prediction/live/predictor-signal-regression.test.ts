/**
 * P0 regression tests for the live N+1 prediction path (onGameEndPredict).
 *
 * Test D: full ED → N+1 → persistence → outbox path. Verifies the outbox
 * record corresponds to the N+1 TARGET round — not the already-completed
 * source round — and that predictionId/correlationId/targetGameId survive
 * the whole pipeline.
 *
 * Claim-leak regression: when prediction fails (e.g. validation stage),
 * the in-memory target claim must be RELEASED — a failed attempt must not
 * permanently block the target as a phantom "duplicate".
 *
 * Concurrency: simultaneous ED + poll recovery + immediate recovery for the
 * same source round must yield exactly ONE prediction, one target, one
 * outbox record.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { onGameEndPredict } from "@/lib/prediction/live/predictor";
import { getSql } from "@/lib/db";
import { insertNewRounds } from "@/lib/crash/ingest";
import type { FetchedRound } from "@/lib/crash/fetch-bc";
import {
  warmLiveHistoryBuffer,
  _resetLiveHistoryBufferForTests,
} from "@/lib/prediction/live/live-history-buffer";
import { _resetTargetCoordinatorForTests } from "@/lib/prediction/live/target-coordinator";
import type { HistoricalRound, ThresholdTarget } from "@/lib/prediction/types";

const SOURCE_GAME_ID = 31234;
const TARGET_GAME_ID = String(SOURCE_GAME_ID + 1);

function stubPredictFn() {
  const predictionId = `live-stub-${randomUUID()}`;
  return {
    fn: (
      _priorRounds: HistoricalRound[],
      targetRoundId: string,
      _timestamp: string,
      _target: ThresholdTarget,
    ) => ({
      predictionId,
      probability: 0.85,
      confidence: 0.75,
      regimeId: "r-test",
      reasoning: ["stub"],
      featureSummary: { f1: 1 },
      modelVersion: "stub@1.0.0",
      featurePath: "V1_FALLBACK",
      // sanity: the target the predictor passes must be N+1
      ...(targetRoundId ? {} : {}),
    }),
    predictionId,
  };
}

async function resetDb(): Promise<void> {
  const sql = await getSql();
  await sql`truncate pending_predictions, prediction_validations, notification_outbox, live_event_log, crash_rounds restart identity cascade`;
  // insertNewRounds skips game_ids present in the global hot cache — clear it
  // so re-seeding the same ids after truncate actually inserts rows.
  const { globalRecentRoundCache } = await import("@/lib/observability/performance/hot-cache");
  globalRecentRoundCache.clear();
}

async function seedAndWarm(n = 60): Promise<void> {
  const sql = await getSql();
  const dbNow = await sql<{ n: string }>`select now()::text as n`;
  const refMs = new Date(String(dbNow[0]!.n)).getTime();
  const seeds: FetchedRound[] = [];
  for (let i = 1; i <= n; i += 1) {
    const crashedAt = new Date(refMs - (n - i) * 4_000 - 60_000);
    seeds.push({
      gameId: String(SOURCE_GAME_ID - n + i),
      multiplier: 1 + (i % 13) * 0.13,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  await insertNewRounds(seeds);
  await warmLiveHistoryBuffer(sql, Math.max(n, 100));
}

async function setup(): Promise<void> {
  await resetDb();
  _resetTargetCoordinatorForTests();
  _resetLiveHistoryBufferForTests();
  await seedAndWarm();
}

/** Poll until `predicate` is true (durable handoff is async by design). */
async function waitFor(
  predicate: () => Promise<boolean>,
  timeoutMs = 8_000,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return true;
    await new Promise((r) => setTimeout(r, 50));
  }
  return await predicate();
}

// ── Test D: full ED → N+1 → DB → outbox ────────────────────────────────────

test("D: ED N+1 prediction persists for the TARGET round and reaches the outbox", async () => {
  await setup();
  const sql = await getSql();
  const stub = stubPredictFn();
  const correlationId = randomUUID();
  const crashedAt = new Date().toISOString();

  const result = await onGameEndPredict(
    String(SOURCE_GAME_ID),
    crashedAt,
    1.87,
    correlationId,
    { predictFn: stub.fn },
  );

  assert.equal(result.kind, "predicted");
  assert.equal(result.targetGameId, TARGET_GAME_ID);
  assert.equal(result.predictionId, stub.predictionId);

  const persisted = await waitFor(async () => {
    const rows = await sql<{ prediction_id: string }>`
      select prediction_id from pending_predictions where target_game_id = ${TARGET_GAME_ID}
    `;
    return rows.length === 1;
  });
  assert.ok(persisted, "pending_predictions row for the N+1 target must appear");

  const pending = await sql<{
    prediction_id: string;
    source_round_id: string | null;
    correlation_id: string | null;
    target_game_id: string;
  }>`
    select prediction_id, source_round_id, correlation_id, target_game_id
    from pending_predictions where target_game_id = ${TARGET_GAME_ID}
  `;
  assert.equal(pending.length, 1, "exactly one pending prediction");
  assert.equal(pending[0]!.prediction_id, stub.predictionId);
  assert.equal(pending[0]!.source_round_id, String(SOURCE_GAME_ID), "source round is the completed ED round");
  assert.equal(pending[0]!.correlation_id, correlationId, "correlationId survives to persistence");
  // The prediction must NOT be for the already-completed source round.
  const sourceRows = await sql<{ count: number }>`
    select count(*)::int as count from pending_predictions where target_game_id = ${String(SOURCE_GAME_ID)}
  `;
  assert.equal(sourceRows[0]!.count, 0, "no prediction row for the completed source round");

  const outboxAppeared = await waitFor(async () => {
    const rows = await sql<{ notification_id: string }>`
      select notification_id from notification_outbox
      where metadata->>'predictionId' = ${stub.predictionId}
    `;
    return rows.length === 1;
  });
  assert.ok(outboxAppeared, "notification_outbox record must appear");

  const outbox = await sql<{
    metadata: { targetGameId?: string; correlationId?: string; sourceGameId?: string };
    content: string;
    status: string;
  }>`
    select metadata, content, status from notification_outbox
    where metadata->>'predictionId' = ${stub.predictionId}
  `;
  assert.equal(outbox.length, 1, "exactly one outbox record");
  assert.equal(outbox[0]!.metadata.targetGameId, TARGET_GAME_ID, "outbox targets the N+1 round");
  assert.equal(outbox[0]!.metadata.sourceGameId, String(SOURCE_GAME_ID));
  assert.equal(outbox[0]!.metadata.correlationId, correlationId, "correlationId survives to the outbox");
  assert.equal(outbox[0]!.status, "pending");
  assert.ok(outbox[0]!.content.includes(stub.predictionId), "Telegram content carries the prediction id");

  const eventLog = await sql<{ count: number }>`
    select count(*)::int as count from live_event_log
    where game_id = ${TARGET_GAME_ID} and event_kind = 'PREDICT'
  `;
  assert.ok((eventLog[0]!.count ?? 0) >= 1, "live_event_log PREDICT row for the target");
});

// ── Claim-leak regression: failed attempt must release the target ──────────

test("failed N+1 attempt (validation stage) releases the target claim — no phantom duplicate", async () => {
  await setup();
  const correlationId = randomUUID();

  const failing = () => {
    const err = new Error("[prediction_output_validation] probability missing (field=probability)") as never;
    (err as unknown as { stage?: string }).stage = "prediction_output_validation";
    throw err;
  };

  const failed = await onGameEndPredict(
    String(SOURCE_GAME_ID),
    new Date().toISOString(),
    1.5,
    correlationId,
    { predictFn: failing as never },
  );

  assert.equal(failed.kind, "error", "predictFn failure must surface as kind=error, not a throw");
  assert.equal(failed.predictionId, null);
  assert.equal(failed.targetGameId, TARGET_GAME_ID);

  // THE regression: the claim must be released, so the immediate retry with
  // a healthy predictFn is NOT swallowed as a phantom "duplicate".
  const stub = stubPredictFn();
  const retry = await onGameEndPredict(
    String(SOURCE_GAME_ID),
    new Date().toISOString(),
    1.5,
    correlationId,
    { predictFn: stub.fn },
  );
  assert.equal(retry.kind, "predicted", "target claim was released after the failure — retry succeeds");
  assert.equal(retry.predictionId, stub.predictionId);

  const sql = await getSql();
  const persisted = await waitFor(async () => {
    const rows = await sql<{ count: number }>`
      select count(*)::int as count from pending_predictions where target_game_id = ${TARGET_GAME_ID}
    `;
    return rows[0]!.count === 1;
  });
  assert.ok(persisted, "retry prediction persisted exactly once");
});

// ── Concurrency: ED + poll recovery + immediate recovery, same source ───────

test("concurrent ED/recovery for the same source round → exactly one prediction and one outbox row", async () => {
  await setup();
  const sql = await getSql();
  const correlationId = randomUUID();
  const crashedAt = new Date().toISOString();

  const mk = () => stubPredictFn();
  const attempts = [
    onGameEndPredict(String(SOURCE_GAME_ID), crashedAt, 2.1, correlationId, {
      predictFn: mk().fn,
    }),
    onGameEndPredict(String(SOURCE_GAME_ID), crashedAt, 2.1, correlationId, {
      predictFn: mk().fn,
      recoveryMode: true,
    }),
    onGameEndPredict(String(SOURCE_GAME_ID), crashedAt, 2.1, correlationId, {
      predictFn: mk().fn,
      recoveryMode: true,
    }),
  ];
  const results = await Promise.all(attempts);

  const kinds = results.map((r) => r.kind).sort();
  assert.deepEqual(
    kinds,
    ["duplicate", "duplicate", "predicted"],
    `exactly one owner wins the target; got ${kinds.join(",")}`,
  );

  const settled = await waitFor(async () => {
    const [pending, outbox] = await Promise.all([
      sql<{ count: number }>`
        select count(*)::int as count from pending_predictions where target_game_id = ${TARGET_GAME_ID}
      `,
      sql<{ count: number }>`
        select count(*)::int as count from notification_outbox
        where metadata->>'targetGameId' = ${TARGET_GAME_ID}
      `,
    ]);
    return pending[0]!.count === 1 && outbox[0]!.count === 1;
  });
  assert.ok(settled, "exactly one pending prediction AND one outbox row for the target");

  const pending = await sql<{ prediction_id: string; source_round_id: string | null }>`
    select prediction_id, source_round_id from pending_predictions where target_game_id = ${TARGET_GAME_ID}
  `;
  assert.equal(pending.length, 1);
  assert.equal(pending[0]!.source_round_id, String(SOURCE_GAME_ID));
});
