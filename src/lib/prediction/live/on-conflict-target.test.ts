/**
 * Fix 3 regression test — pending_predictions duplicate guard.
 *
 * The persistence insert used `ON CONFLICT (prediction_id)`, which did NOT
 * match the real duplicate guard: the partial unique index
 * `pending_predictions_target_game_id_unmatched_uidx (target_game_id)
 * WHERE matched = false AND target_game_id is not null`
 * (migrations/0007_prediction_correlation.sql).
 *
 * This test proves the corrected conflict clause:
 *   1. is ACCEPTED by Postgres/pglite verbatim (a predicate mismatch is
 *      rejected at query time — better here than in production), and
 *   2. dedupes concurrent inserts for the same unmatched target — exactly
 *      one row survives, neither caller sees a unique_violation.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { getSql, type Sql } from "@/lib/db";
import { randomUUID } from "node:crypto";

async function insertPending(sql: Sql, opts: { predictionId: string; targetGameId: string }) {
  return sql<{ prediction_id: string; requested_at: string }>`
    insert into pending_predictions (
      prediction_id, target_multiplier, probability, confidence,
      regime_name, regime_confidence, reasoning, feature_summary,
      model_version, requested_at, generated_at,
      target_game_id, source_round_id,
      correlation_id
    ) values (
      ${opts.predictionId}, 1.30, 0.5, 0.5,
      null, null, ARRAY['test']::text[], '{}'::jsonb,
      'test-model', now(), now(),
      ${opts.targetGameId}, ${'src-' + opts.targetGameId},
      ${randomUUID()}
    )
    on conflict (target_game_id) where matched = false and target_game_id is not null do nothing
    returning prediction_id, requested_at
  `;
}

test("ON CONFLICT (target_game_id) partial index: concurrent duplicates resolve to exactly one row", async () => {
  const sql = await getSql();
  const targetGameId = `conflict-${Date.now()}-${randomUUID().slice(0, 8)}`;
  try {
    // Two concurrent persistence calls for the SAME target with DIFFERENT
    // prediction ids — the multi-replica edge case the old clause missed.
    const [a, b] = await Promise.all([
      insertPending(sql, { predictionId: randomUUID(), targetGameId }),
      insertPending(sql, { predictionId: randomUUID(), targetGameId }),
    ]);

    // Exactly one caller saw a fresh insert; the other got an empty result
    // (the "Duplicate — already persisted by another path" branch). No throw.
    const inserted = [a.length, b.length].filter((n) => n === 1).length;
    assert.equal(inserted, 1, "exactly one insert must succeed");

    const rows = await sql<{ c: number }>`
      select count(*)::int as c from pending_predictions
      where target_game_id = ${targetGameId}
    `;
    assert.equal(rows[0]!.c, 1, "exactly one pending row exists for the target");
  } finally {
    await sql`delete from pending_predictions where target_game_id = ${targetGameId}`;
  }
});
