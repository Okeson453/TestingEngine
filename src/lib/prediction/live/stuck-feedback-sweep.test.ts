/**
 * Recovery sweep + skip-reason tests (investigation report fixes).
 *
 * Covers two gaps in the closed-loop feedback chain:
 *
 *   1. sweepStuckFeedback re-drives rows whose feedback_applied_at is NULL
 *      past the age threshold (process crashed between validation commit and
 *      the deferred feedback call). The durable claim makes the pipeline
 *      idempotent — a second sweep must be a no-op.
 *   2. Rows with feedback_skip_reason='TEMPORALLY_INVALID' (intentional
 *      skips) are excluded from the sweep AND from the
 *      feedback_not_applied_within_sla invariant — previously every
 *      temporally-invalid prediction tripped the invariant forever.
 *
 * Requires migration 0024 (feedback_skip_reason) applied to the local pglite.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { getSql } from "@/lib/db";
import { sweepStuckFeedback } from "@/lib/prediction/live/feedback";

// Unique-per-run suffix: the pglite DB persists rows across runs and
// game_id carries a UNIQUE index.
const rid = Math.random().toString(36).slice(2, 10);

async function insertStaleValidation(opts: {
  predictionId: string;
  gameId: string;
  skipReason?: string | null;
}): Promise<void> {
  const sql = await getSql();
  // resolved_at 10 minutes in the past — past both the invariant's 2-minute
  // and the sweep's 5-minute thresholds.
  await sql`
    INSERT INTO prediction_validations (
      prediction_id, game_id, target_multiplier, predicted_probability,
      actual_multiplier, result, model_version, requested_at, resolved_at,
      feedback_skip_reason
    ) VALUES (
      ${opts.predictionId}, ${opts.gameId}, 1.30, 0.42,
      1.85, 'WIN', 'v1', now() - interval '15 minutes',
      now() - interval '10 minutes',
      ${opts.skipReason ?? null}
    )
    ON CONFLICT (prediction_id) DO NOTHING
  `;
}

test("sweep applies feedback to genuinely stuck rows (idempotently)", async () => {
  const sql = await getSql();
  const predictionId = `sweep-test-${randomUUIDShort()}`;
  await insertStaleValidation({ predictionId, gameId: `sweep-game-1-${rid}` });

  const before = await sql<{ feedback_applied_at: string | Date | null }>`
    SELECT feedback_applied_at FROM prediction_validations
    WHERE prediction_id = ${predictionId}
  `;
  assert.equal(before[0]!.feedback_applied_at, null, "fixture row must start unapplied");

  const first = await sweepStuckFeedback(sql, { olderThanMinutes: 5, limit: 50 });
  assert.ok(first.swept >= 1, "sweep must see the stale row");

  const after = await sql<{ feedback_applied_at: string | Date | null }>`
    SELECT feedback_applied_at FROM prediction_validations
    WHERE prediction_id = ${predictionId}
  `;
  assert.notEqual(after[0]!.feedback_applied_at, null, "sweep must set feedback_applied_at");

  // Second pass: the row is claimed — nothing left to do for it. The count
  // assertion targets THIS row only: the shared pglite DB persists stale
  // rows from earlier runs, which the sweep may legitimately re-drive.
  const appliedAtAfterFirst = after[0]!.feedback_applied_at;
  await sweepStuckFeedback(sql, { olderThanMinutes: 5, limit: 50 });
  const after2 = await sql<{ feedback_applied_at: string | Date | null }>`
    SELECT feedback_applied_at FROM prediction_validations
    WHERE prediction_id = ${predictionId}
  `;
  assert.notEqual(after2[0]!.feedback_applied_at, null);
  assert.equal(
    after2[0]!.feedback_applied_at instanceof Date
      ? (after2[0]!.feedback_applied_at as Date).toISOString()
      : after2[0]!.feedback_applied_at,
    appliedAtAfterFirst instanceof Date
      ? (appliedAtAfterFirst as Date).toISOString()
      : appliedAtAfterFirst,
    "second sweep must not re-apply this row (durable claim idempotency)",
  );
});

test("skip-reasoned rows (TEMPORALLY_INVALID) are excluded from the sweep", async () => {
  const sql = await getSql();
  const predictionId = `sweep-skip-${randomUUIDShort()}`;
  await insertStaleValidation({
    predictionId,
    gameId: `sweep-game-2-${rid}`,
    skipReason: "TEMPORALLY_INVALID",
  });

  await sweepStuckFeedback(sql, { olderThanMinutes: 5, limit: 50 });

  const row = await sql<{ feedback_applied_at: string | Date | null }>`
    SELECT feedback_applied_at FROM prediction_validations
    WHERE prediction_id = ${predictionId}
  `;
  assert.equal(
    row[0]!.feedback_applied_at,
    null,
    "intentional skip must never be 'recovered' — it never trains the models",
  );
});

test("skip-reasoned rows are excluded from the feedback_not_applied_within_sla invariant", async () => {
  const sql = await getSql();
  const skipId = `invariant-skip-${randomUUIDShort()}`;
  await insertStaleValidation({
    predictionId: skipId,
    gameId: `sweep-game-3-${rid}`,
    skipReason: "TEMPORALLY_INVALID",
  });

  const { sampleProductionInvariants } = await import(
    "@/lib/prediction/live/invariants"
  );
  const snapshot = await sampleProductionInvariants(sql);
  const feedbackViolations = (snapshot.violations ?? []).filter(
    (v: { id?: string; predictionId?: string }) =>
      v.id === "feedback_not_applied_within_sla" && v.predictionId === skipId,
  );
  assert.equal(
    feedbackViolations.length,
    0,
    "an intentional skip must not be an invariant violation",
  );
});

function randomUUIDShort(): string {
  return Math.random().toString(36).slice(2, 10);
}
