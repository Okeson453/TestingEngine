/**
 * Phase 17 — Production invariants.
 * Continuous checks; violations are logged and returned (never throw on the hot path).
 */
import { getSql, type Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("production-invariants");

export type InvariantId =
  | "prediction_before_target_start"
  | "one_active_prediction_per_target"
  | "one_validation_per_prediction"
  | "one_feedback_per_validation"
  | "one_crash_result_per_game"
  | "no_prediction_without_history"
  | "no_duplicate_outbox_for_prediction";

export interface InvariantViolation {
  id: InvariantId;
  detail: string;
  gameId?: string;
  predictionId?: string;
}

export interface InvariantCheckResult {
  ok: boolean;
  violations: InvariantViolation[];
}

/** temporal: prediction.generated_at < target_round_started_at when known */
export function checkPredictionBeforeTargetStart(params: {
  predictionGeneratedAt: string | Date;
  targetRoundStartedAt: string | Date | null | undefined;
  toleranceMs?: number;
}): InvariantViolation | null {
  if (params.targetRoundStartedAt == null) return null;
  const gen = new Date(params.predictionGeneratedAt).getTime();
  const start = new Date(params.targetRoundStartedAt).getTime();
  if (!Number.isFinite(gen) || !Number.isFinite(start)) return null;
  const tol = params.toleranceMs ?? 0;
  if (gen >= start - tol) {
    return {
      id: "prediction_before_target_start",
      detail: `generated_at=${new Date(gen).toISOString()} >= started_at=${new Date(start).toISOString()}`,
    };
  }
  return null;
}

/** Run DB-backed invariant sample (cheap, for heartbeat / poll). */
export async function sampleProductionInvariants(
  sql?: Sql,
): Promise<InvariantCheckResult> {
  const db = sql ?? (await getSql());
  const violations: InvariantViolation[] = [];

  try {
    // Multiple active pending rows for same target
    const dups = await db<{ target_game_id: string; c: number }>`
      SELECT target_game_id, count(*)::int AS c
      FROM pending_predictions
      WHERE matched = false AND status = 'PENDING'
      GROUP BY target_game_id
      HAVING count(*) > 1
      LIMIT 10
    `;
    for (const row of dups) {
      violations.push({
        id: "one_active_prediction_per_target",
        detail: `target=${row.target_game_id} active=${row.c}`,
        gameId: row.target_game_id,
      });
    }
  } catch (e) {
    logger.debug({ error: String(e) }, "invariant sample skip (pending dups)");
  }

  try {
    // Validations without feedback_applied_at older than 2 minutes (stuck feedback)
    const stuck = await db<{ prediction_id: string; game_id: string }>`
      SELECT prediction_id, game_id
      FROM prediction_validations
      WHERE feedback_applied_at IS NULL
        AND resolved_at < now() - interval '2 minutes'
      ORDER BY resolved_at ASC
      LIMIT 5
    `;
    for (const row of stuck) {
      violations.push({
        id: "one_feedback_per_validation",
        detail: `feedback not applied within 2m for prediction=${row.prediction_id}`,
        predictionId: row.prediction_id,
        gameId: row.game_id,
      });
    }
  } catch (e) {
    logger.debug({ error: String(e) }, "invariant sample skip (feedback)");
  }

  try {
    // Temporal violations on recent pending rows
    const temporal = await db<{
      prediction_id: string;
      target_game_id: string;
      requested_at: string | Date;
      target_round_started_at: string | Date;
    }>`
      SELECT prediction_id, target_game_id, requested_at, target_round_started_at
      FROM pending_predictions
      WHERE target_round_started_at IS NOT NULL
        AND requested_at >= target_round_started_at
      ORDER BY requested_at DESC
      LIMIT 5
    `;
    for (const row of temporal) {
      violations.push({
        id: "prediction_before_target_start",
        detail: `prediction ${row.prediction_id} generated at/after target start`,
        predictionId: row.prediction_id,
        gameId: row.target_game_id,
      });
    }
  } catch (e) {
    logger.debug({ error: String(e) }, "invariant sample skip (temporal)");
  }

  if (violations.length > 0) {
    logger.warn(
      { component: "production-invariants", count: violations.length, violations },
      "production invariant violations detected",
    );
  }

  return { ok: violations.length === 0, violations };
}

/** Soft assert used on hot path — logs only. */
export function assertInvariantSoft(
  violation: InvariantViolation | null,
  context?: Record<string, unknown>,
): void {
  if (!violation) return;
  logger.warn(
    { component: "production-invariants", ...violation, ...context },
    `invariant violated: ${violation.id}`,
  );
}
