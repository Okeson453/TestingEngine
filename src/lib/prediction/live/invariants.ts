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
  // Fix plan Phase 14: renamed semantics + new duplicate-state check.
  | "feedback_not_applied_within_sla"
  | "feedback_state_contradiction"
  | "one_crash_result_per_game"
  | "no_prediction_without_history"
  | "no_duplicate_outbox_for_prediction";

export interface InvariantViolation {
  id: InvariantId;
  detail: string;
  gameId?: string;
  predictionId?: string;
  /** What the invariant requires, stated concretely. */
  expected?: string;
  /** What was actually observed in the DB. */
  actual?: string;
  /** Pipeline stage the violation belongs to (persistence, ownership, feedback…). */
  stage?: string;
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
      expected: `prediction generated before target_round_started_at (${new Date(start).toISOString()})`,
      actual: `generated_at=${new Date(gen).toISOString()} (Δ ${Math.round(gen - start)}ms after target start)`,
      stage: "persistence",
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
        expected: "exactly 1 active (PENDING, unmatched) prediction per target round",
        actual: `${row.c} active PENDING predictions for target ${row.target_game_id}`,
        stage: "ownership/persistence",
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
        AND feedback_skip_reason IS NULL
        AND resolved_at < now() - interval '2 minutes'
      ORDER BY resolved_at ASC
      LIMIT 5
    `;
    for (const row of stuck) {
      // Fix plan Phase 14: renamed — this detects FEEDBACK THAT MISSED ITS SLA
      // (never applied), not duplicate feedback. Duplicate/contradictory
      // feedback state is checked by feedback_state_contradiction below.
      violations.push({
        id: "feedback_not_applied_within_sla",
        detail: `feedback not applied within 2m for prediction=${row.prediction_id}`,
        predictionId: row.prediction_id,
        gameId: row.game_id,
        expected: "feedback_applied_at set within 2 minutes of resolved_at",
        actual: `feedback_applied_at IS NULL for prediction=${row.prediction_id} (game ${row.game_id})`,
        stage: "feedback",
      });
    }
  } catch (e) {
    logger.debug({ error: String(e) }, "invariant sample skip (feedback)");
  }

  try {
    // Fix plan Phase 14: duplicate/contradictory feedback state. A row that is
    // BOTH skipped (intentionally never applied) and marked applied is
    // corrupted state — one of the two markers is wrong.
    const contradicted = await db<{ prediction_id: string }>`
      SELECT prediction_id
      FROM prediction_validations
      WHERE feedback_applied_at IS NOT NULL
        AND feedback_skip_reason IS NOT NULL
      LIMIT 5
    `;
    for (const row of contradicted) {
      violations.push({
        id: "feedback_state_contradiction",
        detail: `prediction=${row.prediction_id} has feedback_applied_at AND feedback_skip_reason set`,
        predictionId: row.prediction_id,
        expected: "feedback_applied_at and feedback_skip_reason are mutually exclusive",
        actual: "both set",
        stage: "feedback",
      });
    }
  } catch (e) {
    logger.debug({ error: String(e) }, "invariant sample skip (feedback contradiction)");
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
      const requested = new Date(row.requested_at).toISOString();
      const started = new Date(row.target_round_started_at).toISOString();
      violations.push({
        id: "prediction_before_target_start",
        detail: `prediction ${row.prediction_id} generated at/after target start`,
        predictionId: row.prediction_id,
        gameId: row.target_game_id,
        expected: `requested_at < target_round_started_at (${started})`,
        actual: `requested_at=${requested} (at/after target start)`,
        stage: "persistence",
      });
    }
  } catch (e) {
    logger.debug({ error: String(e) }, "invariant sample skip (temporal)");
  }

  if (violations.length > 0) {
    const fp = violations
      .map((v) => `${v.id}:${v.gameId ?? ""}:${v.predictionId ?? ""}`)
      .join("|");
    const now = Date.now();
    const g = globalThis as { __teInvFp?: string; __teInvAt?: number };
    if (g.__teInvFp === fp && g.__teInvAt != null && now - g.__teInvAt < 60_000) {
      logger.debug(
        { component: "production-invariants", count: violations.length },
        "production invariant violations (deduped)",
      );
    } else {
      g.__teInvFp = fp;
      g.__teInvAt = now;
      logger.warn(
        {
          component: "production-invariants",
          violationCount: violations.length,
          violations: violations.map((v) => ({
            invariant: v.id,
            severity: v.id === "prediction_before_target_start" ? "P0" : "P1",
            detail: v.detail,
            gameId: v.gameId,
            predictionId: v.predictionId,
            expected: v.expected,
            actual: v.actual,
            stage: v.stage,
          })),
        },
        `production invariant violations detected: ${[...new Set(violations.map((v) => v.id))].join(",")}`,
      );
    }
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
