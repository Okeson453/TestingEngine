/**
 * Prediction-loss cooldown — risk control independent of the 65% floor.
 *
 * Rules (round-based, not time-based):
 *   ACTIVE
 *     → confirmed LOSS on target T  →  COOLDOWN(skip T+1)
 *   COOLDOWN
 *     → betting prediction for T+1 is forced SKIP (even at 90%+)
 *     → T+1 completes (or skip is consumed)  →  ACTIVE
 *   WIN does not enter cooldown; consecutive prediction losses each
 *   schedule exactly one additional skip round (never bypassed by PR/BG/ED).
 *
 * Only VALIDATED outcomes of ISSUED betting predictions (pending_predictions
 * matched WIN/LOSS after durable outbox handoff) arm this machine.
 * Strategy veto / skipped_no_edge / never-issued rows do not create pending
 * rows and therefore cannot trigger cooldown. Intermediate ED/BG crashes do not.
 *
 * State is durable in worker_state so restart/fencing/recovery cannot bypass
 * an armed skip.
 */

import { getLogger } from "@/lib/observability/logger";
import type { Sql } from "@/lib/db";

const logger = getLogger("prediction-loss-cooldown");

export const LOSS_COOLDOWN_STATE_KEY = "prediction_loss_cooldown_v1";

export type LossCooldownSnapshot = {
  consecutivePredictionLosses: number;
  /** Betting rounds still required to skip. */
  skipRemaining: number;
  /** Target game id that recorded the LOSS. */
  lossTargetGameId: string | null;
  /** Target that must not receive a betting signal. */
  skipTargetGameId: string | null;
  /** Recent validation keys for idempotency across restart (bounded). */
  seenValidationKeys?: string[];
};

let consecutivePredictionLosses = 0;
let skipRemaining = 0;
let lossTargetGameId: string | null = null;
let skipTargetGameId: string | null = null;

/** Processed validation keys — duplicate ED cannot re-arm cooldown. */
const seenValidations = new Set<string>();
const SEEN_MAX = 500;

let persistQueued = false;
let persistSqlFn: (() => Promise<Sql>) | null = null;

/** Wire a SQL factory so state changes can durable-write without blocking. */
export function setLossCooldownPersistSql(fn: (() => Promise<Sql>) | null): void {
  persistSqlFn = fn;
}

function schedulePersist(): void {
  if (!persistSqlFn || persistQueued) return;
  persistQueued = true;
  setImmediate(() => {
    persistQueued = false;
    void persistLossCooldownNow().catch(() => undefined);
  });
}

export async function persistLossCooldownNow(): Promise<void> {
  if (!persistSqlFn) return;
  try {
    const sql = await persistSqlFn();
    const snap: LossCooldownSnapshot = {
      ...getLossCooldownState(),
      seenValidationKeys: [...seenValidations].slice(-100),
    };
    const payload = JSON.stringify(snap);
    await sql`
      INSERT INTO worker_state (key, value, updated_at)
      VALUES (${LOSS_COOLDOWN_STATE_KEY}, ${payload}, now())
      ON CONFLICT (key) DO UPDATE
        SET value = excluded.value, updated_at = excluded.updated_at
    `;
  } catch (e) {
    logger.warn(
      { error: String(e) },
      "loss cooldown durable persist failed (in-memory state retained)",
    );
  }
}

/** Load durable cooldown before LIVE_N1_READY so restart cannot bypass skip. */
export async function loadLossCooldownFromSql(sql: Sql): Promise<LossCooldownSnapshot | null> {
  try {
    const rows = await sql<{ value: unknown }>`
      SELECT value FROM worker_state WHERE key = ${LOSS_COOLDOWN_STATE_KEY} LIMIT 1
    `;
    const raw = rows[0]?.value;
    if (raw == null) return null;
    const snap =
      typeof raw === "string"
        ? (JSON.parse(raw) as Partial<LossCooldownSnapshot>)
        : (raw as Partial<LossCooldownSnapshot>);
    restoreLossCooldown(snap);
    logger.info(
      { component: "prediction-loss-cooldown", ...getLossCooldownState() },
      "loss cooldown restored from worker_state",
    );
    return getLossCooldownState();
  } catch (e) {
    logger.warn(
      { error: String(e) },
      "loss cooldown restore failed — starting ACTIVE",
    );
    return null;
  }
}

function nextNumericId(gameId: string): string | null {
  if (!/^\d+$/.test(gameId)) return null;
  try {
    return String(BigInt(gameId) + 1n);
  } catch {
    return null;
  }
}

function rememberValidation(key: string): boolean {
  if (seenValidations.has(key)) return false;
  seenValidations.add(key);
  if (seenValidations.size > SEEN_MAX) {
    const first = seenValidations.values().next().value;
    if (first != null) seenValidations.delete(first);
  }
  return true;
}

export function getLossCooldownState(): LossCooldownSnapshot {
  return {
    consecutivePredictionLosses,
    skipRemaining,
    lossTargetGameId,
    skipTargetGameId,
  };
}

/** Test/reset helper. */
export function resetLossCooldownForTests(): void {
  consecutivePredictionLosses = 0;
  skipRemaining = 0;
  lossTargetGameId = null;
  skipTargetGameId = null;
  seenValidations.clear();
}

/**
 * Restore after worker restart (best-effort). Does not clear skip if
 * snapshot still has remaining skips.
 */
export function restoreLossCooldown(snap: Partial<LossCooldownSnapshot> | null | undefined): void {
  if (!snap) return;
  if (typeof snap.consecutivePredictionLosses === "number") {
    consecutivePredictionLosses = Math.max(0, Math.floor(snap.consecutivePredictionLosses));
  }
  if (typeof snap.skipRemaining === "number") {
    skipRemaining = Math.max(0, Math.floor(snap.skipRemaining));
  }
  if (typeof snap.lossTargetGameId === "string") lossTargetGameId = snap.lossTargetGameId;
  if (typeof snap.skipTargetGameId === "string") skipTargetGameId = snap.skipTargetGameId;
  if (Array.isArray(snap.seenValidationKeys)) {
    for (const k of snap.seenValidationKeys.slice(-SEEN_MAX)) {
      if (typeof k === "string") seenValidations.add(k);
    }
  }
}

/**
 * Confirmed WIN/LOSS for a matched pending prediction.
 * Duplicate predictionId+result is ignored.
 */
export function noteValidatedPredictionOutcome(args: {
  predictionId: string;
  targetGameId: string;
  result: "WIN" | "LOSS";
}): void {
  const key = `${args.predictionId}:${args.result}`;
  if (!rememberValidation(key)) return;

  if (args.result === "WIN") {
    consecutivePredictionLosses = 0;
    logger.info(
      {
        component: "prediction-loss-cooldown",
        predictionId: args.predictionId,
        targetGameId: args.targetGameId,
        skipRemaining,
      },
      "prediction WIN — consecutive loss streak cleared (active skip unchanged)",
    );
    schedulePersist();
    return;
  }

  // LOSS on issued prediction targeting N → mandatory skip of N+1 only.
  // Identity is the losing target (args.targetGameId), not "whatever is current".
  consecutivePredictionLosses += 1;
  const lossN = args.targetGameId;
  const next = nextNumericId(lossN);
  lossTargetGameId = lossN;
  skipTargetGameId = next;
  skipRemaining = Math.max(skipRemaining, 1);

  logger.info(
    {
      component: "prediction-loss-cooldown",
      event: "COOLDOWN_ARMED",
      predictionId: args.predictionId,
      lossTargetGameId: lossN,
      skipTargetGameId: next,
      skipRemaining,
      consecutivePredictionLosses,
    },
    `prediction LOSS — cooldown armed: skip next betting round target=${next ?? "next-attempt"} (loss_on=${lossN})`,
  );
  schedulePersist();
}

/**
 * PR-primary race: prediction for N+1 is often issued at PR(N) *before*
 * ED(N) validates LOSS on the issued prediction for N. After arming, kill
 * undelivered outbox rows and retire unmatched pending for the skip target
 * so no betting signal remains live. History/crash ingestion for N+1 is
 * unaffected (validator still records the round).
 */
export async function suppressCooldownTargetBetting(
  sql: Sql,
  skipTarget: string,
  lossTarget: string,
): Promise<{ outboxKilled: number; pendingRetired: number }> {
  let outboxKilled = 0;
  let pendingRetired = 0;
  try {
    const killed = await sql<{ notification_id: string }>`
      UPDATE notification_outbox
      SET status = 'dead_letter',
          last_error = ${`loss_cooldown_suppress: LOSS on ${lossTarget} requires skip of ${skipTarget}`},
          updated_at = now()
      WHERE type = 'prediction'
        AND target_game_id = ${skipTarget}
        AND status IN ('pending', 'inflight')
      RETURNING notification_id
    `;
    outboxKilled = killed.length;
  } catch (e) {
    logger.warn(
      { skipTarget, lossTarget, error: String(e) },
      "cooldown outbox suppress failed (soft)",
    );
  }
  try {
    // Retire unmatched pending so ED(N+1) does not grade a suppressed bet.
    // matched=true without a validation row is intentional — cooldown skip,
    // not a WIN/LOSS. Prevents re-arming cooldown from a phantom grade.
    const retired = await sql<{ prediction_id: string }>`
      UPDATE pending_predictions
      SET matched = true,
          matched_at = now(),
          matched_game_id = target_game_id
      WHERE target_game_id = ${skipTarget}
        AND matched = false
      RETURNING prediction_id
    `;
    pendingRetired = retired.length;
  } catch (e) {
    logger.warn(
      { skipTarget, lossTarget, error: String(e) },
      "cooldown pending retire failed (soft)",
    );
  }
  logger.info(
    {
      component: "prediction-loss-cooldown",
      event: "COOLDOWN_SUPPRESS",
      skipTarget,
      lossTarget,
      outboxKilled,
      pendingRetired,
    },
    `COOLDOWN_SUPPRESS target=${skipTarget} after LOSS on ${lossTarget} outbox_killed=${outboxKilled} pending_retired=${pendingRetired}`,
  );
  return { outboxKilled, pendingRetired };
}

/**
 * True when this target must not emit a betting signal.
 * Enforced at strategy/selectivity — PR/BG/ED all share shouldSkipReason.
 */
export function shouldForceLossCooldownSkip(targetGameId: string): {
  skip: boolean;
  reason: string | null;
} {
  if (skipRemaining <= 0) return { skip: false, reason: null };

  if (skipTargetGameId != null) {
    if (String(targetGameId) === String(skipTargetGameId)) {
      return {
        skip: true,
        reason: `loss_cooldown: mandatory skip after LOSS on ${lossTargetGameId}`,
      };
    }
    // Numeric ordering: any target still at/before the skip target while
    // skipRemaining>0 stays blocked (reconnect/out-of-order protection).
    if (/^\d+$/.test(targetGameId) && /^\d+$/.test(skipTargetGameId)) {
      if (BigInt(targetGameId) <= BigInt(skipTargetGameId) && lossTargetGameId != null) {
        if (BigInt(targetGameId) > BigInt(lossTargetGameId)) {
          return {
            skip: true,
            reason: `loss_cooldown: mandatory skip after LOSS on ${lossTargetGameId}`,
          };
        }
      }
    }
    return { skip: false, reason: null };
  }

  // Non-numeric ids: skip the next prediction attempt only.
  return {
    skip: true,
    reason: `loss_cooldown: mandatory skip after LOSS on ${lossTargetGameId}`,
  };
}

/** Call when a betting signal for target was forced to NO_BET by cooldown. */
export function consumeLossCooldownSkip(targetGameId: string): void {
  const check = shouldForceLossCooldownSkip(targetGameId);
  if (!check.skip) return;
  skipRemaining = Math.max(0, skipRemaining - 1);
  if (skipRemaining === 0) {
    skipTargetGameId = null;
  }
  logger.info(
    {
      component: "prediction-loss-cooldown",
      targetGameId,
      skipRemaining,
    },
    "loss cooldown skip consumed",
  );
  schedulePersist();
}

/**
 * When the skip-target round completes (ED/history), clear cooldown even if
 * no prediction attempt ran (e.g. deploy gap). Crash is still ingested for
 * history/model — only betting is skipped.
 */
export function noteRoundCompletedForCooldown(gameId: string): void {
  if (skipRemaining <= 0 || skipTargetGameId == null) return;
  if (String(gameId) !== String(skipTargetGameId)) return;
  skipRemaining = 0;
  skipTargetGameId = null;
  logger.info(
    { component: "prediction-loss-cooldown", gameId },
    "loss cooldown cleared — skip-target round completed",
  );
  schedulePersist();
}
