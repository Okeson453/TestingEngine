/**
 * Prediction-loss cooldown — risk control independent of the 65% floor.
 *
 * Rules (round-based, not time-based):
 *   ACTIVE
 *     → confirmed LOSS on target T  →  COOLDOWN starting at T+1
 *   COOLDOWN
 *     → skipRemaining consecutive betting rounds forced SKIP (even at 90%+)
 *     → each completed skip round decrements; when 0 → ACTIVE
 *   WIN clears the consecutive-loss streak (and any active skip window).
 *
 * Escalation (stops multi-loss rolls):
 *   1 consecutive LOSS → skip 1 round
 *   2 consecutive      → skip 2 rounds
 *   3+ consecutive     → skip min(streak, MAX) rounds (default MAX=5)
 *
 * Only VALIDATED outcomes of ISSUED betting predictions arm this machine.
 * State is durable in worker_state so restart cannot bypass an armed skip.
 */

/** Max mandatory skip rounds after a long loss streak. */
const MAX_SKIP_ROUNDS = Math.max(
  1,
  Math.min(10, Number(process.env.LOSS_COOLDOWN_MAX_SKIP ?? 5)),
);

import { getLogger } from "@/lib/observability/logger";
import type { Sql } from "@/lib/db";
import { markCooldownSkipTarget } from "@/lib/prediction/live/target-coordinator";

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
  /** Consecutive completed crashes in [1.00, 1.20]x. */
  lowBandStreak?: number;
  /** Recent validation keys for idempotency across restart (bounded). */
  seenValidationKeys?: string[];
};

/** Crash multipliers in this closed interval count toward the low-band streak. */
const LOW_BAND_MIN = Number(process.env.LOW_BAND_STREAK_MIN ?? 1.0);
const LOW_BAND_MAX = Number(process.env.LOW_BAND_STREAK_MAX ?? 1.2);
/** How many consecutive low-band crashes arm the skip. */
const LOW_BAND_ARM_AT = Math.max(
  1,
  Number(process.env.LOW_BAND_STREAK_ARM_AT ?? 2),
);
/** Betting rounds to skip when low-band streak arms. */
const LOW_BAND_SKIP_ROUNDS = Math.max(
  1,
  Number(process.env.LOW_BAND_STREAK_SKIP_ROUNDS ?? 2),
);

let consecutivePredictionLosses = 0;
let skipRemaining = 0;
let lossTargetGameId: string | null = null;
let skipTargetGameId: string | null = null;
/** Consecutive ED crashes with multiplier in [LOW_BAND_MIN, LOW_BAND_MAX]. */
let lowBandStreak = 0;

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
    lowBandStreak,
  };
}

/** Test/reset helper. */
export function resetLossCooldownForTests(): void {
  consecutivePredictionLosses = 0;
  skipRemaining = 0;
  lossTargetGameId = null;
  skipTargetGameId = null;
  lowBandStreak = 0;
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
    skipRemaining = Math.min(
      MAX_SKIP_ROUNDS,
      Math.max(0, Math.floor(snap.skipRemaining)),
    );
  }
  if (typeof snap.lowBandStreak === "number") {
    lowBandStreak = Math.max(0, Math.floor(snap.lowBandStreak));
  }
  if (typeof snap.lossTargetGameId === "string") lossTargetGameId = snap.lossTargetGameId;
  if (typeof snap.skipTargetGameId === "string") skipTargetGameId = snap.skipTargetGameId;
  if (Array.isArray(snap.seenValidationKeys)) {
    for (const k of snap.seenValidationKeys.slice(-SEEN_MAX)) {
      if (typeof k === "string") seenValidations.add(k);
    }
  }
  // Re-apply ownership terminal so PR cannot reserve the skip target after restart.
  if (skipRemaining > 0 && skipTargetGameId) {
    markCooldownSkipTarget(skipTargetGameId);
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
    // WIN ends any active cooldown window — streak broken.
    skipRemaining = 0;
    skipTargetGameId = null;
    lossTargetGameId = null;
    logger.info(
      {
        component: "prediction-loss-cooldown",
        predictionId: args.predictionId,
        targetGameId: args.targetGameId,
      },
      "prediction WIN — consecutive loss streak + cooldown cleared",
    );
    schedulePersist();
    return;
  }

  // LOSS on issued prediction targeting N → escalate skip window from N+1.
  consecutivePredictionLosses += 1;
  const lossN = args.targetGameId;
  const next = nextNumericId(lossN);
  lossTargetGameId = lossN;
  skipTargetGameId = next;
  // Escalate: 1 loss → 1 skip, 2 → 2, … capped at MAX_SKIP_ROUNDS.
  skipRemaining = Math.min(MAX_SKIP_ROUNDS, consecutivePredictionLosses);

  logger.info(
    {
      component: "prediction-loss-cooldown",
      event: "COOLDOWN_ARMED",
      predictionId: args.predictionId,
      lossTargetGameId: lossN,
      skipTargetGameId: next,
      skipRemaining,
      consecutivePredictionLosses,
      maxSkip: MAX_SKIP_ROUNDS,
    },
    `prediction LOSS — cooldown armed: skip ${skipRemaining} round(s) from target=${next ?? "next-attempt"} (loss_on=${lossN} streak=${consecutivePredictionLosses})`,
  );
  // Mark the first skip target; further targets marked as each prior completes.
  if (next) markCooldownSkipTarget(next);
  // Also mark the full skip window when numeric (blocks PR race across N+1..N+k).
  if (next && /^\d+$/.test(next) && skipRemaining > 1) {
    let cur: string | null = next;
    for (let i = 1; i < skipRemaining && cur; i++) {
      cur = nextNumericId(cur);
      if (cur) markCooldownSkipTarget(cur);
    }
  }
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
/** Last game id in the current skip window (inclusive). */
function skipWindowEndId(): string | null {
  if (skipRemaining <= 0 || skipTargetGameId == null) return null;
  let end = skipTargetGameId;
  for (let i = 1; i < skipRemaining; i++) {
    const n = nextNumericId(end);
    if (!n) break;
    end = n;
  }
  return end;
}

export function shouldForceLossCooldownSkip(targetGameId: string): {
  skip: boolean;
  reason: string | null;
} {
  if (skipRemaining <= 0) return { skip: false, reason: null };

  if (skipTargetGameId != null) {
    const end = skipWindowEndId();
    // Pure check — never mutate here (noteRoundCompleted advances the window).
    if (
      /^\d+$/.test(targetGameId) &&
      lossTargetGameId != null &&
      /^\d+$/.test(lossTargetGameId) &&
      end != null &&
      /^\d+$/.test(end)
    ) {
      const t = BigInt(targetGameId);
      if (t > BigInt(lossTargetGameId) && t <= BigInt(end)) {
        return {
          skip: true,
          reason: `loss_cooldown: skip window after LOSS on ${lossTargetGameId} (${skipRemaining} left)`,
        };
      }
      return { skip: false, reason: null };
    }

    if (String(targetGameId) === String(skipTargetGameId)) {
      return {
        skip: true,
        reason: `loss_cooldown: skip ${skipRemaining} remaining after LOSS on ${lossTargetGameId}`,
      };
    }
    return { skip: false, reason: null };
  }

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
 * Low-band crash streak (1.00x–1.20x): consecutive hits in this band arm a
 * mandatory 2-round betting skip (independent of issued-prediction LOSS).
 * Called on every completed crash (ED), including rounds with no issued bet.
 */
export function noteLowBandCrashStreak(args: {
  gameId: string;
  multiplier: number;
}): void {
  const m = Number(args.multiplier);
  if (!Number.isFinite(m) || m <= 0) return;

  if (m >= LOW_BAND_MIN && m <= LOW_BAND_MAX) {
    lowBandStreak += 1;
  } else {
    if (lowBandStreak > 0) {
      logger.info(
        {
          component: "prediction-loss-cooldown",
          event: "LOW_BAND_STREAK_BROKEN",
          previousStreak: lowBandStreak,
          multiplier: m,
          gameId: args.gameId,
        },
        `low-band streak broken at ${m.toFixed(2)}x (was ${lowBandStreak})`,
      );
    }
    lowBandStreak = 0;
    schedulePersist();
    return;
  }

  if (lowBandStreak < LOW_BAND_ARM_AT) {
    schedulePersist();
    return;
  }

  // Arm / extend skip window: at least LOW_BAND_SKIP_ROUNDS from next round.
  const next = nextNumericId(args.gameId);
  if (!next) {
    schedulePersist();
    return;
  }

  const need = LOW_BAND_SKIP_ROUNDS;
  // Take the stronger of existing LOSS cooldown vs low-band skip.
  if (skipRemaining < need || skipTargetGameId == null) {
    lossTargetGameId = args.gameId;
    skipTargetGameId = next;
    skipRemaining = Math.max(skipRemaining, need);
  } else if (skipRemaining < need) {
    skipRemaining = need;
  }

  let cur: string | null = skipTargetGameId;
  for (let i = 0; i < skipRemaining && cur; i++) {
    markCooldownSkipTarget(cur);
    cur = nextNumericId(cur);
  }

  logger.info(
    {
      component: "prediction-loss-cooldown",
      event: "LOW_BAND_COOLDOWN_ARMED",
      gameId: args.gameId,
      multiplier: m,
      lowBandStreak,
      skipRemaining,
      skipTargetGameId,
      band: `[${LOW_BAND_MIN}, ${LOW_BAND_MAX}]`,
    },
    `low-band streak ${lowBandStreak} in ${LOW_BAND_MIN}-${LOW_BAND_MAX}x — skip ${skipRemaining} betting round(s) from ${skipTargetGameId}`,
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

  const end = skipWindowEndId();
  // Fully past the window (missed intermediate ED) → clear.
  if (
    end != null &&
    /^\d+$/.test(gameId) &&
    /^\d+$/.test(end) &&
    BigInt(gameId) > BigInt(end)
  ) {
    skipRemaining = 0;
    skipTargetGameId = null;
    logger.info(
      { component: "prediction-loss-cooldown", gameId, event: "COOLDOWN_CLEARED", clearedEnd: end },
      "loss cooldown cleared — completed round past skip window",
    );
    schedulePersist();
    return;
  }

  // Consume every skip head ≤ completed gameId.
  if (!/^\d+$/.test(gameId) || !/^\d+$/.test(skipTargetGameId)) {
    if (String(gameId) === String(skipTargetGameId)) {
      skipRemaining = Math.max(0, skipRemaining - 1);
      skipTargetGameId = skipRemaining > 0 ? nextNumericId(gameId) : null;
      if (skipTargetGameId) markCooldownSkipTarget(skipTargetGameId);
      schedulePersist();
    }
    return;
  }

  let advanced = false;
  while (
    skipRemaining > 0 &&
    skipTargetGameId != null &&
    /^\d+$/.test(skipTargetGameId) &&
    BigInt(gameId) >= BigInt(skipTargetGameId)
  ) {
    skipRemaining -= 1;
    skipTargetGameId = nextNumericId(skipTargetGameId);
    advanced = true;
  }
  if (!advanced) return;

  if (skipRemaining <= 0) {
    skipTargetGameId = null;
    logger.info(
      { component: "prediction-loss-cooldown", gameId, event: "COOLDOWN_CLEARED" },
      "loss cooldown cleared — skip window completed",
    );
  } else {
    if (skipTargetGameId) markCooldownSkipTarget(skipTargetGameId);
    logger.info(
      {
        component: "prediction-loss-cooldown",
        gameId,
        event: "COOLDOWN_ADVANCE",
        skipRemaining,
        nextSkipTarget: skipTargetGameId,
      },
      `loss cooldown advanced — ${skipRemaining} skip(s) left, next=${skipTargetGameId}`,
    );
  }
  schedulePersist();
}
