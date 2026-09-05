/**
 * Synchronous-on-event predictor.
 *
 * Spec: TestingEngine_Comprehensive_Diagnosis_and_Solution.md §5–§7
 *
 * PRIMARY production path (ahead-of-time N+1):
 *   ED(N) → onGameEndPredict → persist pending for N+1 with generated_at
 *   BG(N+1) → backfill target_round_started_at only (never creates prediction)
 *
 * Hard temporal invariant:
 *   prediction_generated_at < target_round_started_at < target_round_crashed_at
 *
 * `onGameStart` is retained only for tests / emergency recovery. Production
 * Socket.IO handlers must NOT call it to create predictions.
 */
import { randomUUID } from "node:crypto";
import { getSql, getPgPool, type Sql } from "@/lib/db";
import { runInTransaction } from "@/lib/prediction/live/tx";
import { PredictionEngine } from "@/lib/prediction/prediction-engine";
import type { HistoricalRound, ThresholdTarget } from "@/lib/prediction/types";
import { getConfiguredChatIds } from "@/lib/notifications/telegram";
import { getLogger } from "@/lib/observability/logger";

import {
  evaluateSheath,
  recordPredictionOutcome,
} from "@/lib/core/sheath-mode";

const logger = getLogger("live-predictor");

/** Prediction-related constants. */
const DEFAULT_TARGET: ThresholdTarget = 1.3;
const MIN_HISTORY = 20;
const MAX_HISTORY = 100;
/** SLA gate: if the bg payload's `beginTime` is older than this, the
 *  prediction is still persisted (correctness preserved) but the Telegram
 *  outbox writes are skipped to avoid the "predicts the past" operator
 *  symptom. */
export const SLA_LAG_MS = Number(process.env.SLA_LAG_MS ?? 2_000);
/** Residual window below which we skip prediction entirely (no row written). */
export const MIN_REQUIRED_WINDOW_MS = Number(process.env.MIN_REQUIRED_WINDOW_MS ?? 800);
/** Stronger short-circuit: if residual is below this, skip even earlier. */
export const SKIP_BELOW_MS = Number(process.env.SKIP_BELOW_MS ?? 500);
/** Hard timeout for PredictionEngine.predict (ms). */
export const PREDICT_TIMEOUT_MS = Number(process.env.PREDICT_TIMEOUT_MS ?? 80);
/** DB-level CHECK constraint cap: a bg payload whose `beginTime` is in the
 *  future of the prediction row's `prediction_generated_at` is rejected. */
/** Default 500ms (was 100) — P1 recommendation. */
export const TEMPORAL_TOLERANCE_MS = Number(process.env.TEMPORAL_TOLERANCE_MS ?? 500);

export interface GameStartEvent {
  gameId: string;
  beginTime: string;
  hash: string | null;
  salt: string | null;
  /** The most recently settled round (BC.Game `gameId`); used for the
   *  `source_round_game_id` column. May be null on the first bg after
   *  boot, in which case the cold-start seeder must have already populated
   *  history. */
  sourceRoundGameId: string | null;
  receivedAt: string;
}

export type OnGameStartResult =
  | {
      kind: "predicted";
      predictionId: string;
      targetGameId: string;
      targetBeganAt: string;
      predictionGeneratedAt: string;
      latencyMs: number;
      slaViolated: boolean;
      correlationId: string;
      outboxEnqueued: number;
    }
  | { kind: "duplicate"; predictionId: string; targetGameId: string }
  | { kind: "no_history"; available: number; targetGameId: string }
  | { kind: "temporal_violation"; targetGameId: string; beginTime: string; reason: string }
  | { kind: "sla_violated_no_outbox"; predictionId: string; targetGameId: string };

interface PredictorDeps {
  getSqlFn?: () => Promise<Sql>;
  /** Injected for tests; default uses the real `PredictionEngine`. */
  predictFn?: (
    priorRounds: HistoricalRound[],
    targetRoundId: string,
    timestamp: string,
    target: ThresholdTarget,
  ) => {
    predictionId: string;
    probability: number;
    confidence: number;
    regimeId: string | null;
    reasoning: string[];
    featureSummary: Record<string, unknown>;
    modelVersion: string;
  };
  /** Injected for tests; default reads env. */
  getChatIds?: () => string[];
  /** Injected for tests; default uses `Date.now`. */
  now?: () => number;
  /** Injected for tests. */
  minHistory?: number;
  /** Injected for tests. */
  slaLagMs?: number;
  /** Injected for tests. */
  temporalToleranceMs?: number;
  /**
   * Poll/recovery path: newest crash may be seconds old. Do not apply the
   * elapsed-since-ED residual estimate (it always looks "too late"). Hard
   * checks (target started/crashed/duplicate) still apply.
   */
  recoveryMode?: boolean;
}

interface PriorRow {
  game_id: string;
  multiplier: string | number;
  began_at: string | Date | null;
  crashed_at: string | Date;
}

function mapRowToHistorical(r: PriorRow): HistoricalRound {
  const crashedAt =
    r.crashed_at instanceof Date ? r.crashed_at.toISOString() : String(r.crashed_at);
  const beganAt =
    r.began_at instanceof Date
      ? r.began_at.toISOString()
      : r.began_at
        ? String(r.began_at)
        : null;
  return {
    id: r.game_id,
    externalRoundId: r.game_id,
    sessionId: null,
    startedAt: beganAt,
    crashedAt,
    crashPoint: Number(r.multiplier),
    observationSource: "bc-game-socket",
    dataQuality: "high",
    createdAt: crashedAt,
    sequenceIndex: undefined,
  };
}

/** Singleton PredictionEngine — avoid re-allocating FeatureEngine/RegimeDetector/Registry per call (P1 #5). */
let cachedEngine: PredictionEngine | null = null;
function getSharedPredictionEngine(): PredictionEngine {
  cachedEngine ??= new PredictionEngine();
  return cachedEngine;
}

const USE_ADVANCED_PIPELINE =
  (process.env.USE_ADVANCED_PIPELINE ?? "1") !== "0";

type PipelineFn = (input: {
  baseProbability: number;
  regime: string;
  regimeConfidence?: number;
  predictionId?: string;
  modelVersion?: string;
  baseThreshold?: number;
}) => {
  calibratedProbability: number;
  metaProbability: number;
  action: string;
  reason: string;
  threshold: number;
};

let cachedPipelineFn: PipelineFn | null | undefined;
function getPipelineFn(): PipelineFn | null {
  if (cachedPipelineFn !== undefined) return cachedPipelineFn;
  try {
    // Dynamic path so a broken advanced-pipeline dependency cannot take down the live worker.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const mod = require("../../prediction/prediction-pipeline.ts") as {
      runPredictionPipeline: PipelineFn;
    };
    cachedPipelineFn = mod.runPredictionPipeline;
  } catch {
    try {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const mod = require("@/lib/prediction/prediction-pipeline") as {
        runPredictionPipeline: PipelineFn;
      };
      cachedPipelineFn = mod.runPredictionPipeline;
    } catch {
      cachedPipelineFn = null;
    }
  }
  return cachedPipelineFn;
}

const defaultPredictFn = (
  priorRounds: HistoricalRound[],
  targetRoundId: string,
  timestamp: string,
  target: ThresholdTarget,
) => {
  const engine = getSharedPredictionEngine();
  const signal = engine.predict({
    priorRounds,
    targetRoundId,
    timestamp,
    target,
  });

  let probability = signal.probability;
  let confidence = signal.confidence;
  let modelVersion = signal.modelVersion ?? "live-v2";
  let reasoning: string[] = Array.isArray(signal.reasoning)
    ? [...signal.reasoning]
    : signal.reasoning
      ? [String(signal.reasoning)]
      : [];

  // Optional Phases 4–8 (was dead code). Soft-fail keeps baseline signal.
  if (USE_ADVANCED_PIPELINE) {
    try {
      const runPipeline = getPipelineFn();
      if (!runPipeline) throw new Error("pipeline_unavailable");
      const pipe = runPipeline({
        baseProbability: signal.probability,
        regime: signal.regimeId ?? "unknown",
        regimeConfidence: 0.6,
        predictionId: signal.predictionId,
        modelVersion: signal.modelVersion,
        baseThreshold: Number(target),
      });
      probability =
        pipe.calibratedProbability ?? pipe.metaProbability ?? probability;
      confidence = Math.min(1, Math.max(confidence, probability));
      modelVersion = `${modelVersion}+pipeline`;
      reasoning.push(
        `pipeline_action=${pipe.action}`,
        `pipeline_reason=${pipe.reason}`,
        `threshold=${pipe.threshold}`,
      );
    } catch {
      /* keep baseline — pipeline may be unavailable in pure unit tests */
    }
  }

  return {
    predictionId: signal.predictionId,
    probability,
    confidence,
    regimeId: signal.regimeId,
    reasoning,
    featureSummary: signal.featureSummary,
    modelVersion,
  };
};

async function loadPriorRoundsStrict(
  sql: Sql,
  beganAt: string,
  limit: number,
): Promise<HistoricalRound[]> {
  const rows = await sql<PriorRow>`
    select game_id, multiplier, began_at, crashed_at
    from crash_rounds
    where crashed_at < ${beganAt}::timestamptz
      and crashed_at is not null
    order by crashed_at desc, game_id desc
    limit ${limit}
  `;
  return rows.reverse().map(mapRowToHistorical);
}

/**
 * Handle a BC.Game `bg` (begin) event.
 *
 * The function is synchronous from the caller's perspective (it awaits the
 * single transaction) and is responsible for:
 *  1. SLA-gate: if `now - beginTime > SLA_LAG_MS`, set `slaViolated = true`
 *     and skip outbox writes (prediction still persists).
 *  2. Duplicate check: SELECT pending_predictions for the same target
 *     `game_id`; if found, return `{ kind: 'duplicate' }` (idempotency).
 *  3. tx1 — atomic: anchor `crash_rounds` row, INSERT prediction,
 *     INSERT outbox rows, INSERT `live_event_log` row. The strict temporal
 *     invariant `prediction_generated_at < target_round_began_at` holds at
 *     the COMMIT of this transaction.
 *  4. If `target_round_began_at > now + TEMPORAL_TOLERANCE_MS` (future-dated
 *     payload, clock skew or replay attack), return `{ kind:
 *     'temporal_violation' }` and DO NOT insert.
 */
export async function onGameStart(
  evt: GameStartEvent,
  deps: PredictorDeps = {},
): Promise<OnGameStartResult> {
  const getSqlFn = deps.getSqlFn ?? getSql;
  const predictFn = deps.predictFn ?? defaultPredictFn;
  const getChatIds = deps.getChatIds ?? getConfiguredChatIds;
  const now = deps.now ?? Date.now;
  const minHistory = deps.minHistory ?? MIN_HISTORY;
  const slaLagMs = deps.slaLagMs ?? SLA_LAG_MS;
  const temporalToleranceMs = deps.temporalToleranceMs ?? TEMPORAL_TOLERANCE_MS;

  const correlationId = randomUUID();
  const beginMs = new Date(evt.beginTime).getTime();
  const receivedMs = new Date(evt.receivedAt).getTime();
  const slaLagMsActual = receivedMs - beginMs;
  const slaViolated = slaLagMsActual > slaLagMs;

  // Step 1: temporal violation — bg payload claims a future beginTime.
  // Defends against clock skew / replay attack / future-dated payload.
  if (beginMs > now() + temporalToleranceMs) {
    logger.error(
      {
        component: "live-predictor",
        correlationId,
        targetGameId: evt.gameId,
        beginTime: evt.beginTime,
        now: new Date(now()).toISOString(),
      },
      "temporal violation: beginTime is in the future; skipping prediction",
    );
    return {
      kind: "temporal_violation",
      targetGameId: evt.gameId,
      beginTime: evt.beginTime,
      reason: "beginTime in the future",
    };
  }

  // Defensive: if beginTime is invalid, treat as a parse failure.
  if (!Number.isFinite(beginMs)) {
    return {
      kind: "temporal_violation",
      targetGameId: evt.gameId,
      beginTime: evt.beginTime,
      reason: "beginTime unparseable",
    };
  }

  const sql = await getSqlFn();

  // Refine the temporal check using the DB clock when available —
  // PGLite/Neon, containerized runners, and CI can have drifted clocks
  // relative to the host, and the DB clock is the canonical
  // `requested_at` source.
  let refNow = now();
  try {
    const dbNow = await sql<{ now: string | Date }>`select now() as now`;
    if (dbNow[0]?.now) {
      const v = dbNow[0].now;
      const dbMs = v instanceof Date ? v.getTime() : new Date(v).getTime();
      if (Number.isFinite(dbMs)) refNow = dbMs;
    }
  } catch {
    /* fall back to local clock */
  }
  if (beginMs > refNow + temporalToleranceMs) {
    logger.error(
      {
        component: "live-predictor",
        correlationId,
        targetGameId: evt.gameId,
        beginTime: evt.beginTime,
        refNowIso: new Date(refNow).toISOString(),
      },
      "temporal violation (db-clock): beginTime is in the future; skipping prediction",
    );
    return {
      kind: "temporal_violation",
      targetGameId: evt.gameId,
      beginTime: evt.beginTime,
      reason: "beginTime in the future (db clock)",
    };
  }

  // Step 2: duplicate check. Partial unique index on target_game_id in 0008
  // makes the second INSERT a no-op too, so the belt is the SELECT and the
  // suspenders are the index.
  const existing = await sql<{ prediction_id: string }>`
    select prediction_id from pending_predictions
    where target_game_id = ${evt.gameId} and matched = false
    limit 1
  `;
  if (existing.length > 0) {
    logger.info(
      {
        component: "live-predictor",
        correlationId,
        targetGameId: evt.gameId,
        predictionId: existing[0]!.prediction_id,
      },
      "duplicate bg event; prediction already exists for this target",
    );
    return {
      kind: "duplicate",
      predictionId: existing[0]!.prediction_id,
      targetGameId: evt.gameId,
    };
  }

  // Step 3: strict causal window — only rounds whose outcome is fully
  // known BEFORE the target began are valid model input.
  const priorRounds = await loadPriorRoundsStrict(sql, evt.beginTime, MAX_HISTORY);
  if (priorRounds.length < minHistory) {
    logger.warn(
      {
        component: "live-predictor",
        correlationId,
        targetGameId: evt.gameId,
        available: priorRounds.length,
        minHistory,
      },
      "insufficient history; skipping prediction (cold-start seeder should run first)",
    );
    return { kind: "no_history", available: priorRounds.length, targetGameId: evt.gameId };
  }

  // Step 4: model inference. Pure CPU. The engine must NEVER see rows
  // whose `crashed_at` is the target round's own (the strict window
  // guarantees that).
  const timestamp = new Date(now()).toISOString();
  const signal = predictFn(priorRounds, evt.gameId, timestamp, DEFAULT_TARGET);

  // Step 5: atomic transaction. The crash_rounds row is anchored first so
  // any consumer querying `WHERE game_id = $G` finds a row even before
  // the prediction is committed. The pending_predictions INSERT is
  // gated by the partial unique index — re-runs of the same bg are
  // no-ops. The outbox rows are written in the same transaction as
  // the prediction so the strict invariant is durable.
  let predictionId: string = signal.predictionId;
  let predictionGeneratedAt = timestamp;
  let outboxEnqueued = 0;

  // chat fan-out is handled by sendTelegramMessage, not per-row inserts
  void getChatIds;

  try {
    await runInTransaction(sql, async (tx) => {
      // Note: we do NOT pre-insert the crash_rounds row here. The schema
      // requires `multiplier` and `crashed_at` to be NOT NULL; both will
      // arrive on the `ed` event. The validator's UPDATE will create the
      // row via `insertNewRounds` (REST backfill) or via the ed handler's
      // own INSERT path on first arrival. Skipping here keeps the
      // transaction idempotent and removes the schema mismatch.

      const ins = await tx<{ prediction_id: string; requested_at: string }>`
        insert into pending_predictions (
          prediction_id, target_multiplier, probability, confidence,
          regime_name, regime_confidence, reasoning, feature_summary,
          model_version, requested_at,
          target_game_id, target_round_started_at, source_round_id,
          correlation_id
        ) values (
          ${predictionId}, ${DEFAULT_TARGET}, ${signal.probability},
          ${signal.confidence}, ${signal.regimeId},
          ${signal.regimeId ? 0.5 : null},
          ${signal.reasoning}, ${JSON.stringify(signal.featureSummary)},
          ${signal.modelVersion}, ${timestamp},
          ${evt.gameId}, ${evt.beginTime}, ${evt.sourceRoundGameId},
          ${correlationId}
        )
        on conflict (prediction_id) do nothing
        returning prediction_id, requested_at
      `;
      if (ins.length === 0) {
        // Lost the race; the unique index caught a duplicate.
        const dup = await tx<{ prediction_id: string }>`
          select prediction_id from pending_predictions
          where target_game_id = ${evt.gameId} and matched = false
          limit 1
        `;
        if (dup.length === 0) {
          throw new Error("PREDICTION_DUPLICATE_BUT_UNREADABLE");
        }
        predictionId = dup[0]!.prediction_id;
        return;
      }
      predictionId = ins[0]!.prediction_id;
      predictionGeneratedAt = String(ins[0]!.requested_at);

      // ONE outbox row per prediction. sendTelegramMessage fans out to all
      // chats — per-chat rows caused duplicate deliveries.
      // SKIP when slaViolated so operators do not see "predicts the past".
      if (!slaViolated) {
        const regimeText = signal.regimeId ? ` (${signal.regimeId})` : "";
        const predictionContent = [
          `🎯 NEW PREDICTION${regimeText}`,
          ``,
          `Target: ${Number(DEFAULT_TARGET).toFixed(2)}x`,
          `Probability: ${(signal.probability * 100).toFixed(1)}%`,
          `Confidence: ${(signal.confidence * 100).toFixed(1)}%`,
          ``,
          `Prediction ID: ${predictionId}`,
          `Generated: ${predictionGeneratedAt}`,
        ].join("\n");
        await tx`
          insert into notification_outbox (
            notification_id, type, content, metadata, status, priority,
            attempt_count, next_attempt_at
          ) values (
            ${randomUUID()}::uuid, 'prediction',
            ${predictionContent},
            ${JSON.stringify({
              predictionId,
              correlationId,
              targetGameId: evt.gameId,
              targetBeganAt: evt.beginTime,
              targetMultiplier: Number(DEFAULT_TARGET),
              probability: signal.probability,
              confidence: signal.confidence,
              regimeName: signal.regimeId,
              slaViolated: false,
              kind: "prediction",
            })},
            'pending', 2,
            0, now()
          )
        `;
        outboxEnqueued = 1;
      }

      // live_event_log row: append-only observability.
      await tx`
        insert into live_event_log (
          correlation_id, event_kind, game_id, payload, received_at, processed_at,
          processor_latency_ms, sla_violated
        ) values (
          ${correlationId}::text, 'BG', ${evt.gameId},
          ${JSON.stringify({ beginTime: evt.beginTime, sourceRoundGameId: evt.sourceRoundGameId })},
          ${evt.receivedAt}::timestamptz, now(),
          ${Math.max(0, now() - receivedMs)}, ${slaViolated}
        )
      `;
    });
  } catch (e) {
    logger.error(
      {
        component: "live-predictor",
        correlationId,
        targetGameId: evt.gameId,
        error: String(e),
      },
      "predictor.onGameStart failed; logging and recording in worker_state",
    );
    // Best-effort: write last_error to worker_state.
    try {
      await sql`
        insert into worker_state (key, value)
        values ('last_error', ${String(e)})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
    } catch {
      /* ignore */
    }
    return { kind: "temporal_violation", targetGameId: evt.gameId, beginTime: evt.beginTime, reason: String(e) };
  }

  logger.info(
    {
      component: "live-predictor",
      correlationId,
      predictionId,
      targetGameId: evt.gameId,
      targetBeganAt: evt.beginTime,
      latencyMs: now() - receivedMs,
      slaViolated,
      outboxEnqueued,
    },
    slaViolated
      ? "prediction persisted; SLA-gate suppressed outbox writes"
      : "prediction generated and persisted for next round",
  );

  if (slaViolated) {
    return {
      kind: "sla_violated_no_outbox",
      predictionId,
      targetGameId: evt.gameId,
    };
  }
  return {
    kind: "predicted",
    predictionId,
    targetGameId: evt.gameId,
    targetBeganAt: evt.beginTime,
    predictionGeneratedAt,
    latencyMs: now() - receivedMs,
    slaViolated: false,
    correlationId,
    outboxEnqueued,
  };
}


/**
 * CRITICAL (TestingEngine_Deep_Diagnosis.md §3.2): Generate prediction for
 * Round N+1 immediately after Round N ends. This is the ONLY correct trigger
 * point for ahead-of-time prediction so that
 *   prediction_generated_at < target_round_started_at
 * always holds (target_round_started_at is backfilled later on the bg event).
 */
export type TemporalValidity = "TEMPORALLY_VALID" | "TEMPORALLY_UNVERIFIED" | "TEMPORALLY_INVALID";

export interface OnGameEndPredictResult {
  predictionId: string | null;
  targetGameId: string;
  kind:
    | "predicted"
    | "duplicate"
    | "too_late"
    | "skipped_late"
    | "insufficient_history"
    | "error"
    | "temporally_invalid";
  /** Proven only when targetStartedAt is known and remainingBeforeTargetMs > 0 */
  temporalValidity?: TemporalValidity;
  sourceGameId?: string;
  sourceCrashAt?: string;
  targetStartedAt?: string | null;
  predictionGeneratedAt?: string;
  predictionLatencyMs?: number;
  availableWindowMs?: number | null;
  remainingBeforeTargetMs?: number | null;
}

export async function onGameEndPredict(
  gameId: string,
  crashedAt: string,
  multiplier: number,
  correlationId: string,
  deps: PredictorDeps = {},
): Promise<OnGameEndPredictResult> {
  const getSqlFn = deps.getSqlFn ?? getSql;
  const predictFn = deps.predictFn ?? defaultPredictFn;
  const sql = await getSqlFn();
  const targetGameId = (BigInt(gameId) + 1n).toString();
  const generatedAt = new Date().toISOString();

  // === Timing-critical gate (findings 3.1, 3.2, 3.6) ===
  // P1.1: Parallelize independent SELECTs using Promise.all
  const tGate0 = performance.now();
  const GENERATION_BUDGET_MS = Number(process.env.GENERATION_BUDGET_MS ?? 500);
  const DELIVERY_BUDGET_MS = Number(process.env.DELIVERY_BUDGET_MS ?? 200);
  try {
    let skipThreshold = Math.min(MIN_REQUIRED_WINDOW_MS, SKIP_BELOW_MS);

    // P1.1: Use Promise.all for parallel DB queries
    const [thrRows, residualRows, gapRows, skewRows, existingPending, liveLifecycle, alreadyCrashedRows] =
      await Promise.all([
        sql<{ value: string }>`
          SELECT value FROM worker_state WHERE key = 'effective_skip_below_ms' LIMIT 1
        `.catch(() => []),
        sql<{ began_at: string | Date | null }>`
          SELECT began_at FROM live_round_state WHERE game_id = ${targetGameId} LIMIT 1
        `.catch(() => []),
        sql<{ value: string }>`
          SELECT value FROM worker_state WHERE key = 'median_inter_round_gap_ms' LIMIT 1
        `.catch(() => []),
        sql<{ value: string }>`
          SELECT value FROM worker_state WHERE key = 'wall_clock_skew_ms' LIMIT 1
        `.catch(() => []),
        sql<{ prediction_id: string }>`
          SELECT prediction_id FROM pending_predictions
          WHERE target_game_id = ${targetGameId} AND status = 'PENDING' LIMIT 1
        `.catch(() => []),
        sql<{ lifecycle: string | null; began_at: string | Date | null }>`
          SELECT lifecycle, began_at FROM live_round_state WHERE game_id = ${targetGameId} LIMIT 1
        `.catch(() => []),
        sql<{ game_id: string }>`
          SELECT game_id FROM crash_rounds WHERE game_id = ${targetGameId} LIMIT 1
        `.catch(() => []),
      ]);

    if (thrRows[0]?.value) {
      const t = Number(thrRows[0].value);
      if (Number.isFinite(t) && t >= 300 && t <= 5_000) {
        skipThreshold = Math.max(skipThreshold, t);
      }
    }

    // Fast reject: already have pending / already crashed / target started
    if (existingPending.length > 0) {
      return {
        predictionId: existingPending[0]!.prediction_id,
        targetGameId,
        kind: "duplicate",
      };
    }
    if (alreadyCrashedRows.length > 0) {
      logger.warn({ targetGameId }, "Target round N+1 already crashed — too late");
      recordPredictionOutcome(true);
      return { predictionId: null, targetGameId, kind: "too_late" };
    }
    const life = liveLifecycle[0];
    if (life?.began_at != null) {
      const beganMs = new Date(life.began_at).getTime();
      if (Number.isFinite(beganMs) && beganMs <= Date.now()) {
        logger.warn({ targetGameId }, "Target round N+1 already started — too late");
        recordPredictionOutcome(true);
        return { predictionId: null, targetGameId, kind: "too_late" };
      }
    }

    // Predictive residual window
    let remainingMs: number | null = null;
    let medianGapMs = 4_000;
    if (gapRows[0]?.value) {
      const g = Number(gapRows[0].value);
      if (Number.isFinite(g) && g > 500 && g < 30_000) medianGapMs = g;
    }
    const elapsedSinceEd = Date.now() - new Date(crashedAt).getTime();
    const recoveryMode = deps.recoveryMode === true;

    if (residualRows.length > 0 && residualRows[0]!.began_at != null) {
      // Authoritative: target N+1 already has began_at
      remainingMs = new Date(residualRows[0]!.began_at).getTime() - Date.now();
    } else if (recoveryMode || (Number.isFinite(elapsedSinceEd) && elapsedSinceEd > medianGapMs * 1.5)) {
      // Recovery/stale: use conservative residual = medianGap/2 from *now*
      // (not from crash time). Hard checks still reject started/crashed targets.
      remainingMs = Math.floor(medianGapMs / 2);
      logger.info(
        {
          targetGameId,
          elapsedSinceEd,
          medianGapMs,
          remainingMs,
          recoveryMode,
        },
        "recovery/stale: conservative residual estimate (medianGap/2)",
      );
    } else {
      // Hot ED path: estimate time left until typical N+1 start
      remainingMs = medianGapMs - Math.max(0, elapsedSinceEd);
    }

    const skew = skewRows[0]?.value != null ? Number(skewRows[0].value) : 0;
    if (Number.isFinite(skew) && remainingMs != null) {
      remainingMs = remainingMs - skew;
    }

    // Predictive deadline: need generation + delivery budget inside residual
    const deadlineBudget = GENERATION_BUDGET_MS + DELIVERY_BUDGET_MS;
    const effectiveFloor = Math.max(skipThreshold, deadlineBudget);
    if (remainingMs != null && Number.isFinite(remainingMs) && remainingMs < effectiveFloor) {
      logger.warn(
        {
          targetGameId,
          remainingMs,
          threshold: effectiveFloor,
          generationBudgetMs: GENERATION_BUDGET_MS,
          deliveryBudgetMs: DELIVERY_BUDGET_MS,
          elapsedSinceEd,
          recoveryMode,
        },
        "Skipping prediction: predictive deadline gate",
      );
      try {
        await sql`
          INSERT INTO live_event_log (
            correlation_id, event_kind, game_id, payload, received_at, processed_at,
            processor_latency_ms, sla_violated
          ) VALUES (
            ${correlationId}::text, 'PREDICT', ${targetGameId},
            ${JSON.stringify({ kind: "skipped_late", remainingMs, threshold: effectiveFloor, recoveryMode })},
            ${generatedAt}::timestamptz, now(), ${Math.round(performance.now() - tGate0)}, true
          )
          ON CONFLICT DO NOTHING
        `;
      } catch { /* non-fatal */ }
      recordPredictionOutcome(true);
      return {
        predictionId: null,
        targetGameId,
        kind: "skipped_late",
        remainingBeforeTargetMs: remainingMs,
      };
    }
  } catch (gateErr) {
    logger.warn(
      { targetGameId, error: String(gateErr) },
      "deadline gate soft-failed — proceeding with caution",
    );
  }

  // Sheath mode (3.2 / 3.10)
  try {
    const sheath = evaluateSheath();
    if (sheath.decision === "halt") {
      logger.warn(
        { targetGameId, lateRate: sheath.rate, total: sheath.total },
        "Sheath mode HALT — skipping prediction due to elevated late rate",
      );
      recordPredictionOutcome(true);
      return {
        predictionId: null,
        targetGameId,
        kind: "skipped_late",
        remainingBeforeTargetMs: null,
      };
    }
    if (sheath.decision === "warn") {
      logger.warn(
        { targetGameId, lateRate: sheath.rate, total: sheath.total },
        "Sheath mode WARN — late rate elevated",
      );
    }
  } catch (sheathErr) {
    logger.debug({ error: String(sheathErr) }, "sheath optional");
  }
  const gateMs = Math.round(performance.now() - tGate0);

  // P1.8: Add Per-Path Timing Metrics
  const tHist0 = performance.now();
  try {
    // Duplicate / too_late checks already done in parallel gate above.
    const rows = await sql<{
      game_id: string;
      multiplier: string | number;
      began_at: string | Date | null;
      crashed_at: string | Date;
    }>`
      SELECT game_id, multiplier, began_at, crashed_at
      FROM crash_rounds
      WHERE crashed_at <= ${crashedAt}::timestamptz
        AND crashed_at IS NOT NULL
      ORDER BY crashed_at DESC, game_id DESC
      LIMIT ${MAX_HISTORY}
    `;
    const histMs = Math.round(performance.now() - tHist0);

    const rounds = rows.reverse().map(mapRowToHistorical);
    if (rounds.length < MIN_HISTORY) {
      logger.info(
        { targetGameId, availableHistory: rounds.length },
        "Insufficient history for N+1 prediction",
      );
      return { predictionId: null, targetGameId, kind: "insufficient_history" };
    }

    // Cold-start seed for advanced pipeline incremental state (once)
    try {
      const { globalIncrementalState } = await import(
        "@/lib/prediction/state/incremental-state-engine"
      );
      if (globalIncrementalState.snapshot().count === 0 && rounds.length > 0) {
        globalIncrementalState.seed(rounds.map((r) => r.crashPoint));
      }
    } catch {
      /* soft */
    }

    const tPredict0 = performance.now();
    // Hard timeout around sync model inference
    let signal: ReturnType<typeof defaultPredictFn>;
    try {
      signal = await Promise.race([
        Promise.resolve().then(() =>
          predictFn(rounds, targetGameId, generatedAt, DEFAULT_TARGET),
        ),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("PREDICT_TIMEOUT")), PREDICT_TIMEOUT_MS),
        ),
      ]);
    } catch (predictErr) {
      logger.error(
        { targetGameId, error: String(predictErr), predictTimeoutMs: PREDICT_TIMEOUT_MS },
        "model inference timed out or failed",
      );
      return { predictionId: null, targetGameId, kind: "error" };
    }
    const predictMs = Math.round(performance.now() - tPredict0);

    const tPersist0 = performance.now();
    let predictionId: string | null = null;
    let predictionGeneratedAt: string | null = null;

    try {
      await runInTransaction(sql, async (tx) => {
        // Anchor the target crash_rounds row for N+1 so consumers can find it
        // even before prediction is committed. Uses UPSERT for idempotency.
        await tx`
          insert into crash_rounds (game_id, began_at)
          values (${targetGameId}, null)
          on conflict (game_id) do nothing
        `;

        predictionGeneratedAt = generatedAt;
        const ins = await tx<{ prediction_id: string; requested_at: string }>`
          insert into pending_predictions (
            prediction_id, target_multiplier, probability, confidence,
            regime_name, regime_confidence, reasoning, feature_summary,
            model_version, requested_at,
            target_game_id, target_round_started_at, source_round_id,
            correlation_id
          ) values (
            ${signal.predictionId}, ${DEFAULT_TARGET}, ${signal.probability},
            ${signal.confidence}, ${signal.regimeId},
            ${signal.regimeId ? 0.5 : null},
            ${signal.reasoning}, ${JSON.stringify(signal.featureSummary)},
            ${signal.modelVersion}, ${generatedAt},
            ${targetGameId}, null, ${gameId},
            ${correlationId}
          )
          on conflict (prediction_id) do nothing
          returning prediction_id, requested_at
        `;
        if (ins.length > 0) {
          predictionId = ins[0]!.prediction_id;
          predictionGeneratedAt = String(ins[0]!.requested_at);
        } else {
          // Lost the race; the unique index caught a duplicate.
          const dup = await tx<{ prediction_id: string }>`
            select prediction_id from pending_predictions
            where target_game_id = ${targetGameId} and matched = false
            limit 1
          `;
          if (dup.length > 0) {
            predictionId = dup[0]!.prediction_id;
          }
        }

        if (predictionId) {
          // ONE outbox row per prediction. sendTelegramMessage fans out to all
          // chats — per-chat rows caused duplicate deliveries.
          const regimeText = signal.regimeId ? ` (${signal.regimeId})` : "";
          const predictionContent = [
            `🎯 NEW PREDICTION${regimeText}`,
            ``,
            `Target: ${Number(DEFAULT_TARGET).toFixed(2)}x`,
            `Probability: ${(signal.probability * 100).toFixed(1)}%`,
            `Confidence: ${(signal.confidence * 100).toFixed(1)}%`,
            ``,
            `Prediction ID: ${predictionId}`,
            `Generated: ${predictionGeneratedAt}`,
            ``,
            `Source Round: ${gameId} (crashed at ${crashedAt})`,
          ].join("\n");
          await tx`
            insert into notification_outbox (
              notification_id, type, content, metadata, status, priority,
              attempt_count, next_attempt_at
            ) values (
              ${randomUUID()}::uuid, 'prediction',
              ${predictionContent},
              ${JSON.stringify({
                predictionId,
                correlationId,
                targetGameId,
                targetBeganAt: null,
                targetMultiplier: Number(DEFAULT_TARGET),
                probability: signal.probability,
                confidence: signal.confidence,
                regimeName: signal.regimeId,
                slaViolated: false,
                kind: "prediction",
                sourceRoundId: gameId,
                sourceCrashAt: crashedAt,
              })},
              'pending', 3,
              0, now()
            )
          `;
        }

        // live_event_log row: append-only observability.
        await tx`
          insert into live_event_log (
            correlation_id, event_kind, game_id, payload, received_at, processed_at,
            processor_latency_ms, sla_violated
          ) values (
            ${correlationId}::text, 'PREDICT', ${targetGameId},
            ${JSON.stringify({ sourceGameId: gameId, sourceCrashAt: crashedAt })},
            ${generatedAt}::timestamptz, now(),
            ${Math.round(performance.now() - tGate0)}, false
          )
        `;
      });
    } catch (persistErr) {
      logger.error(
        { targetGameId, error: String(persistErr) },
        "persist failed",
      );
      return { predictionId: null, targetGameId, kind: "error" };
    }
    const persistMs = Math.round(performance.now() - tPersist0);

    // P1.8: Emit structured timing metrics
    logger.info({
      component: "timing",
      path: "onGameEndPredict",
      gateMs,
      histMs,
      predictMs,
      persistMs,
      totalMs: Math.round(performance.now() - tGate0),
      targetGameId,
      predictionId,
    }, "prediction timing");

    if (!predictionId) {
      return { predictionId: null, targetGameId, kind: "error" };
    }

    return {
      predictionId,
      targetGameId,
      kind: "predicted",
      temporalValidity: "TEMPORALLY_UNVERIFIED",
      sourceGameId: gameId,
      sourceCrashAt: crashedAt,
      predictionGeneratedAt,
      predictionLatencyMs: predictMs,
      availableWindowMs: remainingMs,
      remainingBeforeTargetMs: remainingMs,
    };
  } catch (e) {
    logger.error(
      { component: "live-predictor", targetGameId, error: String(e) },
      "onGameEndPredict failed",
    );
    return { predictionId: null, targetGameId, kind: "error" };
  }
}
