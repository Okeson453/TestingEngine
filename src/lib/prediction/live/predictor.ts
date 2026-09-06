/**
 * Synchronous-on-event predictor.
 *
 * Spec: TestingEngine_Comprehensive_Diagnosis_and_Solution.md §5–§7
 *
 * PRIMARY production path (ahead-of-time N+1):
 *   ED(N) -> onGameEndPredict -> persist pending for N+1 with generated_at
 *   BG(N+1) -> backfill target_round_started_at only (never creates prediction)
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
// SYNTAX_GUARD_20260906: file must parse under node --experimental-strip-types

/** Prediction-related constants. */
const DEFAULT_TARGET: ThresholdTarget = 1.3;
const MIN_HISTORY = 20;
/** Reduced 100->50: halves history query cost on the hot ED path while
 *  remaining well above MIN_HISTORY for model stability. */
const MAX_HISTORY = 50;
/** SLA gate: if the bg payload's `beginTime` is older than this, the
 *  prediction is still persisted (correctness preserved) but the Telegram
 *  outbox writes are skipped to avoid the "predicts the past" operator
 *  symptom. */
export const SLA_LAG_MS = Number(process.env.SLA_LAG_MS ?? 2_000);
/** Residual window below which we skip prediction entirely (no row written).
 *  Lowered 800->250: prior floor systematically skipped hot-ED predictions when
 *  elapsedSinceEd + gate latency consumed a normal 3-5s inter-round gap,
 *  forcing poll recovery 1-3 rounds later (the observed signal lag). */
export const MIN_REQUIRED_WINDOW_MS = Number(process.env.MIN_REQUIRED_WINDOW_MS ?? 250);
/** Stronger short-circuit: only abandon when the window is truly gone. */
export const SKIP_BELOW_MS = Number(process.env.SKIP_BELOW_MS ?? 150);
/** Hard timeout for PredictionEngine.predict (ms). */
export const PREDICT_TIMEOUT_MS = Number(process.env.PREDICT_TIMEOUT_MS ?? 80);
/** Source-event staleness ceiling. If the crash event we're reacting to is
 *  older than this, the live round has almost certainly advanced past the
 *  target. Set to 15s to avoid interfering with normal delayed events (6-10s)
 *  while catching truly stale reconnect bursts (15s+). */
export const MAX_SOURCE_ROUND_AGE_MS = Number(process.env.MAX_SOURCE_ROUND_AGE_MS ?? 30_000);

/** DB-level CHECK constraint cap: a bg payload whose `beginTime` is in the
 *  future of the prediction row's `prediction_generated_at` is rejected. */
/** Default 500ms (was 100) — P1 recommendation. */
export const TEMPORAL_TOLERANCE_MS = Number(process.env.TEMPORAL_TOLERANCE_MS ?? 500);

// P2.10: SLA Alert threshold for prediction timing
export const PREDICTION_SLA_THRESHOLD_MS = Number(process.env.PREDICTION_SLA_THRESHOLD_MS ?? 2000);

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
  getChatIds?: () => string[];
  now?: () => number;
  minHistory?: number;
  slaLagMs?: number;
  temporalToleranceMs?: number;
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

let cachedEngine: PredictionEngine | null = null;
function getSharedPredictionEngine(): PredictionEngine {
  cachedEngine ??= new PredictionEngine();
  return cachedEngine;
}

const USE_ADVANCED_PIPELINE = (process.env.USE_ADVANCED_PIPELINE ?? "1") !== "0";

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
    const mod = require("../../prediction/prediction-pipeline.ts") as {
      runPredictionPipeline: PipelineFn;
    };
    cachedPipelineFn = mod.runPredictionPipeline;
  } catch {
    try {
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
      probability = pipe.calibratedProbability ?? pipe.metaProbability ?? probability;
      confidence = Math.min(1, Math.max(confidence, probability));
      modelVersion = `${modelVersion}+pipeline`;
      reasoning.push(
        `pipeline_action=${pipe.action}`,
        `pipeline_reason=${pipe.reason}`,
        `threshold=${pipe.threshold}`,
      );
    } catch {
      /* keep baseline - pipeline may be unavailable in pure unit tests */
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

export async function onGameStart(
  evt: GameStartEvent,
  deps: PredictorDeps = {},
): Promise<OnGameStartResult> {
  if (process.env.NODE_ENV === "production" && process.env.ALLOW_LEGACY_ON_GAME_START !== "1") {
    const isTest = process.env.VITEST || process.env.NODE_TEST_CONTEXT || process.env.ALLOW_LEGACY_ON_GAME_START;
    if (!isTest) {
      logger.warn(
        {
          component: "live-predictor",
          targetGameId: evt.gameId,
          deprecation: "onGameStart",
        },
        "onGameStart called in production without ALLOW_LEGACY_ON_GAME_START - prefer onGameEndPredict",
      );
    }
  }
  const getSqlFn = deps.getSqlFn ?? getSql;
  const predictFn = deps.predictFn ?? defaultPredictFn;
  const now = deps.now ?? Date.now;
  const minHistory = deps.minHistory ?? MIN_HISTORY;
  const slaLagMs = deps.slaLagMs ?? SLA_LAG_MS;
  const temporalToleranceMs = deps.temporalToleranceMs ?? TEMPORAL_TOLERANCE_MS;

  const correlationId = randomUUID();
  const beginMs = new Date(evt.beginTime).getTime();
  const receivedMs = new Date(evt.receivedAt).getTime();
  const slaLagMsActual = receivedMs - beginMs;
  const slaViolated = slaLagMsActual > slaLagMs;

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

  if (!Number.isFinite(beginMs)) {
    return {
      kind: "temporal_violation",
      targetGameId: evt.gameId,
      beginTime: evt.beginTime,
      reason: "beginTime unparseable",
    };
  }

  const sql = await getSqlFn();

  let refNow = now();
  try {
    const dbNow = await sql<{ now: string | Date }>`select now() as now`;
    if (dbNow[0]?.now) {
      const v = dbNow[0].now;
      const dbMs = v instanceof Date ? v.getTime() : new Date(v).getTime();
      if (Number.isFinite(dbMs)) refNow = dbMs;
    }
  } catch {}

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

  const timestamp = new Date(now()).toISOString();
  const signal = predictFn(priorRounds, evt.gameId, timestamp, DEFAULT_TARGET);

  let predictionId: string = signal.predictionId;
  let predictionGeneratedAt = timestamp;
  let outboxEnqueued = 0;

  try {
    await runInTransaction(sql, async (tx) => {
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

      if (!slaViolated) {
        const regimeText = signal.regimeId ? ` (${signal.regimeId})` : "";
        const predictionContent = [
          `NEW PREDICTION${regimeText}`,
          "",
          `Target: ${Number(DEFAULT_TARGET).toFixed(2)}x`,
          `Probability: ${(signal.probability * 100).toFixed(1)}%`,
          `Confidence: ${(signal.confidence * 100).toFixed(1)}%`,
          "",
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
        errorStack: e instanceof Error ? e.stack : undefined,
        beginTime: evt.beginTime,
        receivedAt: evt.receivedAt,
        slaLagMsActual,
        slaViolated,
      },
      "predictor.onGameStart failed; logging and recording in worker_state",
    );
    try {
      await sql`
        insert into worker_state (key, value)
        values ('last_error', ${String(e)})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
    } catch {}
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
    | "skipped_stale_source"
    | "skipped_invalid_target"
    | "temporally_invalid";
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

  let targetGameId: string;
  try {
    if (typeof gameId !== "string" || !/^\d+$/.test(gameId)) {
      logger.warn(
        { sourceGameId: gameId },
        "Skipping prediction: source gameId is not a safe numeric sequence",
      );
      return {
        predictionId: null,
        targetGameId: String(gameId ?? "unknown"),
        kind: "skipped_invalid_target",
        sourceGameId: String(gameId ?? ""),
        sourceCrashAt: crashedAt,
      };
    }
    const next = BigInt(gameId) + 1n;
    if (next <= 0n) {
      throw new Error("non-positive next id");
    }
    targetGameId = next.toString();
  } catch (e) {
    logger.warn(
      { sourceGameId: gameId, error: String(e) },
      "Skipping prediction: cannot derive safe N+1 target from source gameId",
    );
    return {
      predictionId: null,
      targetGameId: String(gameId ?? "unknown"),
      kind: "skipped_invalid_target",
      sourceGameId: String(gameId ?? ""),
      sourceCrashAt: crashedAt,
    };
  }

  const generatedAt = new Date().toISOString();
  const recoveryMode = deps.recoveryMode === true;
  
  // FIX 1: Source-staleness gate now respects recoveryMode
  // In recovery mode (poll worker), skip the staleness check since poll worker
  // intentionally processes slightly delayed rounds. Hard checks (target started/crashed/duplicate) still apply.
  {
    const sourceAgeMs = Date.now() - new Date(crashedAt).getTime();
    
    // Only enforce staleness check in non-recovery mode
    if (!recoveryMode && Number.isFinite(sourceAgeMs) && sourceAgeMs > MAX_SOURCE_ROUND_AGE_MS) {
      logger.warn(
        { targetGameId, sourceGameId: gameId, sourceAgeMs, crashedAt },
        "Skipping prediction: source round crash event is stale - live round has likely advanced past this target",
      );
      try {
        await sql`
          INSERT INTO live_event_log (
            correlation_id, event_kind, game_id, payload, received_at, processed_at,
            processor_latency_ms, sla_violated
          ) VALUES (
            ${correlationId}::text, 'PREDICT', ${targetGameId},
            ${JSON.stringify({ kind: "skipped_stale_source", sourceGameId: gameId, sourceAgeMs, crashedAt })},
            ${generatedAt}::timestamptz, now(), 0, true
          )
          ON CONFLICT DO NOTHING
        `;
      } catch {}
      recordPredictionOutcome(true);
      return {
        predictionId: null,
        targetGameId,
        kind: "skipped_stale_source" as const,
        sourceGameId: gameId,
        sourceCrashAt: crashedAt,
      };
    }
    
    if (recoveryMode && sourceAgeMs > 10_000) {
      logger.info(
        { targetGameId, sourceGameId: gameId, sourceAgeMs, crashedAt },
        "Recovery mode: source round age elevated but acceptable for poll path"
      );
    }
  }

  const GENERATION_BUDGET_MS = Number(process.env.GENERATION_BUDGET_MS ?? 150);
  const DELIVERY_BUDGET_MS = Number(process.env.DELIVERY_BUDGET_MS ?? 100);
  try {
    let skipThreshold = Math.min(MIN_REQUIRED_WINDOW_MS, SKIP_BELOW_MS);

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

    if (existingPending.length > 0) {
      return {
        predictionId: existingPending[0]!.prediction_id,
        targetGameId,
        kind: "duplicate",
      };
    }
    if (alreadyCrashedRows.length > 0) {
      logger.warn({ targetGameId }, "Target round N+1 already crashed - too late");
      recordPredictionOutcome(true);
      return { predictionId: null, targetGameId, kind: "too_late" };
    }
    const life = liveLifecycle[0];
    if (life?.began_at != null) {
      const beganMs = new Date(life.began_at).getTime();
      if (Number.isFinite(beganMs) && beganMs <= Date.now()) {
        logger.warn({ targetGameId }, "Target round N+1 already started - too late");
        recordPredictionOutcome(true);
        return { predictionId: null, targetGameId, kind: "too_late" };
      }
    }

    let remainingMs: number | null = null;
    let medianGapMs = 4_000;
    if (gapRows[0]?.value) {
      const g = Number(gapRows[0].value);
      if (Number.isFinite(g) && g > 500 && g < 30_000) medianGapMs = g;
    }
    const elapsedSinceEd = Date.now() - new Date(crashedAt).getTime();

    if (residualRows.length > 0 && residualRows[0]!.began_at != null) {
      remainingMs = new Date(residualRows[0]!.began_at).getTime() - Date.now();
    } else if (recoveryMode || (Number.isFinite(elapsedSinceEd) && elapsedSinceEd > medianGapMs * 1.5)) {
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
      remainingMs = medianGapMs - Math.max(0, elapsedSinceEd);
    }

    const skew = skewRows[0]?.value != null ? Number(skewRows[0].value) : 0;
    if (Number.isFinite(skew) && remainingMs != null) {
      remainingMs = remainingMs - skew;
    }

    const deadlineBudget = GENERATION_BUDGET_MS + DELIVERY_BUDGET_MS;
    const effectiveFloor = skipThreshold;
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
        "tight residual window - generating anyway; outbox will expire if too late",
      );
    }
    if (
      remainingMs != null &&
      Number.isFinite(remainingMs) &&
      remainingMs < deadlineBudget
    ) {
      logger.info(
        {
          targetGameId,
          remainingMs,
          deadlineBudget,
          elapsedSinceEd,
        },
        "tight residual - generating anyway; outbox may expire if target starts first",
      );
    }
  } catch (gateErr) {
    logger.warn(
      { targetGameId, error: String(gateErr) },
      "deadline gate soft-failed - proceeding with caution",
    );
  }

  try {
    const sheath = evaluateSheath();
    if (sheath.decision === "halt") {
      logger.warn(
        { targetGameId, sheathLevel: sheath.level, sheathReason: sheath.reason },
        "Sheath mode HALT - skipping prediction due to elevated late rate",
      );
      recordPredictionOutcome(true);
      return {
        predictionId: null,
        targetGameId,
        kind: "skipped_late",
        temporalValidity: "TEMPORALLY_UNVERIFIED",
      };
    }
  } catch {}

  const sql2 = await getSqlFn();
  const priorRounds = await loadPriorRoundsStrict(
    sql2,
    crashedAt,
    MAX_HISTORY,
  ).catch((e) => {
    logger.error(
      { targetGameId, error: String(e) },
      "loadPriorRoundsStrict failed",
    );
    throw e;
  });

  if (priorRounds.length < MIN_HISTORY) {
    logger.warn(
      { targetGameId, available: priorRounds.length, minHistory: MIN_HISTORY },
      "insufficient history for prediction",
    );
    recordPredictionOutcome(true);
    return {
      predictionId: null,
      targetGameId,
      kind: "insufficient_history",
      temporalValidity: "TEMPORALLY_UNVERIFIED",
    };
  }

  const timestamp = generatedAt;
  const signal = predictFn(priorRounds, targetGameId, timestamp, DEFAULT_TARGET);
  const predictionId = signal.predictionId;

  try {
    await runInTransaction(sql, async (tx) => {
      const targetStarted = await tx<{ began_at: string | Date | null }>`
        SELECT began_at FROM live_round_state WHERE game_id = ${targetGameId} LIMIT 1
      `;
      
      const targetBeganAt = targetStarted[0]?.began_at;
      if (targetBeganAt != null) {
        const beganMs = new Date(targetBeganAt).getTime();
        const genMs = new Date(timestamp).getTime();
        if (Number.isFinite(beganMs) && Number.isFinite(genMs) && genMs >= beganMs - TEMPORAL_TOLERANCE_MS) {
          logger.error(
            { targetGameId, predictionId, targetBeganAt, generatedAt: timestamp },
            "TEMPORAL INVARIANT VIOLATION: prediction_generated_at >= target_round_started_at",
          );
          throw new Error("TEMPORAL_INVARIANT_VIOLATION");
        }
      }

      const ins = await tx<{ prediction_id: string; requested_at: string }>`
        insert into pending_predictions (
          prediction_id, target_multiplier, probability, confidence,
          regime_name, regime_confidence, reasoning, feature_summary,
          model_version, requested_at, generated_at,
          target_game_id, source_round_id,
          correlation_id
        ) values (
          ${predictionId}, ${DEFAULT_TARGET}, ${signal.probability},
          ${signal.confidence}, ${signal.regimeId},
          ${signal.regimeId ? 0.5 : null},
          ${signal.reasoning}, ${JSON.stringify(signal.featureSummary)},
          ${signal.modelVersion}, ${timestamp}, ${timestamp},
          ${targetGameId}, ${gameId},
          ${correlationId}
        )
        on conflict (prediction_id) do nothing
        returning prediction_id, requested_at
      `;

      if (ins.length === 0) {
        const dup = await tx<{ prediction_id: string }>`
          select prediction_id from pending_predictions
          where target_game_id = ${targetGameId} and matched = false
          limit 1
        `;
        if (dup.length === 0) {
          throw new Error("PREDICTION_DUPLICATE_BUT_UNREADABLE");
        }
        return;
      }

      const effectiveSlaLagMs = recoveryMode ? SLA_LAG_MS * 2 : SLA_LAG_MS;
      const receivedMs = new Date(crashedAt).getTime();
      const slaLagMsActual = Date.now() - receivedMs;
      const slaViolated = slaLagMsActual > effectiveSlaLagMs;

      if (!slaViolated) {
        const regimeText = signal.regimeId ? ` (${signal.regimeId})` : "";
        const predictionContent = [
          `NEW PREDICTION${regimeText}`,
          "",
          `Target: ${Number(DEFAULT_TARGET).toFixed(2)}x`,
          `Probability: ${(signal.probability * 100).toFixed(1)}%`,
          `Confidence: ${(signal.confidence * 100).toFixed(1)}%`,
          "",
          `Prediction ID: ${predictionId}`,
          `Generated: ${timestamp}`,
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
              sourceGameId: gameId,
              targetMultiplier: Number(DEFAULT_TARGET),
              probability: signal.probability,
              confidence: signal.confidence,
              regimeName: signal.regimeId,
              slaViolated: false,
              kind: "prediction",
              recoveryMode,
            })},
            'pending', 2,
            0, Date.now()
          )
        `;
      }

      await tx`
        insert into live_event_log (
          correlation_id, event_kind, game_id, payload, received_at, processed_at,
          processor_latency_ms, sla_violated
        ) values (
          ${correlationId}::text, 'PREDICT', ${targetGameId},
          ${JSON.stringify({ sourceGameId: gameId, targetGameId, recoveryMode })},
          ${crashedAt}::timestamptz, now(),
          ${Math.max(0, Date.now() - new Date(crashedAt).getTime())}, ${slaViolated}
        )
      `;
    });

    logger.info(
      {
        component: "live-predictor",
        predictionId,
        targetGameId,
        sourceGameId: gameId,
        correlationId,
        recoveryMode,
        latencyMs: Date.now() - new Date(crashedAt).getTime(),
      },
      recoveryMode
        ? "recovery prediction generated and persisted"
        : "prediction generated and persisted for next round",
    );

    return {
      predictionId,
      targetGameId,
      kind: "predicted",
      sourceGameId: gameId,
      sourceCrashAt: crashedAt,
      predictionGeneratedAt: timestamp,
      predictionLatencyMs: Date.now() - new Date(crashedAt).getTime(),
      temporalValidity: "TEMPORALLY_VALID",
    };
  } catch (e) {
    logger.error(
      {
        component: "live-predictor",
        targetGameId,
        error: String(e),
        errorStack: e instanceof Error ? e.stack : undefined,
        sourceGameId: gameId,
        correlationId,
        recoveryMode,
      },
      "onGameEndPredict failed",
    );
    recordPredictionOutcome(true);
    try {
      const sqlErr = await getSqlFn();
      await sqlErr`
        insert into worker_state (key, value)
        values ('last_error', ${String(e)})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
    } catch {}
    return {
      predictionId: null,
      targetGameId,
      kind: "error",
      sourceGameId: gameId,
      sourceCrashAt: crashedAt,
      temporalValidity: "TEMPORALLY_INVALID",
    };
  }
}
