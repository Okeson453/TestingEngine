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
import { authoritativeNowMs } from "@/lib/prediction/live/clock-offset";
import { claimTarget, completeTarget, releaseTarget } from "@/lib/prediction/live/target-coordinator";
import type { Trace } from "@/lib/prediction/live/latency-trace";
import { getSql, getPgPool, type Sql } from "@/lib/db";
import { runInTransaction } from "@/lib/prediction/live/tx";
import { PredictionEngine } from "@/lib/prediction/prediction-engine";
import type { FeaturePath, HistoricalRound, ThresholdTarget } from "@/lib/prediction/types";
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
/** Require model P to beat fair odds (1/target) by this margin before emitting.
 *  Default 0.015 (~78.4% for 1.3x): filters pure base-rate spam without
 *  silencing the engine for hours. Set MIN_SIGNAL_EDGE=0 to emit every round.
 *  Prior default 0.04 needed ~81% which almost never fired with baseline P≈fair. */
const MIN_SIGNAL_EDGE = Number(process.env.MIN_SIGNAL_EDGE ?? 0);
const MIN_SIGNAL_PROBABILITY = Number(process.env.MIN_SIGNAL_PROBABILITY ?? 0);
const MIN_SIGNAL_CONFIDENCE = Number(process.env.MIN_SIGNAL_CONFIDENCE ?? 0);
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
export const MIN_REQUIRED_WINDOW_MS = Number(process.env.MIN_REQUIRED_WINDOW_MS ?? 150);
/** Stronger short-circuit: only abandon when the window is truly gone. */
export const SKIP_BELOW_MS = Number(process.env.SKIP_BELOW_MS ?? 80);
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
  | { kind: "error"; targetGameId: string; reason: string }
  | { kind: "sla_violated_no_outbox"; predictionId: string; targetGameId: string };

interface PredictorDeps {
  getSqlFn?: () => Promise<Sql>;
  /** Optional latency trace — marked at outbox enqueue on the ED hot path. */
  trace?: Trace | null;
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
    featurePath?: string;
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
export function getSharedPredictionEngine(): PredictionEngine {
  cachedEngine ??= new PredictionEngine();
  return cachedEngine;
}

const USE_ADVANCED_PIPELINE = process.env.USE_ADVANCED_PIPELINE === "1";

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
    } catch (e) {
      logger.warn(
        { component: "live-predictor", error: e instanceof Error ? e.message : String(e) },
        "advanced pipeline failed — explicit fallback to baseline PredictionEngine",
      );
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
    featurePath: signal.featurePath,
  };
};

async function loadPriorRoundsStrict(
  sql: Sql,
  beganAt: string,
  limit: number,
): Promise<HistoricalRound[]> {
  // Hot path: MEMORY ONLY by default. At 800ms–1s Neon RTT, a SQL history
  // fallback alone is a multi-hundred-ms regression. Buffer is warmed at boot
  // and appended on every completed ED/poll insert.
  try {
    const {
      getPriorRoundsSync,
      isLiveHistoryWarmed,
      warmLiveHistoryBuffer,
    } = await import("@/lib/prediction/live/live-history-buffer");
    if (!isLiveHistoryWarmed()) {
      await warmLiveHistoryBuffer(sql, Math.max(limit, 100));
    }
    const fromMem = getPriorRoundsSync(limit, undefined, beganAt);
    if (fromMem.length > 0) {
      return fromMem;
    }
  } catch {
    /* fall through only if forced */
  }

  // Escape hatch / empty buffer — costs 700–1000ms on Neon; must be rare.
  logger.warn(
    { component: "live-predictor", beganAt, limit },
    "HISTORY BUFFER MISS — SQL fallback (high latency on Neon)",
  );
  try {
    const { dbFallbackCount } = await import("@/lib/observability/performance/latency");
    dbFallbackCount.observe(1);
  } catch { /* soft */ }

  if (process.env.FORCE_HISTORY_SQL !== "0") {
    const t0 = performance.now();
    const rows = await sql<PriorRow>`
      select game_id, multiplier, began_at, crashed_at
      from crash_rounds
      where crashed_at < ${beganAt}::timestamptz
        and crashed_at is not null
      order by crashed_at desc, game_id desc
      limit ${limit}
    `;
    try {
      const { dbQueryMs } = await import("@/lib/observability/performance/latency");
      dbQueryMs.observe(performance.now() - t0);
    } catch { /* soft */ }
    return rows.reverse().map(mapRowToHistorical);
  }
  return [];
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

  // Boot-synced DB clock offset — no hot-path SELECT now().
  let refNow = authoritativeNowMs();
  try {
    const n = now();
    if (Number.isFinite(n)) {
      // Prefer deps.now when tests inject a clock; otherwise offset-aware wall.
      refNow = n === Date.now() ? authoritativeNowMs() : n;
    }
  } catch {
    /* soft */
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
  let signal: ReturnType<NonNullable<PredictorDeps["predictFn"]>>;
  try {
    signal = predictFn(priorRounds, evt.gameId, timestamp, DEFAULT_TARGET);
  } catch (e) {
    const stage = (e as { stage?: string }).stage ?? "prediction";
    const errObj = e as Record<string, unknown>;
    const failure: Record<string, unknown> = {
      component: "live-predictor",
      stage,
      sourceRoundId: evt.sourceRoundGameId,
      targetRoundId: evt.gameId,
      predictionType: `bg:${DEFAULT_TARGET}x`,
      failureReason: (errObj.failureReason as string | undefined) ?? String(e),
      errorName: e instanceof Error ? e.name : "Error",
      errorMessage: e instanceof Error ? e.message : String(e),
      correlationId,
    };
    if (errObj.predictionResult !== undefined) failure.predictionResult = errObj.predictionResult;
    else if (errObj.prediction !== undefined) failure.predictionResult = errObj.prediction;
    for (const k of ["invalidField", "expectedType", "actualType"] as const) {
      if (errObj[k] !== undefined && errObj[k] !== null) failure[k] = errObj[k];
    }
    logger.error(failure, `bg prediction attempt failed at stage=${stage}`);
    return {
      kind: "error",
      targetGameId: evt.gameId,
      reason: String(e),
    };
  }

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
        // P1.6: Populate telegram_deadline_at for onGameStart path too.
        const deadlineAt = new Date(Date.now() + Number(process.env.TELEGRAM_DEADLINE_MS ?? 8_000)).toISOString();
        await tx`
          insert into notification_outbox (
            notification_id, type, content, metadata, status, priority,
            attempt_count, next_attempt_at, telegram_deadline_at
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
            'pending', 3,
            0, now(), ${deadlineAt}::timestamptz
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
      "predictor.onGameStart soft-fail (often temporal under live bg)",
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

  // Wake dispatcher immediately after TX commit (no setImmediate boundary).
  if (outboxEnqueued > 0 && !slaViolated) {
    try {
      const { notifyOutbox } = await import("@/lib/prediction/live/outbox-wake");
      notifyOutbox();
    } catch {
      /* soft */
    }
  }

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
    | "skipped_no_edge"
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
  const predictFn = deps.predictFn ?? defaultPredictFn;
  const trace = deps.trace ?? null;

  // ── Latency instrumentation: stage-level timing ──
  const t0 = performance.now(); // WS event received (caller already decoded)

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

  // ── P0: In-memory target claim (ZERO DB) ──
  const owner = deps.recoveryMode ? `poll:${gameId}` : `ed:${gameId}`;
  const claim = claimTarget(targetGameId, owner);
  const t1 = performance.now(); // target claimed

  if (!claim.owned) {
    return {
      predictionId: null,
      targetGameId,
      kind: "duplicate",
      sourceGameId: gameId,
      sourceCrashAt: crashedAt,
    };
  }

  const generatedAt = new Date().toISOString();
  const recoveryMode = deps.recoveryMode === true;

  // ── P0: Update in-memory history buffer (ZERO DB) ──
  // The edHandler in game-event-handlers.ts already calls appendCompletedRound
  // before this function, but we keep it here as a belt-and-suspenders measure
  // for the poll-worker path which calls onGameEndPredict directly.
  try {
    const { appendCompletedRound } = await import(
      "@/lib/prediction/live/live-history-buffer"
    );
    appendCompletedRound({
      gameId,
      multiplier,
      crashedAt,
    });
  } catch {
    /* soft — buffer is best-effort */
  }

  // ── P0: History MUST come from memory. NEVER call getSql() here. ──
  // No SQL fallback allowed on the ED prediction path. If the buffer is cold,
  // boot should have warmed it. SQL is only for boot/recovery/cold-start.
  let priorRounds: HistoricalRound[] = [];
  try {
    const {
      getPriorRoundsSync,
      isLiveHistoryWarmed,
    } = await import("@/lib/prediction/live/live-history-buffer");

    if (!isLiveHistoryWarmed()) {
      logger.warn(
        { targetGameId, sourceGameId: gameId },
        "realtime prediction blocked — live history not ready (boot should warm before first ED)",
      );
    }

    priorRounds = getPriorRoundsSync(MAX_HISTORY, gameId, crashedAt);
  } catch {
    /* soft — priorRounds stays empty */
  }

  const t2 = performance.now(); // history loaded from memory

  if (priorRounds.length < MIN_HISTORY) {
    logger.warn(
      {
        targetGameId,
        sourceGameId: gameId,
        historySize: priorRounds.length,
        minHistory: MIN_HISTORY,
      },
      "realtime prediction blocked — insufficient warmed history",
    );
    try { releaseTarget(targetGameId, owner); } catch { /* soft */ }
    recordPredictionOutcome(true);
    return {
      predictionId: null,
      targetGameId,
      kind: "insufficient_history",
      temporalValidity: "TEMPORALLY_UNVERIFIED",
      sourceGameId: gameId,
      sourceCrashAt: crashedAt,
    };
  }

  // ── P0: Prediction computation only (ZERO DB, ZERO Telegram, ZERO outbox) ──
  const timestamp = generatedAt;
  const predictT0 = performance.now();
  let signal: ReturnType<NonNullable<PredictorDeps["predictFn"]>>;
  try {
    signal = predictFn(priorRounds, targetGameId, timestamp, DEFAULT_TARGET);
  } catch (e) {
    // Structured N+1 failure diagnostics. The error carries its own stage
    // from the engine (prediction_output_validation / signal_conversion /
    // model_prediction / ...); we add the source/target round context here.
    const stage = (e as { stage?: string }).stage ?? "prediction";
    const errObj = e as Record<string, unknown>;
    const failure: Record<string, unknown> = {
      component: "live-predictor",
      stage,
      sourceRoundId: gameId,
      targetRoundId: targetGameId,
      predictionType: `N+1:${DEFAULT_TARGET}x`,
      failureReason: (errObj.failureReason as string | undefined) ?? String(e),
      errorName: e instanceof Error ? e.name : "Error",
      errorMessage: e instanceof Error ? e.message : String(e),
      correlationId,
      recoveryMode,
    };
    // Bounded prediction summary — never dump the raw model object.
    if (errObj.predictionResult !== undefined) {
      failure.predictionResult = errObj.predictionResult;
    } else if (errObj.prediction !== undefined) {
      failure.predictionResult = errObj.prediction;
    }
    for (const k of ["invalidField", "expectedType", "actualType"] as const) {
      if (errObj[k] !== undefined && errObj[k] !== null) failure[k] = errObj[k];
    }
    logger.error(failure, `N+1 prediction attempt failed at stage=${stage}`);
    // The in-memory claim MUST be released so a later ED/recovery attempt
    // for this target is not permanently blocked as a phantom "duplicate".
    try { releaseTarget(targetGameId, owner); } catch { /* soft */ }
    recordPredictionOutcome(true);
    return {
      predictionId: null,
      targetGameId,
      kind: "error",
      temporalValidity: "TEMPORALLY_UNVERIFIED",
      sourceGameId: gameId,
      sourceCrashAt: crashedAt,
    };
  }
  const predictElapsed = performance.now() - predictT0;
  const t3 = performance.now(); // prediction completed

  if (predictElapsed > PREDICT_TIMEOUT_MS) {
    logger.warn(
      { targetGameId, predictElapsedMs: predictElapsed, budgetMs: PREDICT_TIMEOUT_MS },
      "prediction exceeded PREDICT_TIMEOUT_MS budget — consider offloading to worker thread",
    );
  }
  try {
    const { predictionGenerationMs } = await import(
      "@/lib/observability/performance/latency"
    );
    predictionGenerationMs.observe(predictElapsed);
  } catch { /* metrics optional */ }

  const predictionId = signal.predictionId;

  // Compute SLA status (in-memory, no DB clock access)
  const effectiveSlaLagMs = recoveryMode ? SLA_LAG_MS * 2 : SLA_LAG_MS;
  const receivedMs = new Date(crashedAt).getTime();
  const slaLagMsActual = Date.now() - receivedMs;
  const slaViolated =
    Number.isFinite(slaLagMsActual) && slaLagMsActual > effectiveSlaLagMs;

  // Selectivity gate: only persist/notify when there is edge vs fair odds.
  {
    const targetNum = Number(DEFAULT_TARGET);
    const fair = targetNum > 1 ? 1 / targetNum : 0.5;
    const needP = Math.max(MIN_SIGNAL_PROBABILITY, fair + MIN_SIGNAL_EDGE);
    const p = signal.probability;
    const c = signal.confidence;
    if (
      (Number.isFinite(MIN_SIGNAL_EDGE) && MIN_SIGNAL_EDGE > 0 && p < needP) ||
      (MIN_SIGNAL_PROBABILITY > 0 && p < MIN_SIGNAL_PROBABILITY) ||
      (MIN_SIGNAL_CONFIDENCE > 0 && c < MIN_SIGNAL_CONFIDENCE)
    ) {
      logger.info(
        {
          component: "live-predictor",
          targetGameId,
          sourceGameId: gameId,
          probability: p,
          confidence: c,
          fair,
          needP,
          minEdge: MIN_SIGNAL_EDGE,
          recoveryMode,
        },
        "skip signal — no edge vs fair odds (not every round should fire)",
      );
      try { releaseTarget(targetGameId, owner); } catch { /* soft */ }
      return {
        predictionId: null,
        targetGameId,
        kind: "skipped_no_edge",
        temporalValidity: "TEMPORALLY_VALID",
        sourceGameId: gameId,
        sourceCrashAt: crashedAt,
      };
    }
  }

  // ── P0: SIGNAL READY ──
  // The prediction signal is complete. Everything below is async persistence
  // that must NOT block the signal path. We return immediately after logging.
  const t4 = performance.now(); // signal ready

  logger.info(
    {
      component: "live-predictor",
      predictionId,
      targetGameId,
      sourceGameId: gameId,
      correlationId,
      recoveryMode,
      // Precise stage-level latency instrumentation
      claimMs: Number((t1 - t0).toFixed(2)),
      historyMs: Number((t2 - t1).toFixed(2)),
      predictionMs: Number((t3 - t2).toFixed(2)),
      predictionToSignalMs: Number((t4 - t3).toFixed(2)),
      totalMs: Number((t4 - t0).toFixed(2)),
      sourceAgeMs: Math.max(0, Date.now() - new Date(crashedAt).getTime()),
      signalReady: true,
    },
    "PREDICTION_READY — model computation finished; durable handoff async",
  );

  // ── P1/P2: EVERYTHING BELOW IS NON-BLOCKING ──
  // pending_predictions, notification_outbox, and live_event_log are all
  // written asynchronously. The DB remains the durability/idempotency
  // backstop via ON CONFLICT DO NOTHING. If async persistence fails, the
  // PREDICTION_READY was returned; DB durability still required for outbox/Telegram.
  // The poll worker and validator will reconcile any missing DB state.

  const persistPromise = (async () => {
    const getSqlFn = deps.getSqlFn ?? getSql;
    let sql: Sql;
    try {
      sql = await getSqlFn();
    } catch (e) {
      logger.error(
        { component: "live-predictor", targetGameId, error: String(e) },
        "async persistence: getSql failed — PREDICTION_READY returned but durable handoff pending",
      );
      return;
    }

    try {
      await runInTransaction(sql, async (tx) => {
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
          // Duplicate — already persisted by another path (DB is the backstop)
          return;
        }

        // Enqueue prediction Telegram signal
        {
          const regimeText = signal.regimeId ? ` (${signal.regimeId})` : "";
          const lateTag = slaViolated ? " (delayed)" : "";
          const predictionContent = [
            `NEW PREDICTION${regimeText}${lateTag}`,
            "",
            `Target: ${Number(DEFAULT_TARGET).toFixed(2)}x`,
            `Probability: ${(signal.probability * 100).toFixed(1)}%`,
            `Confidence: ${(signal.confidence * 100).toFixed(1)}%`,
            "",
            `Prediction ID: ${predictionId}`,
            `Generated: ${timestamp}`,
            recoveryMode ? "Source: poll recovery" : "Source: live ED",
          ].join("\n");
          const deadlineMs = recoveryMode
            ? Number(process.env.TELEGRAM_DEADLINE_RECOVERY_MS ?? 12_000)
            : Number(process.env.TELEGRAM_DEADLINE_MS ?? 8_000);
          const deadlineAt = new Date(Date.now() + deadlineMs).toISOString();
          try {
            await tx`
              insert into notification_outbox (
                notification_id, type, content, metadata, status, priority,
                attempt_count, next_attempt_at, telegram_deadline_at
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
                  slaViolated,
                  slaLagMsActual,
                  kind: "prediction",
                  recoveryMode,
                })},
                'pending', 3,
                0, now(), ${deadlineAt}::timestamptz
              )
            `;
          } catch (err) {
            // Tag so the outer catch distinguishes outbox_enqueue from
            // pending_predictions persistence failures.
            (err as { stage?: string }).stage = "outbox_enqueue";
            throw err;
          }
        }
      });

      // live_event_log outside TX (not required for correctness)
      void sql`
        insert into live_event_log (
          correlation_id, event_kind, game_id, payload, received_at, processed_at,
          processor_latency_ms, sla_violated
        ) values (
          ${correlationId}::text, 'PREDICT', ${targetGameId},
          ${JSON.stringify({ sourceGameId: gameId, targetGameId, recoveryMode })},
          ${crashedAt}::timestamptz, now(),
          ${Math.max(0, Date.now() - new Date(crashedAt).getTime())}, ${slaViolated}
        )
      `.catch(() => undefined);

      // Wake outbox dispatcher after TX commit
      try {
        const { notifyOutbox } = await import("@/lib/prediction/live/outbox-wake");
        notifyOutbox();
      } catch { /* soft */ }
      // P1 latency chain: outbox row is durably enqueued at this point.
      try {
        const { mark } = await import("@/lib/prediction/live/latency-trace");
        if (trace) mark(trace, "outbox_enqueued");
      } catch { /* soft */ }

      try { completeTarget(targetGameId, owner); } catch { /* soft */ }

      logger.info(
        {
          component: "live-predictor",
          predictionId,
          targetGameId,
          correlationId,
          persistenceMs: Number((performance.now() - t4).toFixed(2)),
        },
        "async persistence complete",
      );
    } catch (e) {
      const stage = (e as { stage?: string }).stage ?? "persistence";
      logger.error(
        {
          component: "live-predictor",
          stage,
          targetGameId,
          sourceRoundId: gameId,
          correlationId,
          predictionId,
          failureReason: String(e),
          errorName: e instanceof Error ? e.name : "Error",
          errorMessage: e instanceof Error ? e.message : String(e),
        },
        stage === "outbox_enqueue"
          ? "outbox enqueue failed — PREDICTION_READY returned but Telegram handoff failed"
          : "async prediction persistence failed — PREDICTION_READY returned but durable handoff failed",
      );
      try { releaseTarget(targetGameId, owner); } catch { /* soft */ }
    }
  })();

  // Don't await — return the signal immediately.
  // Keep the promise alive so it doesn't become an unhandled rejection.
  void persistPromise.catch(() => undefined);

  // P0 (identity): register the immutable prediction record keyed by target
  // round. Feedback resolves against THIS record — never "last emitted".
  try {
    const { globalPredictionRegistry } = await import(
      "@/lib/prediction/identity/prediction-registry"
    );
    const fs = signal.featureSummary as Record<string, unknown> | null | undefined;
    const featureVersionOf = (signal as { featureVersion?: string | null }).featureVersion ?? null;
    globalPredictionRegistry.register({
      predictionId,
      sourceRoundId: gameId,
      targetRoundId: targetGameId,
      createdAt: timestamp,
      targetStartedAt: null,
      targetEndedAt: null,
      rawProbability: signal.probability,
      calibratedProbability: null,
      pipelineProbability: null,
      finalProbability: signal.probability,
      confidence: signal.confidence,
      target: Number(DEFAULT_TARGET),
      regime: signal.regimeId ?? null,
      modelVersion: signal.modelVersion,
      featureVersion: featureVersionOf,
      featurePath: (signal.featurePath as FeaturePath | undefined) ?? null,
      temporalValidity: "TEMPORALLY_UNVERIFIED",
      provenance: {
        stateVersion: (fs?.stateVersion as string | number | undefined) ?? null,
        acieStateVersion: (fs?.stateVersion as string | number | undefined) ?? null,
        calibrationVersion: (fs?.calibrationVersion as string | number | undefined) ?? null,
        regimeVersion: (fs?.regimeVersion as string | number | undefined) ?? null,
        pipelineVersion: null,
      },
      stages: {
        acieSignal: true,
        calibrationApplied: null,
        pipelineApplied: null,
        finalSignal: true,
      },
      resolved: false,
    });
  } catch { /* soft — registry is best-effort */ }

  return {
    predictionId,
    targetGameId,
    kind: "predicted",
    temporalValidity: "TEMPORALLY_VALID",
    sourceGameId: gameId,
    sourceCrashAt: crashedAt,
    targetStartedAt: null,
    predictionGeneratedAt: timestamp,
    predictionLatencyMs: Math.round(t4 - t0),
    availableWindowMs: null,
    remainingBeforeTargetMs: null,
  };
}