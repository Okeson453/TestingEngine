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
import { getMedianBettingWindowMs, isTargetPastBettingWindow } from "@/lib/prediction/live/live-round-registry";
import { getEffectiveSkipBelowMs } from "@/lib/prediction/live/gate-cache";
import { claimTarget, completeTarget, releaseTarget } from "@/lib/prediction/live/target-coordinator";
import type { Trace } from "@/lib/prediction/live/latency-trace";
import { predictionLifecycleCounters } from "@/lib/prediction/live/latency-trace";
import { getSql, getCriticalSql, getPgPool, getLastPoolAcquireMs, type Sql } from "@/lib/db";
import { runInTransaction, type TxStageTimings } from "@/lib/prediction/live/tx";
import { PredictionEngine } from "@/lib/prediction/prediction-engine";
import type { FeaturePath, HistoricalRound, ThresholdTarget } from "@/lib/prediction/types";
import { getConfiguredChatIds } from "@/lib/notifications/telegram";
import { getLogger } from "@/lib/observability/logger";
// P0 FIX: these were previously loaded via require() inside try/catch. The
// production worker is pure ESM (node --experimental-strip-types +
// scripts/paths-loader.mjs) where `require` does not exist, so every call
// threw ReferenceError and was silently caught — the ACIE authoritative path
// and the advanced pipeline NEVER ran (every prediction fell back to
// FALLBACK_BASELINE). Static ESM imports work in both the Vite dev server
// and the standalone ESM worker.
import { getSharedACIEEngine, getSharedACIEInstanceId } from "@/lib/prediction/acie/shared-engine";
import { buildAcieFeatureFingerprint, buildProvenance, computeFeatureHash } from "@/lib/prediction/acie/provenance";
import { recordAcieObservation, assertFreshAcieState, getLastAcieObservation } from "@/lib/prediction/acie/stale-guard";
import { runPredictionPipeline } from "@/lib/prediction/prediction-pipeline";

import {
  evaluateSheath,
  recordPredictionOutcome,
} from "@/lib/core/sheath-mode";

const logger = getLogger("live-predictor");
// SYNTAX_GUARD_20260906: file must parse under node --experimental-strip-types

/**
 * Observe crash N on shared ACIE — IDEMPOTENT, ownership-independent.
 *
 * SEP 11 FIX (production 14:28:18 STALE_REJECTED): ACIE observation was
 * inline in onGameEndPredict AFTER the target claim, so when BG(N) had
 * already claimed/decided target N+1 (duplicate or terminal NO_BET), the
 * ED(N) attempt returned early and round N was NEVER observed. The next
 * BG(N+1) evaluation then proved stale (ACIE last observed N-1, buffer
 * tail N) and was rejected — the BG-primary path starved its own model.
 *
 * Learning is not ownership: every authoritative crash is observed exactly
 * once per process, regardless of who owns the prediction. The
 * last-observation guard makes double calls (edHandler direct + inside
 * onGameEndPredict on the owned path) a no-op.
 */
export function observeCrashForACIE(
  gameId: string,
  multiplier: number,
  crashedAt: string,
): void {
  const prev = getLastAcieObservation();
  if (prev.gameId === String(gameId) && prev.observationCount > 0) return;
  try {
    const acie = getSharedACIEEngine();
    const learnResult = acie.observeRound({
      roundId: gameId,
      crashPoint: multiplier,
      timestamp: crashedAt,
    });
    const obsCount =
      learnResult?.online?.observationCount ?? acie.historySize();
    try {
      recordAcieObservation(gameId, obsCount);
    } catch {
      /* soft */
    }
    // Forensic provenance: prove the observation advanced the state.
    // state_version is the monotonic state version (== observationCount);
    // history_hash fingerprints the observed crash history so a repeat
    // lineup complaint can be answered from the log alone.
    let historyHash: string | null = null;
    try {
      historyHash = computeFeatureHash({
        crashPointsTail: acie.exportSnapshot().crashPoints,
      });
    } catch {
      /* forensic metadata only */
    }
    logger.info(
      {
        component: "live-predictor",
        event: "ACIE_OBSERVATION",
        sourceGameId: gameId,
        multiplier,
        observation_count: obsCount,
        state_version: obsCount,
        acieInstanceId: getSharedACIEInstanceId(),
        history_hash: historyHash,
      },
      "ACIE observed crash before N+1 evaluation",
    );
  } catch (e) {
    logger.warn(
      { component: "live-predictor", sourceGameId: gameId, error: String(e) },
      "ACIE observeRound failed on hot path — prediction may use stale/fallback path",
    );
  }
}

/** Prediction-related constants. */
const DEFAULT_TARGET: ThresholdTarget = 1.3;
/** Require model P to beat fair odds (1/target) by this margin before emitting.
 *  Default 0.015 (~78.4% for 1.3x): filters pure base-rate spam without
 *  silencing the engine for hours. Set MIN_SIGNAL_EDGE=0 to emit every round.
 *  Prior default 0.04 needed ~81% which almost never fired with baseline P≈fair.
 *  NOTE: defaults were briefly 0 which disabled the live "NO BET" path entirely;
 *  restored 0.015 so production can actually skip weak rounds. */
export const MIN_SIGNAL_EDGE = Number(process.env.MIN_SIGNAL_EDGE ?? 0.015);
export const MIN_SIGNAL_PROBABILITY = Number(process.env.MIN_SIGNAL_PROBABILITY ?? 0);
export const MIN_SIGNAL_CONFIDENCE = Number(process.env.MIN_SIGNAL_CONFIDENCE ?? 0);

/** Pure selectivity decision used by onGameEndPredict (and unit tests).
 *  Returns true when this evaluation must NOT become a delivered signal. */
export function shouldSkipSignal(input: {
  probability: number;
  confidence: number;
  target?: number;
  strategyAction?: string | null;
  pipelineAction?: string | null;
  reasoning?: string[] | string | null;
  minEdge?: number;
  minProbability?: number;
  minConfidence?: number;
}): boolean {
  const targetNum = Number(input.target ?? 1.3);
  const fair = targetNum > 1 ? 1 / targetNum : 0.5;
  const minEdge = input.minEdge ?? MIN_SIGNAL_EDGE;
  const minP = input.minProbability ?? MIN_SIGNAL_PROBABILITY;
  const minC = input.minConfidence ?? MIN_SIGNAL_CONFIDENCE;
  const needP = Math.max(minP, fair + minEdge);
  const p = input.probability;
  const c = input.confidence;
  const strategyAction = String(input.strategyAction ?? "").toUpperCase();
  const pipelineAction = String(input.pipelineAction ?? "").toUpperCase();
  const reasoningJoined = Array.isArray(input.reasoning)
    ? input.reasoning.join(" ")
    : String(input.reasoning ?? "");
  const reasoningSaysSkip =
    /\baction=SKIP\b/i.test(reasoningJoined) ||
    /\bpipeline_action=SKIP\b/i.test(reasoningJoined);
  const noEdge =
    (Number.isFinite(minEdge) && minEdge > 0 && p < needP) ||
    (minP > 0 && p < minP) ||
    (minC > 0 && c < minC);
  const strategySkip =
    strategyAction === "SKIP" || pipelineAction === "SKIP" || reasoningSaysSkip;
  return noEdge || strategySkip;
}
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
  /**
   * ISO instant the authoritative live ED(N) event entered the worker.
   * Anchors the delivery timeline at true receipt (ed_received_at) so the
   * full ED→Telegram latency is measurable end-to-end from outbox metadata
   * alone. Undefined on poll-recovery attempts (there is no ED event).
   */
  edReceivedAt?: string;
  /**
   * BG-PRIMARY trigger (sep 11 architecture change): the source round N has
   * STARTED (BG(N) received) — it has NOT crashed yet. Round N's multiplier
   * is unknown and must NOT be appended to history or observed on ACIE;
   * features/history run through N-1 only. Target is still sourceRoundId+1
   * and ALL gates apply unchanged.
   */
  bgTrigger?: boolean;
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
  // P0 FIX: was require()-based (ReferenceError under ESM → silent null).
  // runPredictionPipeline is now a static import above.
  try {
    cachedPipelineFn = runPredictionPipeline as PipelineFn;
  } catch {
    cachedPipelineFn = null;
  }
  return cachedPipelineFn;
}

/**
 * P0 authoritative path: observe is done by the caller (onGameEndPredict)
 * on the shared ACIE; this function evaluates N+1 from that shared state.
 * Falls back to PredictionEngine only when ACIE is unavailable.
 */
const defaultPredictFn = (
  priorRounds: HistoricalRound[],
  targetRoundId: string,
  timestamp: string,
  target: ThresholdTarget,
) => {
  // Prefer ACIE evaluation from the shared singleton (post-observe).
  try {
    // P0 FIX: was require() — ReferenceError under the ESM worker made this
    // fall back to PredictionEngine on every crash. Static import now.
    const sharedMod = {
      getSharedACIEEngine,
      getSharedACIEInstanceId,
    };
    // P0 FIX: was require() — ReferenceError under ESM. Static import now.
    const provMod = {
      buildAcieFeatureFingerprint,
      buildProvenance,
    };
    const acie = sharedMod.getSharedACIEEngine();
    if (acie.historySize() >= 5) {
      const evaluation = acie.evaluateNext();
      const online = acie.getOnlineState();
      const snap = acie.exportSnapshot();
      const probability = Math.min(
        0.99,
        Math.max(0.01, evaluation.psi.estimatedProbability),
      );
      const confidence = Math.max(
        0,
        Math.min(1, 1 - (evaluation.psi.modelUncertainty ?? 0.3)),
      );
      const featureHash = provMod.buildAcieFeatureFingerprint({
        crashPointsTail: snap.crashPoints,
        observationCount: online.observationCount ?? 0,
        regime: String(evaluation.regime ?? "unknown"),
        ewmaHitRate: online.ewmaHitRate ?? 0,
        psiProbability: probability,
      });
      const provenance = provMod.buildProvenance({
        sourceGameId: String(
          (priorRounds[priorRounds.length - 1] as { externalRoundId?: string } | undefined)
            ?.externalRoundId ?? "",
        ),
        targetGameId: targetRoundId,
        online,
        evaluation,
        mode: "NORMAL_ACIE",
        executionPath: "shared-acie.evaluateNext",
        probability,
        confidence,
        featureHash,
        modelName: "acie-psi",
        modelVersion: "acie-v3",
      });
      const predictionId = randomUUID();
      logger.info(
        {
          component: "live-predictor",
          event: "PREDICTION_GENERATION",
          predictionId,
          targetGameId: targetRoundId,
          ...provenance,
        },
        "ACIE authoritative prediction generated",
      );
      const strategyAction = evaluation.strategy?.action ?? null;
      return {
        predictionId,
        probability,
        confidence,
        regimeId: String(evaluation.regime ?? null),
        reasoning: [
          evaluation.strategy?.reason ?? "acie",
          `action=${strategyAction}`,
          `evidence=${evaluation.evidence?.status}`,
          `obs=${online.observationCount ?? 0}`,
          `feature_hash=${featureHash}`,
        ],
        featureSummary: {
          ...(typeof provenance === "object" ? provenance : {}),
          acieAuthoritative: true,
          strategy_action: strategyAction,
        },
        modelVersion: "acie-v3",
        featurePath: "ACIE_STATE",
      };
    }
  } catch (e) {
    logger.warn(
      {
        component: "live-predictor",
        error: e instanceof Error ? e.message : String(e),
      },
      "ACIE evaluateNext unavailable — falling back to PredictionEngine",
    );
  }

  // FALLBACK_BASELINE path (must be visible)
  const engine = getSharedPredictionEngine();
  const signal = engine.predict({
    priorRounds,
    targetRoundId,
    timestamp,
    target,
  });

  let probability = signal.probability;
  const confidence = signal.confidence;
  let modelVersion = signal.modelVersion ?? "live-v2";
  let executionMode = "FALLBACK_BASELINE";
  const reasoning: string[] = Array.isArray(signal.reasoning)
    ? [...signal.reasoning]
    : signal.reasoning
      ? [String(signal.reasoning)]
      : [];
  reasoning.push("execution_mode=FALLBACK_BASELINE");

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
      // Do NOT floor confidence to probability — confidence must reflect model
      // uncertainty, not be forced upward by the calibrated probability.
      modelVersion = `${modelVersion}+pipeline`;
      executionMode = "ADVANCED_ACIE";
      reasoning.push(
        `pipeline_action=${pipe.action}`,
        `pipeline_reason=${pipe.reason}`,
        `threshold=${pipe.threshold}`,
        "execution_mode=ADVANCED_ACIE",
      );
      // Stash for the selectivity gate (SKIP must become a real no-signal path)
      (signal as { featureSummary?: Record<string, unknown> }).featureSummary = {
        ...((signal.featureSummary as Record<string, unknown> | undefined) ?? {}),
        pipeline_action: pipe.action,
        pipeline_reason: pipe.reason,
      };
    } catch (e) {
      logger.warn(
        { component: "live-predictor", error: e instanceof Error ? e.message : String(e) },
        "advanced pipeline failed — explicit fallback to baseline PredictionEngine",
      );
    }
  }

  // Forensic provenance for the fallback path: record which shared-ACIE
  // state existed when the fallback fired, so a FALLBACK_BASELINE row can
  // never be mistaken for stale ACIE output. Metadata only — never throws.
  let fallbackAcie: {
    acie_observation_count: number | null;
    acie_state_version: number | null;
    acie_instance_id: string | null;
  } = {
    acie_observation_count: null,
    acie_state_version: null,
    acie_instance_id: null,
  };
  try {
    const acie = getSharedACIEEngine();
    const obsCount = acie.getOnlineState().observationCount ?? acie.historySize();
    fallbackAcie = {
      acie_observation_count: obsCount,
      acie_state_version: obsCount,
      acie_instance_id: getSharedACIEInstanceId(),
    };
  } catch {
    /* forensic metadata only */
  }

  return {
    predictionId: signal.predictionId,
    probability,
    confidence,
    regimeId: signal.regimeId,
    reasoning,
    featureSummary: {
      ...(signal.featureSummary as Record<string, unknown> | undefined),
      ...fallbackAcie,
      prediction_mode: executionMode,
      execution_path: "PredictionEngine.predict",
      acieAuthoritative: false,
    },
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

  // Escape hatch / empty buffer — costs 700–1000ms on Neon.
  // P0: do NOT silently insert a 1s SQL query into the N+1 hot path.
  // Prefer explicit unavailability over destroying the inter-round budget.
  // SQL fallback is only allowed when FORCE_HISTORY_SQL=1 (tests / recovery).
  logger.warn(
    { component: "live-predictor", beganAt, limit },
    "HISTORY BUFFER MISS — refusing SQL fallback on hot path (N+1_UNAVAILABLE_HISTORY)",
  );
  try {
    const { dbFallbackCount } = await import("@/lib/observability/performance/latency");
    dbFallbackCount.observe(1);
  } catch { /* soft */ }

  if (process.env.FORCE_HISTORY_SQL === "1") {
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
  // P0: reserved critical pool — never queue behind dashboard/general work
  const getSqlFn = deps.getSqlFn ?? getCriticalSql;
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
        // Hard temporal contract (report #12-15): a prediction for a round
        // that has ALREADY STARTED (this path fires ON BG — the round is
        // live) must never become a Telegram signal. The prediction row
        // above still persists — learning/calibration/feedback use it and
        // the registry marks it TEMPORALLY_INVALID — but no signal intent
        // is created. The old behavior enqueued it with an 8s
        // creation-relative deadline: a false-timing signal by definition.
        logger.info(
          {
            component: "live-predictor",
            predictionId,
            targetGameId: evt.gameId,
            correlationId,
            expiration_reason: "target_already_started_at_prediction_time",
          },
          "SIGNAL_NOT_ENQUEUED: target started before prediction — learning only",
      );
      outboxEnqueued = 0;
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
    } catch {
      /* worker_state best-effort - the temporal violation is reported regardless */
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

  // Wake dispatcher immediately after TX commit (no setImmediate boundary).
  // PREDICTION wake (plan §11): the dispatcher's prediction lane runs at once.
  if (outboxEnqueued > 0 && !slaViolated) {
    try {
      const { notifyOutbox } = await import("@/lib/prediction/live/outbox-wake");
      notifyOutbox("prediction");
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
    | "skipped_insufficient_window"
    | "skipped_target_started"
    | "skipped_no_edge"
    | "temporally_invalid"
    | "persist_failed";
  temporalValidity?: TemporalValidity;
  sourceGameId?: string;
  sourceCrashAt?: string;
  targetStartedAt?: string | null;
  predictionGeneratedAt?: string;
  predictionLatencyMs?: number;
  availableWindowMs?: number | null;
  remainingBeforeTargetMs?: number | null;
  /** 1 when prediction outbox row was durably enqueued before return. */
  outboxEnqueued?: number;
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

  // ── P0: Priority-ordered target claim (ZERO DB hot path) ──
  // Priority: BG > ED > RECOVERY. BG reserves at receipt (before reconcile);
  // claimTarget here promotes RESERVED_BG → BG_RUNNING or blocks ED when BG
  // already owns. pending_predictions unique constraint remains the durability
  // backstop for multi-process safety.
  const owner = deps.recoveryMode
    ? `poll:${gameId}`
    : deps.bgTrigger
      ? `bg:${gameId}`
      : `ed:${gameId}`;
  const claim = claimTarget(targetGameId, owner);
  const t1 = performance.now(); // target claimed

  if (!claim.owned) {
    const blockedKind =
      claim.reason === "bg_reserved" ||
      claim.reason === "bg_running" ||
      claim.reason === "bg_owned" ||
      claim.blockedByBg
        ? `blocked_by_bg:${claim.reason}`
        : claim.reason === "no_bet_terminal"
          ? "duplicate_no_bet"
          : "duplicate";
    logger.info(
      {
        component: "live-predictor",
        sourceGameId: gameId,
        targetGameId,
        owner,
        claimReason: claim.reason,
        claimOwner: claim.owner,
        claimState: claim.state,
        blockedByBg: !!claim.blockedByBg,
        noBet: !!claim.noBet,
      },
      claim.blockedByBg
        ? `N+1 ownership blocked by BG (${claim.reason})`
        : `N+1 ownership not acquired (${claim.reason})`,
    );
    return {
      predictionId: null,
      targetGameId,
      kind: blockedKind,
      sourceGameId: gameId,
      sourceCrashAt: crashedAt,
    };
  }

  // ── TARGET-ANCHORED SCHEDULING (sep 11 advisor P0.2/P0.3) ──
  // The only deadline that matters is the START of round N+1. Predict it from
  // the in-process betting-window median (bg(N) − ed(N−1), measured by the
  // live-round registry — NOT the crash-to-crash gap, which includes run
  // time). Enforce the previously-dead window guards here: a prediction with
  // no remaining window is refused AT GENERATION instead of being persisted,
  // enqueued, and delivered into a round that already began.
  //
  // RECOVERY GUARD (sep 11 advisor DEF-4): the registry knows synchronously —
  // written at ED/BG handler entry, before any await — when the target round
  // has already started or crashed. Recovery paths deliberately FORCE
  // attempts for targets that are already live ("target live but no pending
  // prediction — forcing recovery attempt"); a signal for such a target is
  // late by construction. Refuse here, never persist-then-kill at dispatch.
  if (isTargetPastBettingWindow(targetGameId)) {
    completeTarget(targetGameId, owner);
    logger.warn(
      {
        component: "live-predictor",
        sourceGameId: gameId,
        targetGameId,
        recoveryMode: !!deps.recoveryMode,
      },
      "skipping prediction: target already started/ended (registry)",
    );
    return {
      predictionId: null,
      targetGameId,
      kind: "skipped_target_started",
      sourceGameId: gameId,
      sourceCrashAt: crashedAt,
      outboxEnqueued: 0,
    };
  }
  const crashedAtMs = new Date(crashedAt).getTime();
  const bettingWindowMs = getMedianBettingWindowMs();
  const predictedStartMs = (Number.isFinite(crashedAtMs) ? crashedAtMs : Date.now()) + bettingWindowMs;
  const remainingBeforeTargetMs = predictedStartMs - Date.now();
  const minWindowMs = Math.max(
    MIN_REQUIRED_WINDOW_MS,
    getEffectiveSkipBelowMs() ?? SKIP_BELOW_MS,
  );
  if (remainingBeforeTargetMs < minWindowMs) {
    completeTarget(targetGameId, owner);
    logger.warn(
      {
        component: "live-predictor",
        sourceGameId: gameId,
        targetGameId,
        recoveryMode: !!deps.recoveryMode,
        bettingWindowMs,
        remainingBeforeTargetMs,
        minWindowMs,
      },
      "skipping prediction: insufficient window before predicted target start",
    );
    return {
      predictionId: null,
      targetGameId,
      kind: "skipped_insufficient_window",
      sourceGameId: gameId,
      sourceCrashAt: crashedAt,
      availableWindowMs: bettingWindowMs,
      remainingBeforeTargetMs,
      outboxEnqueued: 0,
    };
  }

  // TIMESTAMP SEMANTICS FIX (sep 11 investigation §21/§7): this instant is
  // the ATTEMPT START (model as-of time + requested_at), NOT generation.
  // The model runs AFTER it; generated_at is captured post-compute so
  // prediction_compute_ms is a measured fact, not an inferred one.
  const attemptStartedAt = new Date().toISOString();
  const recoveryMode = deps.recoveryMode === true;

  // ── P0: Update in-memory history buffer (ZERO DB) ──
  // The edHandler in game-event-handlers.ts already calls appendCompletedRound
  // before this function, but we keep it here as a belt-and-suspenders measure
  // for the poll-worker path which calls onGameEndPredict directly.
  // BG-PRIMARY: the source round N has NOT crashed — appending it would
  // corrupt the history buffer with a phantom round. Skip.
  if (!deps.bgTrigger) {
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
  }

  // ── P0: History MUST come from memory. NEVER call getSql() here. ──
  // No SQL fallback on the ED prediction path. Prefer explicit
  // N+1_UNAVAILABLE_HISTORY over a silent 700–1000ms Neon query.
  let priorRounds: HistoricalRound[] = [];
  let historyReady = false;
  try {
    const {
      getPriorRoundsSync,
      isHistoryReadyForPrediction,
    } = await import("@/lib/prediction/live/live-history-buffer");

    historyReady = isHistoryReadyForPrediction();
    if (!historyReady) {
      logger.warn(
        { targetGameId, sourceGameId: gameId },
        "N+1_UNAVAILABLE_HISTORY — live history not READY (boot must warm >= MIN_HISTORY before ED)",
      );
    } else {
      priorRounds = getPriorRoundsSync(MAX_HISTORY, gameId, crashedAt);
    }
  } catch {
    /* soft — priorRounds stays empty */
  }

  const t2 = performance.now(); // history loaded from memory

  if (!historyReady || priorRounds.length < MIN_HISTORY) {
    logger.warn(
      {
        targetGameId,
        sourceGameId: gameId,
        historySize: priorRounds.length,
        minHistory: MIN_HISTORY,
        historyReady,
      },
      "N+1_UNAVAILABLE_HISTORY — insufficient warmed history",
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

  // ── P0: Observe crash N on shared ACIE BEFORE evaluating N+1 ──
  // Ordering invariant: Crash N → ACIE.observeRound → state advances → evaluate N+1
  // Never allow PredictionEngine to predict N+1 before ACIE has learned Crash N.
  // BG-PRIMARY: round N has NOT crashed — observing it would poison ACIE
  // state with a phantom crash. ACIE state advances through N-1 (observed at
  // ED(N-1) via observeCrashForACIE); the N+1 evaluation is a valid "one
  // round ahead" projection and ED(N) remains the fallback with the fuller
  // observation.
  if (!deps.bgTrigger) observeCrashForACIE(gameId, multiplier, crashedAt);

  // ── P0: Prediction computation only (ZERO DB, ZERO Telegram, ZERO outbox) ──
  const timestamp = attemptStartedAt;
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

  // TIMESTAMP SEMANTICS FIX: generated_at = model COMPLETION instant. The
  // full timeline lives in outbox metadata: predictionStartedAt (attempt
  // start / model as-of) → prediction_generated_at (here) → persist →
  // outbox → dispatch → telegram_accepted. No derived back-dating.
  const generatedAt = new Date().toISOString();
  const predictionComputeMs = Math.round(predictElapsed);

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

  // P1: Reject emission from stale ACIE state (must have observed this source).
  // BG-PRIMARY: round N is in flight — freshness is proven against the last
  // COMPLETED round (N-1, the tail of the history the evaluation used).
  try {
    const freshnessSource = deps.bgTrigger
      ? (priorRounds[priorRounds.length - 1]?.externalRoundId ?? gameId)
      : gameId;
    const check = assertFreshAcieState(freshnessSource);
    if (!check.ok) {
      logger.error(
        {
          component: "live-predictor",
          event: "STALE_REJECTED",
          sourceGameId: gameId,
          freshnessSource,
          targetGameId,
          reason: check.reason,
          predictionId,
        },
        "Refusing to emit prediction from stale ACIE state",
      );
      try { releaseTarget(targetGameId, owner); } catch { /* soft */ }
      return {
        predictionId: null,
        targetGameId,
        kind: "error",
        temporalValidity: "TEMPORALLY_UNVERIFIED",
        sourceGameId: gameId,
        sourceCrashAt: crashedAt,
      };
    }
  } catch {
    /* if guard module unavailable, continue (boot/test edge) */
  }

  // Compute SLA status (in-memory, no DB clock access)
  const effectiveSlaLagMs = recoveryMode ? SLA_LAG_MS * 2 : SLA_LAG_MS;
  const receivedMs = new Date(crashedAt).getTime();
  const slaLagMsActual = Date.now() - receivedMs;
  const slaViolated =
    Number.isFinite(slaLagMsActual) && slaLagMsActual > effectiveSlaLagMs;

  // Selectivity gate: only persist/notify when there is a real actionable edge.
  // This is the live "NO BET THIS ROUND" path. Without it every evaluation
  // becomes a delivered signal (and is graded WIN/LOSS as if a bet was placed).
  {
    const fs = (signal.featureSummary ?? {}) as Record<string, unknown>;
    const p = signal.probability;
    const c = signal.confidence;
    const strategyAction = String(fs.strategy_action ?? "") || null;
    const pipelineAction = String(fs.pipeline_action ?? "") || null;
    const skip = shouldSkipSignal({
      probability: p,
      confidence: c,
      target: Number(DEFAULT_TARGET),
      strategyAction,
      pipelineAction,
      reasoning: signal.reasoning,
    });
    if (skip) {
      const targetNum = Number(DEFAULT_TARGET);
      const fair = targetNum > 1 ? 1 / targetNum : 0.5;
      const needP = Math.max(MIN_SIGNAL_PROBABILITY, fair + MIN_SIGNAL_EDGE);
      const strategySkip =
        String(strategyAction ?? "").toUpperCase() === "SKIP" ||
        String(pipelineAction ?? "").toUpperCase() === "SKIP";
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
          strategyAction,
          pipelineAction,
          strategySkip,
          recoveryMode,
        },
        strategySkip
          ? "skip signal — strategy/pipeline veto (NO BET this round)"
          : "skip signal — no edge vs fair odds (not every round should fire)",
      );
      // P0 (sep 11 state semantics): an EVALUATED skip — no edge vs fair
      // odds, or strategy/pipeline veto — is a TERMINAL NO_BET decision for
      // this target, not a failure. Complete the claim (not release) so the
      // ED fallback and poll recovery never re-pay the model compute for a
      // decision that was already made. Only genuine failures (persist
      // errors, exceptions, insufficient history/window) leave the target
      // recoverable.
      try {
        completeTarget(targetGameId, owner, { decision: "NO_BET" });
      } catch { /* soft */ }
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

  // ── P0: SIGNAL READY (model only) ──
  // Model computation is complete. Durable pending + outbox handoff MUST
  // complete before we return kind="predicted" so Telegram delivery can
  // start before BG(N+1). live_event_log remains best-effort async.
  const t4 = performance.now(); // signal ready (pre-persist)

  logger.info(
    {
      component: "live-predictor",
      predictionId,
      targetGameId,
      sourceGameId: gameId,
      correlationId,
      recoveryMode,
      claimMs: Number((t1 - t0).toFixed(2)),
      historyMs: Number((t2 - t1).toFixed(2)),
      predictionMs: Number((t3 - t2).toFixed(2)),
      predictionToSignalMs: Number((t4 - t3).toFixed(2)),
      totalMs: Number((t4 - t0).toFixed(2)),
      sourceAgeMs: Math.max(0, Date.now() - new Date(crashedAt).getTime()),
      signalReady: true,
    },
    "PREDICTION_SIGNAL_READY — model computation finished; awaiting durable outbox handoff",
  );
  predictionLifecycleCounters.predictionsReady += 1;

  // ── P0 DURABLE HANDOFF (awaited): pending_predictions + notification_outbox ──
  // Investigation root cause: fire-and-forget persist allowed BG(N+1) / temporal
  // gates to kill or delay the signal until after the target round started.
  // Returning "predicted" only after outbox commit enforces:
  //   generate → durable queue → deliver-before-start
  //
  // POOL-ROUTING FIX: this defaulted to getSql() (the GENERAL pool — shared
  // with dashboard/analytics/forensics/feedback) despite db.ts existing
  // specifically to reserve critical-pool capacity for this exact write.
  // getCriticalPool() (db.ts) was defined but never referenced anywhere —
  // dead code confirming the oversight. This is the ED(N)→N+1 hot path that
  // must complete before the round-N+1 deadline; it belongs on the critical
  // pool like every other caller in this file (see onGameStart above, and
  // the dispatcher's getCriticalSql default in notification-worker.ts).
  const getSqlFn = deps.getSqlFn ?? getCriticalSql;
  const persistT0 = Date.now();
  let outboxEnqueued = 0;
  let pendingWasDuplicate = false;
  let sql: Sql;
  let poolWaitMs = 0;
  try {
    const poolT0 = Date.now();
    sql = await getSqlFn();
    poolWaitMs = Date.now() - poolT0;
  } catch (e) {
    logger.error(
      { component: "live-predictor", targetGameId, error: String(e) },
      "durable handoff: getSql failed — not returning predicted without outbox",
    );
    predictionLifecycleCounters.persistenceFailures += 1;
    try { releaseTarget(targetGameId, owner); } catch { /* soft */ }
    return {
      predictionId,
      targetGameId,
      kind: "persist_failed",
      temporalValidity: "TEMPORALLY_VALID",
      sourceGameId: gameId,
      sourceCrashAt: crashedAt,
      predictionGeneratedAt: timestamp,
      predictionLatencyMs: Math.round(performance.now() - t0),
      outboxEnqueued: 0,
    };
  }

  try {
    const txT0 = Date.now();

    // Message content + deadline are pure JS — computed BEFORE the tx so the
    // transaction holds the pooled client for the minimum possible time.
    const regimeText = signal.regimeId ? ` (${signal.regimeId})` : "";
    const lateTag = slaViolated ? " (delayed)" : "";
    const predictionContent = [
      `NEW PREDICTION${regimeText}${lateTag}`,
      "",
      `Target: ${Number(DEFAULT_TARGET).toFixed(2)}x`,
      `Probability: ${(signal.probability * 100).toFixed(1)}%`,
      `Confidence: ${(signal.confidence * 100).toFixed(1)}%`,
      "",
      // SEP 11: the message previously had NO round identifier — a signal
      // delivered ~2s after crash N read as a LATE prediction for round N,
      // when it is actually for the UPCOMING round N+1 (delivered during
      // round N+1's betting window). Game ID matches the Game ID the
      // WIN/LOSS message later reports, so the pair is verifiable.
      // The completed SOURCE round is stated explicitly so the signal can
      // never be misread as a prediction FOR round N.
      `Game ID: ${targetGameId} (bet NOW — round starting)`,
      deps.bgTrigger
        ? `Trigger round: ${gameId} started — predicting round ${targetGameId}`
        : `Source round: ${gameId} completed — predicting round ${targetGameId}`,
      `Prediction ID: ${predictionId}`,
      `Generated: ${generatedAt}`,
      recoveryMode
        ? "Source: poll recovery"
        : deps.bgTrigger
          ? "Source: live BG (generated during previous round)"
          : "Source: live ED",
    ].join("\n");
    // Shorter live deadline keeps temporal contract tight; recovery keeps more budget.
    // P1: tighter creation-relative deadline (was 8s). Semantic validity is
    // still enforced by target-start checks + BG kill; this shrinks the
    // window where a send can cross round-start while still "in deadline".
    const deadlineMs = recoveryMode
      ? Number(process.env.TELEGRAM_DEADLINE_RECOVERY_MS ?? 10_000)
      : Number(process.env.TELEGRAM_DEADLINE_MS ?? 5_000);
    // CLOCK HYGIENE (co-delivery fix 1): deadline is compared against DB
    // clock_timestamp() in the claim/auth queries. Container Date.now() with
    // DB-ahead skew silently shrank the prediction's send budget by the skew.
    // Use the DB-synced clock (boot + 5-min resync) so the budget is real.
    // TARGET-ANCHORED CLAMP (sep 11 advisor G1/G2): the creation-relative
    // budget alone cannot express "deliver before the target round starts".
    // Clamp the deadline to predictedStart(N+1) − safety margin, with the
    // predicted start expressed on the same DB clock. A late-arriving ED now
    // produces a signal that is dead-lettered past its round start, never
    // delivered into a crashed round.
    const DELIVERY_SAFETY_MS = Number(process.env.DELIVERY_SAFETY_MS ?? 500);
    const deadlineAt = new Date(Math.min(
      authoritativeNowMs() + deadlineMs,
      authoritativeNowMs() + remainingBeforeTargetMs - DELIVERY_SAFETY_MS,
    )).toISOString();
    const outboxNotificationId = randomUUID();
    // EXPLICIT PROVENANCE (sep 11 item 8): trigger identity + full timeline
    // on every prediction, proving N+1 was generated ahead of its round.
    const triggerEvent = deps.bgTrigger ? "BG" : recoveryMode ? "POLL" : "ED";
    const outboxMetadata = JSON.stringify({
      predictionId,
      correlationId,
      targetGameId,
      sourceGameId: gameId,
      triggerEvent,
      triggerRoundId: gameId,
      targetMultiplier: Number(DEFAULT_TARGET),
      probability: signal.probability,
      confidence: signal.confidence,
      regimeName: signal.regimeId,
      slaViolated,
      slaLagMsActual,
      kind: "prediction",
      recoveryMode,
      // TIMESTAMP SEMANTICS FIX: immutable per-row timeline — attempt start
      // (model as-of), measured model-compute cost, model completion. The
      // remaining instants (persist commit, claim, telegram accept) come
      // from the existing columns; no back-dated timestamps anywhere.
      predictionStartedAt: attemptStartedAt,
      predictionComputeMs,
      predictionGeneratedAt: generatedAt,
      signalReadyAt: generatedAt,
      // ED RECEIPT ANCHOR (sep 11): true ED(N) worker-receipt instant — the
      // start of the measured critical path. Persist-commit / outbox-enqueue
      // instants are the outbox row's created_at (same TX commit); dispatch
      // start / telegram accept are send_started_at / telegram_accepted_at.
      edReceivedAt: deps.edReceivedAt ?? null,
    });

    // RTT FIX (sep 11 15:11 logs): persist measured ~590ms = acquire + BEGIN
    // + stmt + COMMIT. The compound CTE is ONE statement — atomic by
    // itself in Postgres — so the explicit transaction adds two round
    // trips of pure overhead to the SIGNAL_READY critical path. Run it
    // directly: one acquire, one round trip (~200ms expected).
    //
    // ROUND-TRIP REDUCTION (plan §3): ONE compound statement performs both
    // inserts atomically. The outbox row is inserted SELECTed from the
    // pending_predictions RETURNING — if the prediction loses a duplicate
    // race (conflict DO NOTHING), the CTE is empty, the outbox insert
    // writes nothing, and we detect the duplicate from 0 returned rows.
    // Same ACID guarantees, one network round trip instead of three.
    //
    // REGRESSION GUARD (sep 11 15:22 logs): these comments were briefly
    // left INSIDE the template literal above (48006b3), so the slash-slash
    // text shipped to Postgres as SQL and every persist failed with a
    // syntax error. Comments live OUTSIDE the template literal. Always.
    const ins = await sql<{ notification_id: string }>`
          with inserted_prediction as (
            insert into pending_predictions (
              prediction_id, target_multiplier, probability, confidence,
              regime_name, regime_confidence, reasoning, feature_summary,
              model_version, requested_at, generated_at,
              target_game_id, source_round_id,
              correlation_id,
              trigger_event, trigger_round_id,
              acie_instance_id, acie_observation_count, acie_state_version,
              feature_hash, prediction_mode, execution_path, strategy_action
            ) values (
              ${predictionId}, ${DEFAULT_TARGET}, ${signal.probability},
              ${signal.confidence}, ${signal.regimeId},
              ${signal.regimeId ? 0.5 : null},
              ${signal.reasoning}, ${JSON.stringify(signal.featureSummary)},
              ${signal.modelVersion}, ${attemptStartedAt}, ${generatedAt},
              ${targetGameId}, ${gameId},
              ${correlationId},
              ${triggerEvent}, ${gameId},
              ${String((signal.featureSummary as Record<string, unknown> | undefined)?.acie_instance_id ?? "") || null},
              ${Number((signal.featureSummary as Record<string, unknown> | undefined)?.acie_observation_count) || null},
              ${Number((signal.featureSummary as Record<string, unknown> | undefined)?.acie_state_version) || null},
              ${String((signal.featureSummary as Record<string, unknown> | undefined)?.feature_hash ?? "") || null},
              ${String((signal.featureSummary as Record<string, unknown> | undefined)?.prediction_mode ?? "UNKNOWN")},
              ${String((signal.featureSummary as Record<string, unknown> | undefined)?.execution_path ?? "") || null},
              ${String((signal.featureSummary as Record<string, unknown> | undefined)?.strategy_action ?? "") || null}
            )
            on conflict (target_game_id) where matched = false and target_game_id is not null do nothing
            returning prediction_id
          )
          insert into notification_outbox (
            notification_id, type, content, metadata, status, priority,
            attempt_count, next_attempt_at, telegram_deadline_at, target_game_id
          )
          select
            ${outboxNotificationId}::uuid, 'prediction', ${predictionContent},
            ${outboxMetadata}::jsonb, 'pending', 100,
            0, now(), ${deadlineAt}::timestamptz, ${targetGameId}
          from inserted_prediction
          returning notification_id
        `;

    if (ins.length === 0) {
      // Duplicate — already persisted by another path (DB is the backstop).
      // No outbox row was inserted (SELECT FROM an empty CTE writes nothing).
      pendingWasDuplicate = true;
    } else {
      outboxEnqueued = 1;
    }

    // live_event_log outside TX (not required for correctness / delivery)
    void sql`
      insert into live_event_log (
        correlation_id, event_kind, game_id, payload, received_at, processed_at,
        processor_latency_ms, sla_violated
      ) values (
        ${correlationId}::text, 'PREDICT', ${targetGameId},
        ${JSON.stringify({ sourceGameId: gameId, targetGameId, recoveryMode, triggerEvent })},
        ${crashedAt}::timestamptz, now(),
        ${Math.max(0, Date.now() - new Date(crashedAt).getTime())}, ${slaViolated}
      )
    `.catch(() => undefined);

    // Wake outbox dispatcher after TX commit so delivery can start immediately.
    // PREDICTION wake (plan §11): lane-aware — dispatcher runs the prediction
    // lane immediately, never queued behind normal work.
    try {
      const { notifyOutbox } = await import("@/lib/prediction/live/outbox-wake");
      notifyOutbox("prediction");
    } catch { /* soft */ }

    {
      const txMs = Date.now() - txT0;
      // RTT FIX: single-statement persist — no explicit TX, so stage timings
      // are unavailable; profile falls back to wall-clock deltas below.
      const stage = null as TxStageTimings | null;
      const profile = {
        component: "live-predictor",
        predictionId,
        sourceGameId: gameId,
        targetGameId,
        correlationId,
        pool_wait_ms: poolWaitMs,
        transaction_acquire_ms: stage?.acquireMs ?? null,
        transaction_begin_ms: stage?.beginMs ?? null,
        prediction_outbox_stmt_ms: stage?.bodyMs ?? null,
        transaction_commit_ms: stage?.commitMs ?? null,
        prediction_persistence_ms:
          stage != null
            ? poolWaitMs + stage.totalMs
            : Date.now() - persistT0,
        crash_end_to_outbox_durable_ms: Math.max(
          0,
          Date.now() - new Date(crashedAt).getTime(),
        ),
        prediction_computation_ms: Math.round(performance.now() - t0) - (Date.now() - persistT0),
        total_persistence_ms: Date.now() - persistT0,
        tx_ms: txMs,
        tx_statements: 1,
        outboxEnqueued,
        // Full pre-dispatch timeline in one line: ED receipt (undefined on
        // recovery) → persist commit = outbox created_at = outbox_enqueued_at.
        persisted_at: new Date().toISOString(),
        ed_received_at: deps.edReceivedAt ?? null,
      };
      if (txMs + poolWaitMs > 300) {
        logger.info(profile, "PERSIST_PROFILE");
      } else {
        logger.debug(profile, "PERSIST_PROFILE");
      }
    }

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
        outboxEnqueued,
        persistenceMs: Number((performance.now() - t4).toFixed(2)),
      },
      outboxEnqueued > 0
        ? "durable prediction handoff complete — outbox pending before return"
        : "durable prediction handoff complete — duplicate pending (outbox may already exist)",
    );
    predictionLifecycleCounters.predictionsPersisted += 1;

    if (pendingWasDuplicate) {
      try { completeTarget(targetGameId, owner); } catch { /* soft */ }
      return {
        predictionId,
        targetGameId,
        kind: "duplicate",
        temporalValidity: "TEMPORALLY_VALID",
        sourceGameId: gameId,
        sourceCrashAt: crashedAt,
        predictionGeneratedAt: timestamp,
        predictionLatencyMs: Math.round(performance.now() - t0),
        outboxEnqueued: 0,
      };
    }
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
        ? "outbox enqueue failed — not returning predicted without Telegram handoff"
        : "durable prediction persistence failed — not returning predicted without handoff",
    );
    predictionLifecycleCounters.persistenceFailures += 1;
    try { releaseTarget(targetGameId, owner); } catch { /* soft */ }
    return {
      predictionId,
      targetGameId,
      kind: "persist_failed",
      temporalValidity: "TEMPORALLY_VALID",
      sourceGameId: gameId,
      sourceCrashAt: crashedAt,
      predictionGeneratedAt: timestamp,
      predictionLatencyMs: Math.round(performance.now() - t0),
      outboxEnqueued: 0,
    };
  }

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
    predictionLatencyMs: Math.round(performance.now() - t0),
    availableWindowMs: bettingWindowMs,
    remainingBeforeTargetMs,
    outboxEnqueued,
  };
}
