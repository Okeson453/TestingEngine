/**
 * Authoritative closed-loop feedback entry point (audit §27–§34).
 *
 * Every resolved prediction MUST flow through processResolvedPredictionFeedback
 * before N+1 is generated. Updates are idempotent by prediction_id.
 *
 * Durability (Phase 2):
 * - In-memory Set is a fast path for the current process lifetime.
 * - PostgreSQL `prediction_validations.feedback_applied_at` is the recovery
 *   source of truth. A row is claimed with
 *   UPDATE ... SET feedback_applied_at = now() WHERE prediction_id = $1
 *   AND feedback_applied_at IS NULL RETURNING prediction_id.
 * - Second calls (restart, duplicate ed, poll recovery) see no claim and no-op.
 */

import { getLogger } from "@/lib/observability/logger";
import { getSql } from "@/lib/db";

const logger = getLogger("prediction-feedback");

export type LearningStatus = "COMPLETE" | "PARTIAL" | "FAILED";

export type FailureClassification =
  | "OVERCONFIDENT"
  | "UNDERCONFIDENT"
  | "CALIBRATION_ERROR"
  | "REGIME_MISMATCH"
  | "SEQUENCE_MISMATCH"
  | "PATTERN_MISREAD"
  | "INSUFFICIENT_DATA"
  | "DRIFT"
  | "MODEL_ERROR"
  | "UNKNOWN"
  | "WIN";

export interface PredictionFeedbackInput {
  predictionId: string;
  targetGameId: string;
  predictedProbability: number;
  predictedConfidence?: number | null;
  targetMultiplier: number;
  actualMultiplier: number;
  result: "WIN" | "LOSS";
  regimeAtPrediction?: string | null;
  modelVersion?: string | null;
  correlationId?: string | null;
  featureSummary?: Record<string, unknown> | null;
  resolvedAt: string;
}

export interface LearningComponents {
  incremental: boolean;
  baseline: boolean;
  calibration: boolean;
  meta: boolean;
  modelPerformance: boolean;
  acie: boolean;
  sol: boolean;
}

export interface FailureAnalysis {
  classification: FailureClassification;
  probabilityError: number;
  residual: number;
  squaredError: number;
  logLoss: number;
  evidence: string[];
}

export interface FeedbackResult {
  predictionId: string;
  targetGameId: string;
  learningStatus: LearningStatus;
  learningComponents: LearningComponents;
  analysis: FailureAnalysis;
  incrementalCount: number | null;
}

/** Idempotency set for process lifetime (DB claim is primary). */
const processedIds = new Set<string>();
const MAX_PROCESSED = 5_000;

function logLoss(p: number, y: 0 | 1): number {
  const eps = 1e-7;
  const q = Math.min(1 - eps, Math.max(eps, p));
  return y === 1 ? -Math.log(q) : -Math.log(1 - q);
}

/**
 * Evidence-based failure classification (audit §35).
 * Does not invent causality beyond residual/probability evidence.
 */
export function analyzeFailure(
  predicted: number,
  actual: 0 | 1,
  result: "WIN" | "LOSS",
): FailureAnalysis {
  const residual = predicted - actual;
  const squaredError = residual * residual;
  const ll = logLoss(predicted, actual);
  const evidence: string[] = [];
  let classification: FailureClassification = "UNKNOWN";

  if (result === "WIN") {
    classification = "WIN";
    evidence.push("outcome matched target threshold");
    if (predicted < 0.35) {
      classification = "UNDERCONFIDENT";
      evidence.push("WIN with low predicted probability");
    }
  } else {
    if (predicted >= 0.7) {
      classification = "OVERCONFIDENT";
      evidence.push("LOSS with high predicted probability (>=0.7)");
    } else if (predicted >= 0.55) {
      classification = "CALIBRATION_ERROR";
      evidence.push("LOSS with moderate-high probability — likely miscalibration");
    } else if (predicted < 0.4) {
      classification = "UNKNOWN";
      evidence.push("LOSS with low probability — residual small; limited signal");
    } else {
      classification = "MODEL_ERROR";
      evidence.push("LOSS near decision boundary");
    }
  }

  return {
    classification,
    probabilityError: Math.abs(residual),
    residual,
    squaredError,
    logLoss: ll,
    evidence,
  };
}

/**
 * Claim durable ownership of feedback for this prediction_id.
 * Returns true if this process/call owns the claim (first successful claim).
 * Returns false if already claimed or no validation row exists.
 */
async function claimFeedbackDurable(predictionId: string): Promise<boolean> {
  try {
    const sql = await getSql();
    const claimed = await sql<{ prediction_id: string }>`
      UPDATE prediction_validations
      SET feedback_applied_at = now()
      WHERE prediction_id = ${predictionId}
        AND feedback_applied_at IS NULL
      RETURNING prediction_id
    `;
    return claimed.length > 0;
  } catch (e) {
    // Column may not exist yet (pre-migration). Fall back to in-memory only.
    logger.warn(
      { predictionId, error: String(e) },
      "durable feedback claim failed — falling back to in-memory idempotency",
    );
    return !processedIds.has(predictionId);
  }
}

/**
 * Canonical feedback pipeline. Safe to call multiple times for same predictionId
 * (second call is a no-op). Always await before generating N+1.
 */
export async function processResolvedPredictionFeedback(
  input: PredictionFeedbackInput,
): Promise<FeedbackResult> {
  const components: LearningComponents = {
    incremental: false,
    baseline: false,
    calibration: false,
    meta: false,
    modelPerformance: false,
    acie: false,
    sol: false,
  };

  const actual: 0 | 1 = input.result === "WIN" ? 1 : 0;
  const predicted = Number.isFinite(input.predictedProbability)
    ? Math.min(0.999, Math.max(0.001, input.predictedProbability))
    : 0.5;
  const analysis = analyzeFailure(predicted, actual, input.result);

  // Fast path: process memory
  if (processedIds.has(input.predictionId)) {
    logger.debug(
      { predictionId: input.predictionId },
      "feedback already processed — idempotent skip (memory)",
    );
    return {
      predictionId: input.predictionId,
      targetGameId: input.targetGameId,
      learningStatus: "COMPLETE",
      learningComponents: components,
      analysis,
      incrementalCount: null,
    };
  }

  // Durable claim (survives restart / duplicate ed / poll recovery)
  const owned = await claimFeedbackDurable(input.predictionId);
  if (!owned) {
    processedIds.add(input.predictionId);
    logger.debug(
      { predictionId: input.predictionId },
      "feedback already processed — idempotent skip (durable)",
    );
    return {
      predictionId: input.predictionId,
      targetGameId: input.targetGameId,
      learningStatus: "COMPLETE",
      learningComponents: components,
      analysis,
      incrementalCount: null,
    };
  }

  let incrementalCount: number | null = null;

  // 1. Incremental state (every crash)
  try {
    const { globalIncrementalState } = await import(
      "@/lib/prediction/state/incremental-state-engine"
    );
    globalIncrementalState.update(input.actualMultiplier);
    incrementalCount = globalIncrementalState.snapshot().count;
    components.incremental = true;
  } catch (e) {
    logger.warn({ error: String(e) }, "incremental state update failed");
  }

  // 2. Baseline adaptive multipliers
  try {
    const { globalBaselineModel } = await import(
      "@/lib/prediction/models/baseline-model"
    );
    if (typeof globalBaselineModel.observeOutcome === "function") {
      globalBaselineModel.observeOutcome(
        predicted,
        actual,
        input.actualMultiplier,
        input.targetMultiplier as 1.3,
      );
      components.baseline = true;
    }
  } catch (e) {
    logger.warn({ error: String(e) }, "baseline observeOutcome failed");
  }

  // 3. Calibration + meta + production controller via feedbackPredictionPipeline
  try {
    const mod = await import("@/lib/prediction/prediction-pipeline");
    let metaFeatures: import("@/lib/prediction/models/meta-logistic-model").MetaFeatures | undefined;
    try {
      const { globalIncrementalState } = await import(
        "@/lib/prediction/state/incremental-state-engine"
      );
      const { globalCalibrationState } = await import(
        "@/lib/prediction/calibration/calibration-state"
      );
      const snap = globalIncrementalState.snapshot();
      const calMetrics = globalCalibrationState.metrics();
      metaFeatures = {
        baseProbability: predicted,
        disagreement: 0,
        regimeConfidence: 0.5,
        dataQuality: Math.min(1, snap.count / 100),
        sampleCount: snap.count,
        recentLogLoss: calMetrics.logLoss || 0.5,
        recentBrier: calMetrics.brier || 0.25,
        ece: calMetrics.ece,
        shortHitRate: globalIncrementalState.shortHitRate13(),
        markovP: globalIncrementalState.markovPNextAbove13(),
      };
    } catch {
      metaFeatures = undefined;
    }
    mod.feedbackPredictionPipeline(predicted, actual, metaFeatures);
    components.calibration = true;
    components.meta = metaFeatures != null;
  } catch (e) {
    logger.warn({ error: String(e) }, "feedbackPredictionPipeline failed");
  }

  // 4. Model performance tracker
  try {
    const mod = await import("@/lib/prediction/ensemble/model-performance");
    const name = String(input.modelVersion ?? "baseline");
    mod.globalModelPerformance.observe(name, predicted, actual);
    mod.globalModelPerformance.observe("live", predicted, actual);
    components.modelPerformance = true;
  } catch (e) {
    logger.warn({ error: String(e) }, "model-performance observe failed");
  }

  // 5. ACIE observeRound (SOL/TPL/PSI/SAFE via engine)
  try {
    const g = globalThis as {
      __acieEngine__?: {
        observeRound: (r: {
          roundId: string;
          crashPoint: number;
          timestamp?: string;
        }) => unknown;
      };
    };
    let eng = g.__acieEngine__;
    // If boot restore failed or tests, lazily construct and cache engine
    if (!eng || typeof eng.observeRound !== "function") {
      try {
        const { ACIEEngine } = await import("@/lib/prediction/acie/engine");
        eng = new ACIEEngine();
        g.__acieEngine__ = eng;
      } catch {
        eng = undefined;
      }
    }
    if (eng && typeof eng.observeRound === "function") {
      eng.observeRound({
        roundId: input.targetGameId,
        crashPoint: input.actualMultiplier,
        timestamp: input.resolvedAt,
      });
      components.acie = true;
      components.sol = true; // SOL is updated inside ACIE observeRound
    }
  } catch (e) {
    logger.warn({ error: String(e) }, "ACIE observeRound failed");
  }

  const okCount = Object.values(components).filter(Boolean).length;
  const learningStatus: LearningStatus =
    okCount >= 4 ? "COMPLETE" : okCount >= 1 ? "PARTIAL" : "FAILED";

  processedIds.add(input.predictionId);
  if (processedIds.size > MAX_PROCESSED) {
    const first = processedIds.values().next().value;
    if (first) processedIds.delete(first);
  }

  // Phase 11 — advance round state machine
  try {
    const { markFeedbackApplied } = await import(
      "@/lib/prediction/live/live-round-state"
    );
    await markFeedbackApplied(input.targetGameId);
  } catch {
    /* soft */
  }

  // Phase 18 — feedback latency observed by caller; count complete here
  try {
    const { feedbackLatencyMs } = await import(
      "@/lib/observability/metrics/lifecycle-metrics"
    );
    // residual timing not known here; observe 0 as completion marker
    feedbackLatencyMs.observe(0);
  } catch {
    /* soft */
  }

  logger.info(
    {
      component: "prediction-feedback",
      predictionId: input.predictionId,
      targetGameId: input.targetGameId,
      result: input.result,
      classification: analysis.classification,
      residual: Number(analysis.residual.toFixed(4)),
      learningStatus,
      learningComponents: components,
      incrementalCount,
    },
    "closed-loop feedback processed",
  );

  return {
    predictionId: input.predictionId,
    targetGameId: input.targetGameId,
    learningStatus,
    learningComponents: components,
    analysis,
    incrementalCount,
  };
}

/** Test helper: clear idempotency set */
export function resetFeedbackIdempotencyForTests(): void {
  processedIds.clear();
}
