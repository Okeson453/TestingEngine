/**
 * Single authoritative N+1 prediction attempt (Diagnosis fix 8).
 *
 * Both the ED hot path and the poll recovery path must go through THIS
 * function so target ownership, temporal gating and result logging behave
 * identically regardless of who calls:
 *
 *   ED(N)      → attemptNPlusOnePrediction({ source: "ED" })
 *   Poll(N)    → attemptNPlusOnePrediction({ source: "RECOVERY" })
 *
 * Layers of duplicate defence (in order):
 *   1. in-memory TargetCoordinator claim (process-local fast path)
 *   2. `pending_predictions` unique constraint (durability backstop)
 * The coordinator is intentionally memory-only; the DB constraint is the
 * real distributed lock. Callers must NOT implement their own prediction
 * decision logic on top.
 */
import { onGameEndPredict } from "@/lib/prediction/live/predictor";
import { getLogger } from "@/lib/observability/logger";
import type { Trace } from "@/lib/prediction/live/latency-trace";

const logger = getLogger("prediction-attempt");

export type PredictionSource = "ED" | "RECOVERY";

export interface AttemptNPlusOneInput {
  sourceRoundId: string;
  sourceCrashAt: string;
  sourceMultiplier: number;
  source: PredictionSource;
  correlationId?: string | null;
  /** Optional latency trace to mark stages on (ED hot path). */
  trace?: Trace | null;
}

export interface AttemptNPlusOneResult {
  attempted: boolean;
  predictionId: string | null;
  targetGameId: string | null;
  kind: string | null;
}

export async function attemptNPlusOnePrediction(
  input: AttemptNPlusOneInput,
): Promise<AttemptNPlusOneResult> {
  const { sourceRoundId, sourceCrashAt, sourceMultiplier, source } = input;
  const trace = input.trace ?? null;
  const recoveryMode = source === "RECOVERY";

  if (trace) trace.marks.prediction_started = performance.now();

  try {
    const result = await onGameEndPredict(
      sourceRoundId,
      sourceCrashAt,
      sourceMultiplier,
      input.correlationId ?? crypto.randomUUID(),
      { recoveryMode },
    );
    if (trace) trace.marks.prediction_completed = performance.now();

    const attempted =
      result?.predictionId != null && result.kind !== "duplicate";

    logger.info(
      {
        source,
        sourceGameId: sourceRoundId,
        targetGameId: result?.targetGameId ?? null,
        predictionId: result?.predictionId ?? null,
        kind: result?.kind ?? null,
        recoveryMode,
      },
      attempted
        ? "N+1 prediction attempt owned"
        : "N+1 prediction attempt not owned / skipped",
    );

    return {
      attempted,
      predictionId: result?.predictionId ?? null,
      targetGameId: result?.targetGameId ?? null,
      kind: result?.kind ?? null,
    };
  } catch (error) {
    if (trace) trace.marks.prediction_completed = performance.now();
    logger.error(
      {
        source,
        sourceGameId: sourceRoundId,
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined,
      },
      "N+1 prediction attempt failed",
    );
    throw error;
  }
}
