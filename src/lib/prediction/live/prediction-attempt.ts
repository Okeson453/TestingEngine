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

// ── Batch 3: rejection telemetry ────────────────────────────────────────────
// Every N+1 attempt that does NOT produce a signal is counted by
// (source, kind) so the "why did 20:50:26 ED not produce a prediction?"
// question is answerable from counters, not log archaeology. The last
// rejection is also persisted to worker_state (fire-and-forget) so the
// evidence survives process restarts.
const attemptCounts = new Map<string, number>();
let lastRejection: {
  at: string;
  source: PredictionSource;
  sourceGameId: string;
  targetGameId: string | null;
  kind: string | null;
} | null = null;

function recordAttempt(
  source: PredictionSource,
  sourceGameId: string,
  targetGameId: string | null,
  kind: string | null,
  attempted: boolean,
): void {
  const key = `${source}:${attempted ? "predicted" : kind ?? "unknown"}`;
  attemptCounts.set(key, (attemptCounts.get(key) ?? 0) + 1);
  if (!attempted) {
    lastRejection = {
      at: new Date().toISOString(),
      source,
      sourceGameId,
      targetGameId,
      kind,
    };
  }
}

/** Counters per (source, kind) + the most recent non-produced attempt. */
export function getN1AttemptStats(): {
  counts: Record<string, number>;
  lastRejection: typeof lastRejection;
} {
  return { counts: Object.fromEntries(attemptCounts), lastRejection };
}

function persistLastRejectionFireAndForget(
  source: PredictionSource,
  sourceGameId: string,
  targetGameId: string | null,
  kind: string | null,
): void {
  void (async () => {
    try {
      const { getSql } = await import("@/lib/db");
      const sql = await getSql();
      const payload = JSON.stringify({
        source,
        sourceGameId,
        targetGameId,
        kind,
        at: new Date().toISOString(),
      });
      await sql`
        insert into worker_state (key, value)
        values ('last_n1_rejection', ${payload})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
    } catch {
      /* soft — telemetry must never throw on the hot path */
    }
  })();
}

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

    recordAttempt(
      source,
      sourceRoundId,
      result?.targetGameId ?? null,
      result?.kind ?? null,
      attempted,
    );
    if (!attempted) {
      // Fire-and-forget durable evidence for post-mortem (batch 3 P0).
      persistLastRejectionFireAndForget(
        source,
        sourceRoundId,
        result?.targetGameId ?? null,
        result?.kind ?? null,
      );
    }

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
