/**
 * Single authoritative N+1 prediction attempt (Diagnosis fix 8).
 *
 * Both the ED hot path and the poll recovery path must go through THIS
 * function so target ownership, temporal gating and result logging behave
 * identically regardless of who calls:
 *
 *   BG(N)      → attemptNPlusOnePrediction({ source: "BG" })   [PRIMARY — fires while round N runs]
 *   ED(N)      → attemptNPlusOnePrediction({ source: "ED" })   [FALLBACK — only if BG(N) failed/missing]
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

export type PredictionSource = "ED" | "RECOVERY" | "BG";

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
  /**
   * Source round crash multiplier. REQUIRED for ED/POLL (the round has
   * ended). NOT meaningful for BG triggers — round N is still running when
   * BG(N) fires, so the multiplier is unknown and unused on that path.
   */
  sourceMultiplier?: number;
  source: PredictionSource;
  correlationId?: string | null;
  /** Optional latency trace to mark stages on (ED hot path). */
  trace?: Trace | null;
  /**
   * ISO instant the authoritative ED(N) event entered the worker. Threaded
   * through to outbox metadata as ed_received_at (undefined on recovery).
   */
  edReceivedAt?: string;
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
  // BG-PRIMARY (sep 11 architecture change): source === "BG" means round N
  // has STARTED (BG(N) received) and we are predicting N+1 while it runs.
  // Round N's crash is UNKNOWN — the predictor must not observe or append it.
  const bgTrigger = source === "BG";

  if (trace) trace.marks.prediction_started = performance.now();

  try {
    const result = await onGameEndPredict(
      sourceRoundId,
      sourceCrashAt,
      bgTrigger ? Number.NaN : (sourceMultiplier as number),
      input.correlationId ?? crypto.randomUUID(),
      { recoveryMode, trace, edReceivedAt: input.edReceivedAt, bgTrigger },
    );
    if (trace) trace.marks.prediction_completed = performance.now();

    // Success requires durable outbox handoff (kind === "predicted").
    // persist_failed / skipped_* / duplicate must not be treated as delivered.
    const attempted =
      result?.kind === "predicted" && result?.predictionId != null;

    recordAttempt(
      source,
      sourceRoundId,
      result?.targetGameId ?? null,
      result?.kind ?? null,
      attempted,
    );
    if (!attempted) {
      // Rate-limited forensic telemetry: skip hot-path worker_state writes for
      // normal terminal outcomes (NO_BET / duplicate / BG-blocked). Those must
      // not contend with ownership or critical DB ops. Persist only genuine
      // failures / unexpected soft kinds.
      const kind = result?.kind ?? null;
      const skipWorkerState =
        kind === "skipped_no_edge" ||
        kind === "duplicate" ||
        kind === "duplicate_no_bet" ||
        (typeof kind === "string" && kind.startsWith("blocked_by_bg"));
      if (!skipWorkerState) {
        persistLastRejectionFireAndForget(
          source,
          sourceRoundId,
          result?.targetGameId ?? null,
          kind,
        );
      }
    }

    logger.info(
      {
        source,
        sourceGameId: sourceRoundId,
        targetGameId: result?.targetGameId ?? null,
        predictionId: result?.predictionId ?? null,
        kind: result?.kind ?? null,
        outboxEnqueued: result?.outboxEnqueued ?? 0,
        recoveryMode,
      },
      attempted
        ? "N+1 SIGNAL_READY (durable outbox enqueued)"
        : `N+1 prediction not persisted (kind=${result?.kind ?? "null"})`,
    );

    return {
      attempted,
      predictionId: result?.predictionId ?? null,
      targetGameId: result?.targetGameId ?? null,
      kind: result?.kind ?? null,
    };
  } catch (error) {
    if (trace) trace.marks.prediction_completed = performance.now();
    const err = error instanceof Error ? error : new Error(String(error));
    const stage =
      (err as { stage?: string }).stage ??
      (err as { featureStage?: string }).featureStage ??
      "unknown";
    const featureStage = (err as { featureStage?: string }).featureStage ?? null;
    const kind = `exception:${stage}`;
    recordAttempt(source, sourceRoundId, null, kind, false);
    persistLastRejectionFireAndForget(
      source,
      sourceRoundId,
      null,
      `${kind}:${err.name}:${err.message}`,
    );
    // P0: never emit a generic failure without stage + root cause.
    logger.error(
      {
        component: "prediction-attempt",
        event: "n1_attempt_failed",
        source,
        sourceRoundId,
        targetRoundId:
          (err as { targetRoundId?: string }).targetRoundId ??
          (err as { context?: { targetRoundId?: string } }).context?.targetRoundId ??
          null,
        predictionType: `N+1:${source === "ED" ? "live" : source === "BG" ? "bg-primary" : "recovery"}`,
        predictionResult: (err as { predictionResult?: unknown }).predictionResult ?? null,
        stage,
        featureStage,
        failureReason: (err as { failureReason?: string }).failureReason ?? err.message,
        errorName: err.name,
        errorMessage: err.message,
        errorStack: err.stack?.slice(0, 2000) ?? null,
        errorCode: (err as { code?: string }).code ?? null,
        correlationId: input.correlationId ?? null,
      },
      `N+1 prediction attempt failed at stage=${stage}`,
    );
    // Do not rethrow — ED must release the claim and schedule recovery; a throw
    // collapsed the failure reason into a single line in some log pipelines.
    return {
      attempted: false,
      predictionId: null,
      targetGameId: null,
      kind: `exception:${stage}:${err.name}:${err.message}`.slice(0, 200),
    };
  }
}
