/**
 * Hot-path latency recorders.
 * Spec: TestingEngine_Comprehensive_Diagnosis §13 / latency budget.
 * Phase 1 expansion — structured measurements for the full poll → signal path.
 *
 * Lightweight — no prom-client dependency on the prediction path.
 * Values are kept in a ring buffer for operator sampling via getRecentSamples().
 */
type Sample = { ms: number; at: number };

function makeRecorder(name: string, maxSamples = 200) {
  const samples: Sample[] = [];
  return {
    name,
    observe(ms: number) {
      if (!Number.isFinite(ms) || ms < 0) return;
      samples.push({ ms, at: Date.now() });
      if (samples.length > maxSamples) samples.shift();
    },
    getRecentSamples() {
      return samples.slice();
    },
    percentile(p: number): number | null {
      if (samples.length === 0) return null;
      const sorted = samples.map((s) => s.ms).sort((a, b) => a - b);
      const idx = Math.min(
        sorted.length - 1,
        Math.max(0, Math.ceil((p / 100) * sorted.length) - 1),
      );
      return sorted[idx]!;
    },
    count(): number {
      return samples.length;
    },
  };
}

/** Named-mark latency timer for multi-stage paths (decision pipeline). */
export class LatencyTimer {
  private readonly startedAt = performance.now();
  private readonly marks = new Map<string, number>();

  /** Stamp a named stage boundary. */
  mark(name: string): void {
    this.marks.set(name, performance.now());
  }

  /**
   * Milliseconds elapsed since timer start, or since a named mark when given.
   * The stage label is accepted for call-site readability / future wiring.
   */
  record(_stage: string, sinceMark?: string): number {
    const from = (sinceMark != null ? this.marks.get(sinceMark) : undefined) ?? this.startedAt;
    return performance.now() - from;
  }
}

/** Entry-decision total latency window (evaluateEntry → result). */
export const entryDecisionMs = makeRecorder("entryDecision");

export const featureLatencyMs = makeRecorder("feature");
export const predictionGenerationMs = makeRecorder("predictionGeneration");
export const predictionPersistMs = makeRecorder("predictionPersist");
export const edToPredictMs = makeRecorder("edToPredict");
export const outboxDeliveryMs = makeRecorder("outboxDelivery");
/** End-to-end: outbox row created (INSERT) → Telegram accepted. Complements
 * outboxDeliveryMs (claim→accepted); together they split queue wait from send.
 * Audit rec 2 note: the outbox INSERT is issued inside the same persist
 * transaction, <1ms after SIGNAL_READY (measured ED→SIGNAL_READY hot path is
 * 0.4–0.75ms), so this recorder IS the ed→telegram_accepted distribution
 * within ~1ms — a separate ED→accepted histogram would be redundant. The
 * ed→outbox_insert leg is covered by the `outbox_enqueued` trace stage. */
export const outboxTotalDeliveryMs = makeRecorder("outboxTotalDelivery");
export const poolWaitMs = makeRecorder("poolWait");
export const interRoundGapMs = makeRecorder("interRoundGap");
export const deliveryMissCount = makeRecorder("deliveryMiss");

/** Phase 1 — baseline expansion */
export const httpFetchMs = makeRecorder("httpFetch");
export const ingestMs = makeRecorder("ingest");
export const validateBatchMs = makeRecorder("validateBatch");
export const pollTickMs = makeRecorder("pollTick");
export const roundDetectMs = makeRecorder("roundDetect");
export const predictionHandoffMs = makeRecorder("predictionHandoff");

/** Poll recovery observability (2026-09-07) */
export const pollDeferMs = makeRecorder("pollDefer");
export const socketHealthCheckMs = makeRecorder("socketHealthCheck");
export const crashEdLagMs = makeRecorder("crashEdLag");
export const dbFallbackCount = makeRecorder("dbFallback");
export const dbQueryMs = makeRecorder("dbQuery");

/** Alias for ACIE heavy path — real histogram may live in metrics-acie if prom is wired */
export const acieHeavyEvidenceLatencyMs = makeRecorder("acieHeavyEvidence");

/** Phase 18/19 — re-export lifecycle + lead-time metrics */
export {
  socketEventLatencyMs,
  bgToEdDurationMs,
  edProcessingLatencyMs,
  predictionGenerationLatencyMs,
  feedbackLatencyMs,
  endToEndPredictionLatencyMs,
  predictionSkipCount,
  duplicateEventCount,
  pollRecoveryCount,
  missedEventCount,
  predictionLeadTimeMs,
  notificationLeadTimeMs,
  recordLeadTimes,
  getLifecycleMetricsSnapshot,
} from "@/lib/observability/metrics/lifecycle-metrics";
