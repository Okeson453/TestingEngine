/**
 * Hot-path latency recorders.
 * Spec: TestingEngine_Comprehensive_Diagnosis §13 / latency budget.
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
      const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
      return sorted[idx]!;
    },
  };
}

export const featureLatencyMs = makeRecorder("feature");
export const predictionGenerationMs = makeRecorder("predictionGeneration");
export const predictionPersistMs = makeRecorder("predictionPersist");
export const edToPredictMs = makeRecorder("edToPredict");
export const outboxDeliveryMs = makeRecorder("outboxDelivery");
export const poolWaitMs = makeRecorder("poolWait");
export const interRoundGapMs = makeRecorder("interRoundGap");
export const deliveryMissCount = makeRecorder("deliveryMiss");

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
