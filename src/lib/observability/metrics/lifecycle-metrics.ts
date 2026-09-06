/**
 * Phase 18 — Lifecycle observability metrics.
 * Phase 19 — Prediction / notification lead-time tracking.
 *
 * Lightweight ring buffers (no prom dependency on hot path).
 */
type Sample = { ms: number; at: number; tags?: Record<string, string> };

function makeRecorder(name: string, maxSamples = 300) {
  const samples: Sample[] = [];
  let count = 0;
  return {
    name,
    observe(ms: number, tags?: Record<string, string>) {
      if (!Number.isFinite(ms) || ms < 0) return;
      count += 1;
      samples.push({ ms, at: Date.now(), tags });
      if (samples.length > maxSamples) samples.shift();
    },
    increment() {
      count += 1;
    },
    get count() {
      return count;
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
    snapshot() {
      return {
        name,
        count,
        p50: this.percentile(50),
        p95: this.percentile(95),
        p99: this.percentile(99),
        lastMs: samples.length ? samples[samples.length - 1]!.ms : null,
      };
    },
  };
}

function makeCounter(name: string) {
  const byReason = new Map<string, number>();
  let total = 0;
  return {
    name,
    inc(reason = "default") {
      total += 1;
      byReason.set(reason, (byReason.get(reason) ?? 0) + 1);
    },
    get total() {
      return total;
    },
    snapshot() {
      return { name, total, byReason: Object.fromEntries(byReason) };
    },
  };
}

/** Phase 18 — end-to-end lifecycle latencies */
export const socketEventLatencyMs = makeRecorder("socket_event_latency");
export const bgToEdDurationMs = makeRecorder("bg_to_ed_duration");
export const edProcessingLatencyMs = makeRecorder("ed_processing_latency");
export const predictionGenerationLatencyMs = makeRecorder("prediction_generation_latency");
export const feedbackLatencyMs = makeRecorder("feedback_latency");
export const acieProcessingLatencyMs = makeRecorder("acie_processing_latency");
export const modelUpdateLatencyMs = makeRecorder("model_update_latency");
export const poolWaitLatencyMs = makeRecorder("pool_wait_latency");
export const transactionLatencyMs = makeRecorder("transaction_latency");
export const outboxLatencyMs = makeRecorder("outbox_latency");
export const telegramDeliveryLatencyMs = makeRecorder("telegram_delivery_latency");

/** Round N ended → feedback done → N+1 prediction generated */
export const endToEndPredictionLatencyMs = makeRecorder("e2e_round_end_to_n1_prediction");

export const predictionSkipCount = makeCounter("prediction_skip");
export const duplicateEventCount = makeCounter("duplicate_event");
export const pollRecoveryCount = makeCounter("poll_recovery");
export const missedEventCount = makeCounter("missed_event");
export const socketReconnectCount = makeCounter("socket_reconnect");

/** Phase 19 — lead times vs next round start */
export const predictionLeadTimeMs = makeRecorder("prediction_lead_time");
export const notificationLeadTimeMs = makeRecorder("notification_lead_time");

export interface LeadTimeInput {
  predictionGeneratedAt: string | Date;
  notificationSentAt?: string | Date | null;
  nextRoundStartAt: string | Date;
}

/**
 * prediction_lead_time = next_round_start - prediction_generated
 * notification_lead_time = next_round_start - notification_sent
 * Negative values mean the signal was stale (after target start).
 */
export function recordLeadTimes(input: LeadTimeInput): {
  predictionLeadMs: number | null;
  notificationLeadMs: number | null;
  stale: boolean;
} {
  const start = new Date(input.nextRoundStartAt).getTime();
  const gen = new Date(input.predictionGeneratedAt).getTime();
  if (!Number.isFinite(start) || !Number.isFinite(gen)) {
    return { predictionLeadMs: null, notificationLeadMs: null, stale: false };
  }
  const predictionLeadMs = start - gen;
  predictionLeadTimeMs.observe(Math.max(0, predictionLeadMs));
  let notificationLeadMs: number | null = null;
  if (input.notificationSentAt) {
    const sent = new Date(input.notificationSentAt).getTime();
    if (Number.isFinite(sent)) {
      notificationLeadMs = start - sent;
      notificationLeadTimeMs.observe(Math.max(0, notificationLeadMs));
    }
  }
  const stale = predictionLeadMs < 0;
  return { predictionLeadMs, notificationLeadMs, stale };
}

export function getLifecycleMetricsSnapshot() {
  return {
    latencies: {
      socketEvent: socketEventLatencyMs.snapshot(),
      bgToEd: bgToEdDurationMs.snapshot(),
      edProcessing: edProcessingLatencyMs.snapshot(),
      predictionGeneration: predictionGenerationLatencyMs.snapshot(),
      feedback: feedbackLatencyMs.snapshot(),
      e2eN1: endToEndPredictionLatencyMs.snapshot(),
      predictionLead: predictionLeadTimeMs.snapshot(),
      notificationLead: notificationLeadTimeMs.snapshot(),
      poolWait: poolWaitLatencyMs.snapshot(),
      outbox: outboxLatencyMs.snapshot(),
    },
    counters: {
      predictionSkip: predictionSkipCount.snapshot(),
      duplicateEvent: duplicateEventCount.snapshot(),
      pollRecovery: pollRecoveryCount.snapshot(),
      missedEvent: missedEventCount.snapshot(),
      socketReconnect: socketReconnectCount.snapshot(),
    },
  };
}
