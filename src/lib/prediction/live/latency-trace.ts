/**
 * Monotonic ED→signal stage latency tracing (P50/P95/P99).
 */
import { getLogger } from "@/lib/observability/logger";
import { pollDeferMs, crashEdLagMs } from "@/lib/observability/performance/latency";

const logger = getLogger("latency-trace");

export type Stage =
  | "ws_received"
  | "decoded"
  | "normalized"
  | "state_updated"
  | "target_claimed"
  | "prediction_started"
  | "prediction_completed"
  | "signal_ready"
  | "persist_started"
  | "outbox_enqueued"
  | "persist_completed"
  | "delivery_started"
  | "delivery_accepted";

export type Trace = {
  traceId: string;
  sourceGameId: string;
  targetGameId?: string;
  t0: number;
  marks: Partial<Record<Stage, number>>;
};

const MAX_SAMPLES = 200;
const samples: number[] = []; // signal_ready - ws_received
const stageSamples: Record<string, number[]> = {};
// Fix 13/14: authoritative timing chain includes signal + delivery legs.
let lastSignalAt: number | null = null;
const deliverySamples: number[] = []; // outbox claim → telegram accepted

// Finding 4.2: ready ↔ durable gap counters. A sustained gap (persisted
// lagging ready) is the early-warning signal for async persistence failures
// — alertable directly, no log-diffing required.
export const predictionLifecycleCounters = {
  predictionsReady: 0,
  predictionsPersisted: 0,
  persistenceFailures: 0,
};

function mono(): number {
  return typeof performance !== "undefined" && performance.now
    ? performance.now()
    : Number(process.hrtime.bigint()) / 1e6;
}

export function startTrace(traceId: string, sourceGameId: string): Trace {
  const t0 = mono();
  return { traceId, sourceGameId, t0, marks: { ws_received: t0 } };
}

export function mark(trace: Trace, stage: Stage): void {
  trace.marks[stage] = mono();
}

export function finishSignalReady(trace: Trace): number {
  mark(trace, "signal_ready");
  const start = trace.marks.ws_received ?? trace.t0;
  const end = trace.marks.signal_ready ?? mono();
  const total = end - start;
  samples.push(total);
  if (samples.length > MAX_SAMPLES) samples.shift();
  lastSignalAt = Date.now();

  const stages: Array<[string, Stage, Stage]> = [
    ["ws_to_state", "ws_received", "state_updated"],
    ["state_to_claim", "state_updated", "target_claimed"],
    ["claim_to_predict", "target_claimed", "prediction_started"],
    ["prediction_compute", "prediction_started", "prediction_completed"],
    ["predict_to_signal", "prediction_completed", "signal_ready"],
  ];
  for (const [name, a, b] of stages) {
    const ta = trace.marks[a];
    const tb = trace.marks[b];
    if (ta != null && tb != null) {
      const arr = (stageSamples[name] ??= []);
      arr.push(tb - ta);
      if (arr.length > MAX_SAMPLES) arr.shift();
    }
  }
  return total;
}

/**
 * P1: record the async persistence leg (signal → outbox insert → DB done).
 * Called when the async persist IIFE completes, after finishSignalReady —
 * the persist marks land on the same trace object after the signal is ready.
 */
export function finishPersist(trace: Trace): number {
  mark(trace, "persist_completed");
  const persistStart = trace.marks.persist_started;
  const persistEnd = trace.marks.persist_completed;
  if (persistStart != null && persistEnd != null) {
    const arr = (stageSamples["persist"] ??= []);
    arr.push(persistEnd - persistStart);
    if (arr.length > MAX_SAMPLES) arr.shift();
  }
  const signalAt = trace.marks.signal_ready;
  if (signalAt != null && persistEnd != null) {
    const arr = (stageSamples["signal_to_persist"] ??= []);
    arr.push(persistEnd - signalAt);
    if (arr.length > MAX_SAMPLES) arr.shift();
  }
  const wsAt = trace.marks.ws_received ?? trace.t0;
  return persistEnd != null ? Math.max(0, persistEnd - wsAt) : 0;
}

function percentile(arr: number[], p: number): number {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((x, y) => x - y);
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx] ?? 0;
}

function recorderPercentiles(r: {
  percentile(p: number): number | null;
  count(): number;
}): { p50: number | null; p95: number | null; n: number } {
  return { p50: r.percentile(50), p95: r.percentile(95), n: r.count() };
}

export function snapshotLatencyBudget(): Record<string, unknown> {
  const n = samples.length;
  return {
    n,
    // Second-opinion audit rec 5: surface poll-stream health distributions in
    // the 5-min snapshot so ops can alert directly (greppable, no log-diffing).
    // Sustained crash_ed_lag_ms p95 above one inter-round gap (~30s cadence,
    // alert well below that) = native WS stream stalled and poll recovery is
    // carrying traffic; poll_defer_ms > 0 means recovery was deferred.
    stream_health: {
      poll_defer_ms: recorderPercentiles(pollDeferMs),
      crash_ed_lag_ms: recorderPercentiles(crashEdLagMs),
    },
    predictionLifecycle: { ...predictionLifecycleCounters },
    ed_to_signal_ms: {
      p50: percentile(samples, 50),
      p95: percentile(samples, 95),
      p99: percentile(samples, 99),
      max: n ? Math.max(...samples) : 0,
    },
    delivery_ms: {
      p50: percentile(deliverySamples, 50),
      p95: percentile(deliverySamples, 95),
      n: deliverySamples.length,
    },
    lastSignalAt: lastSignalAt != null ? new Date(lastSignalAt).toISOString() : null,
    stages: Object.fromEntries(
      Object.entries(stageSamples).map(([k, arr]) => [
        k,
        { p50: percentile(arr, 50), p95: percentile(arr, 95), n: arr.length },
      ]),
    ),
  };
}

export function logLatencyBudgetSnapshot(): void {
  logger.info(
    { component: "latency-trace", ...snapshotLatencyBudget() },
    "ED→signal latency budget snapshot",
  );
}

/** Fix 13: record the delivery leg (outbox claimed → Telegram accepted, ms). */
export function recordDeliveryLatency(latencyMs: number): void {
  deliverySamples.push(Math.max(0, latencyMs));
  if (deliverySamples.length > MAX_SAMPLES) deliverySamples.shift();
}

/** Fix 13/14: last SIGNAL_READY wall-clock time (ms epoch or null). */
export function getLastSignalAt(): number | null {
  return lastSignalAt;
}
