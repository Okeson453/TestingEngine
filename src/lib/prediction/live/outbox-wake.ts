/**
 * Outbox wake channel — process-level, stateful, coalescing, LANE-AWARE.
 *
 * After a prediction or validation row is committed to `notification_outbox`,
 * producers call `notifyOutbox(kind)` so the dispatcher can drain immediately
 * instead of waiting the full tick for the next timer.
 *
 * Fix plan Phase 4: the previous implementation registered a fresh `once`
 * EventEmitter listener per scheduling cycle. If the timer won the race, the
 * listener leaked — and all leaked listeners later fired, producing
 * overlapping drain ticks. This version keeps AT MOST ONE waiter per wait
 * and coalesces wake events.
 *
 * REMEDIATION PLAN (N→N+1 latency) §11: wake categories. A PREDICTION wake
 * and a NORMAL wake are tracked separately so the dispatcher can tell a
 * latency-critical prediction enqueue from background work, and so wake-to-
 * dispatch latency can be instrumented per lane (§10). Both kinds resolve
 * the dispatcher's wait — the loop always checks the prediction lane first,
 * and the normal lane never blocks it (detached background pass).
 *
 *   - notifyOutbox(kind) with no waiter  -> kind latch set (next wait returns
 *     immediately)
 *   - notifyOutbox(kind) with a waiter   -> that waiter resolves (exactly one)
 *   - multiple notifyOutbox() bursts     -> single drain, never N overlapping
 */

export type WakeKind = "prediction" | "normal";

const wakePending: Record<WakeKind, boolean> = {
  prediction: false,
  normal: false,
};

const waiters = new Set<() => void>();

/** Wake-to-dispatch instrumentation (plan §10): last notify per lane. */
export interface WakeStats {
  lastPredictionNotifyAt: number | null;
  lastNormalNotifyAt: number | null;
  predictionWakeCount: number;
  normalWakeCount: number;
}

const wakeStats: WakeStats = {
  lastPredictionNotifyAt: null,
  lastNormalNotifyAt: null,
  predictionWakeCount: 0,
  normalWakeCount: 0,
};

export function getWakeStats(): WakeStats {
  return { ...wakeStats };
}

/**
 * Signal that at least one new outbox row of `kind` is ready to claim.
 * Prediction producers MUST pass "prediction" (predictor.ts durable handoff);
 * result/validation/alert producers may omit `kind` (defaults "normal").
 */
export function notifyOutbox(kind: WakeKind = "normal"): void {
  if (kind === "prediction") {
    wakeStats.lastPredictionNotifyAt = Date.now();
    wakeStats.predictionWakeCount += 1;
  } else {
    wakeStats.lastNormalNotifyAt = Date.now();
    wakeStats.normalWakeCount += 1;
  }
  wakePending[kind] = true;
  if (waiters.size > 0) {
    for (const w of [...waiters]) w();
  }
}

/**
 * Wait for a wake (any lane), a timeout, or both — whichever first. With no
 * timeout, waits until the next wake. Never throws. Multiple concurrent
 * waiters are supported; a wake resolves all of them (coalesced — one
 * event, one resolve per waiter). Returns which kinds were latched at
 * resolution time (caller decides lane scheduling).
 */
export function waitForOutboxWake(timeoutMs?: number): Promise<{
  prediction: boolean;
  normal: boolean;
}> {
  const consumeLatches = (): { prediction: boolean; normal: boolean } => {
    const out = { prediction: wakePending.prediction, normal: wakePending.normal };
    wakePending.prediction = false;
    wakePending.normal = false;
    return out;
  };
  if (wakePending.prediction || wakePending.normal) {
    return Promise.resolve(consumeLatches());
  }
  return new Promise((resolve) => {
    let timer: ReturnType<typeof setTimeout> | null = null;
    let settled = false;
    const settle = (): void => {
      if (settled) return;
      settled = true;
      waiters.delete(settle);
      if (timer) clearTimeout(timer);
      resolve(consumeLatches());
    };
    waiters.add(settle);
    if (timeoutMs != null) {
      timer = setTimeout(settle, timeoutMs);
      timer.unref?.();
    }
  });
}

/** Test helper. */
export function _resetOutboxWakeForTests(): void {
  wakePending.prediction = false;
  wakePending.normal = false;
  for (const w of [...waiters]) w();
  waiters.clear();
}
