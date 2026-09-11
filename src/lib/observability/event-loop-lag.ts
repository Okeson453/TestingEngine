/**
 * Event-loop lag sampler (sep 11 19:05-19:12 forensic pass).
 *
 * PROBLEM: the 19:05-19:12 production window showed uniform ~570-1000ms
 * latency on UNRELATED queries in BOTH pools — including `SELECT 1`
 * (595ms, critical), which has no locks, no scans, and no contention —
 * while pool acquires stayed under the 100ms warn threshold all window.
 * Two candidate causes are indistinguishable from that log alone:
 *   (a) a stalled/blocked Node event loop (application-side waiting), or
 *   (b) degraded Neon network RTT / compute latency.
 * queryMs = socket write + server exec + response + EVENT-LOOP DELAY
 * before the continuation runs — a loop stall inflates every in-flight
 * query without touching pool acquires, exactly matching the signature.
 *
 * This sampler makes the two causes separable in production logs:
 *   - slow-query lines gain `loop_lag_ms=` = max lag observed during that
 *     query's wall-clock window. loop_lag ≈ queryMs ⇒ app-side stall;
 *     loop_lag ≈ 0 ⇒ network/Neon.
 *   - standalone `event_loop_lag_ms` warns give a stall timeline even
 *     when no query is in flight.
 *
 * Design: a 50ms setInterval sampler records {t, lag} into a bounded ring
 * (~6s). Reading is passive; the timer is unref'd so it never holds the
 * process open. No dependencies, no allocation churn (one object per tick).
 */

const SAMPLE_INTERVAL_MS = 50;
const RING_CAP = 128; // ~6.4s of samples

interface LagSample {
  /** Sampler tick time (performance.now ms). */
  at: number;
  /** Milliseconds of observed lag for this tick (measured - expected). */
  lagMs: number;
}

const ring: LagSample[] = [];
let lastTick = performance.now();
let started = false;

/** Threshold for the standalone stall warn (first-lag log only). */
const WARN_LAG_MS = 400;

function sample(): void {
  const now = performance.now();
  const lagMs = Math.max(0, now - lastTick - SAMPLE_INTERVAL_MS);
  lastTick = now;
  ring.push({ at: now, lagMs });
  if (ring.length > RING_CAP) ring.shift();
  if (lagMs >= WARN_LAG_MS) {
    // Inline the value: Railway raw logs show message text only.
    console.warn(`[loop] event_loop_lag_ms=${Math.round(lagMs)} — event loop stalled; slow queries in this window are application-side, not Neon/network`);
  }
}

/** Idempotent start; safe to call from multiple importers. */
export function startEventLoopLagSampler(): void {
  if (started) return;
  started = true;
  const t = setInterval(sample, SAMPLE_INTERVAL_MS);
  t.unref?.();
}

/**
 * Max event-loop lag observed in [fromMs, toMs] (performance.now clock).
 * Returns 0 when the sampler has no samples in the window (e.g. not
 * started) — callers must treat 0 as "no evidence of a stall", not
 * "provably no stall".
 */
export function maxLoopLagBetween(fromMs: number, toMs: number): number {
  let max = 0;
  for (let i = ring.length - 1; i >= 0; i -= 1) {
    const s = ring[i];
    if (s.at < fromMs) break;
    if (s.at <= toMs && s.lagMs > max) max = s.lagMs;
  }
  return max;
}
