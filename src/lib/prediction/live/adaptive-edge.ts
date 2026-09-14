/**
 * Adaptive selectivity edge — raises the minimum edge when recent *emitted*
 * signals underperform the quality target, lowers slightly when they beat it.
 *
 * Goal: quality on delivered signals without permanently silencing the engine.
 *
 * Silence recovery (2026-09-14): after prolonged underperformance the edge
 * could sit near MAX and block all new signals for hours (no outcomes → no
 * recovery). Caps and time-based decay force re-sampling so predictions
 * continue; loss-cooldown remains the hard risk skip.
 */

const FAIR_130 = 1 / 1.3;

/** Target realized hit rate on emitted signals (absolute). */
const TARGET_HIT =
  Number(process.env.SIGNAL_TARGET_HIT_RATE ?? 0.80);

/** Base edge from env — default 0 aligns with absolute 65% probability gate.
 *  Set MIN_SIGNAL_EDGE>0 to re-enable fair+edge selectivity. */
const BASE_EDGE = Number(process.env.MIN_SIGNAL_EDGE ?? 0);

/** Soft cap — was 0.08 (needP≈0.85) which silenced the engine for hours. */
const MAX_EDGE = Number(process.env.SIGNAL_MAX_EDGE ?? 0.04);
/** Floor for adaptive edge. Default 0 so a zero BASE_EDGE stays at absolute
 *  probability gate (65%) until outcomes justify raising selectivity. */
const MIN_EDGE_FLOOR = Number(process.env.SIGNAL_MIN_EDGE_FLOOR ?? 0);

const WINDOW = Math.max(20, Number(process.env.SIGNAL_EDGE_WINDOW ?? 40));

/** Half-life for outcome time-weighting (ms). Stale losses stop dominating. */
const OUTCOME_HALF_LIFE_MS = Number(
  process.env.SIGNAL_EDGE_HALF_LIFE_MS ?? 2 * 60 * 60 * 1000,
); // 2h (was 4h)

/** EWMA blend of currentEdge toward recomputed target (recovery speed). */
const EDGE_BLEND = Number(process.env.SIGNAL_EDGE_BLEND ?? 0.35);

/**
 * If no signal outcome for this long, snap edge toward BASE so the engine
 * keeps predicting (loss-cooldown still enforces post-LOSS skips).
 */
const SILENCE_RECOVERY_MS = Number(
  process.env.SIGNAL_EDGE_SILENCE_RECOVERY_MS ?? 45 * 60 * 1000,
); // 45 min

type Outcome = { win: boolean; at: number };
const outcomes: Outcome[] = [];

let currentEdge = Math.max(MIN_EDGE_FLOOR, Math.min(MAX_EDGE, BASE_EDGE));
let lastOutcomeAt = 0;

export function recordSignalOutcome(win: boolean, at: number = Date.now()): void {
  outcomes.push({ win, at });
  lastOutcomeAt = at;
  if (outcomes.length > WINDOW * 2) {
    outcomes.splice(0, outcomes.length - WINDOW);
  }
  recompute();
}

function timeWeight(at: number, now: number): number {
  if (!Number.isFinite(OUTCOME_HALF_LIFE_MS) || OUTCOME_HALF_LIFE_MS <= 0) return 1;
  const age = Math.max(0, now - at);
  return Math.pow(0.5, age / OUTCOME_HALF_LIFE_MS);
}

function recompute(): void {
  const now = Date.now();
  const recent = outcomes.slice(-WINDOW);
  if (recent.length < 12) {
    // Not enough evidence — stay near base (instant, not blended)
    currentEdge = Math.max(MIN_EDGE_FLOOR, Math.min(MAX_EDGE, BASE_EDGE));
    return;
  }

  let wSum = 0;
  let wHits = 0;
  for (const o of recent) {
    const w = timeWeight(o.at, now);
    wSum += w;
    if (o.win) wHits += w;
  }
  const hitRate = wSum > 0 ? wHits / wSum : 0;
  const gap = TARGET_HIT - hitRate;
  // Softer upward gain: a ~70% window no longer forces the full 0.08 ceiling.
  // gap>0 → underperforming → raise; gap<0 → overperforming → ease.
  const adjustment = Math.max(-0.02, Math.min(0.035, gap * 0.45));
  const targetEdge = Math.max(
    MIN_EDGE_FLOOR,
    Math.min(MAX_EDGE, BASE_EDGE + adjustment),
  );

  // EWMA toward target so a past bad window does not permanently silence
  // the engine once the weighted hit rate improves or ages out.
  const blend = Math.min(1, Math.max(0.05, EDGE_BLEND));
  currentEdge = Math.max(
    MIN_EDGE_FLOOR,
    Math.min(MAX_EDGE, currentEdge * (1 - blend) + targetEdge * blend),
  );
}

export function getAdaptiveMinEdge(): number {
  // Opportunistic recompute so time-decay applies even without new outcomes
  // (worker is long-lived; otherwise edge could stay elevated for hours).
  if (outcomes.length >= 12) recompute();

  // Silence recovery: no recent emitted outcomes → force edge toward base
  // so the live path keeps sending signals instead of multi-hour blackouts.
  const now = Date.now();
  if (
    SILENCE_RECOVERY_MS > 0 &&
    lastOutcomeAt > 0 &&
    now - lastOutcomeAt >= SILENCE_RECOVERY_MS
  ) {
    const blend = 0.5;
    currentEdge = Math.max(
      MIN_EDGE_FLOOR,
      Math.min(MAX_EDGE, currentEdge * (1 - blend) + BASE_EDGE * blend),
    );
    if (now - lastOutcomeAt >= SILENCE_RECOVERY_MS * 2) {
      currentEdge = Math.max(MIN_EDGE_FLOOR, Math.min(MAX_EDGE, BASE_EDGE));
    }
  } else if (SILENCE_RECOVERY_MS > 0 && lastOutcomeAt === 0 && outcomes.length === 0) {
    currentEdge = Math.max(MIN_EDGE_FLOOR, Math.min(MAX_EDGE, BASE_EDGE));
  }

  return currentEdge;
}

export function getAdaptiveEdgeStats(): {
  edge: number;
  n: number;
  hitRate: number | null;
  targetHit: number;
} {
  const recent = outcomes.slice(-WINDOW);
  const n = recent.length;
  const hitRate =
    n === 0 ? null : recent.filter((o) => o.win).length / n;
  return { edge: currentEdge, n, hitRate, targetHit: TARGET_HIT };
}

/** Test helper */
export function _resetAdaptiveEdgeForTests(): void {
  outcomes.length = 0;
  lastOutcomeAt = 0;
  currentEdge = Math.max(MIN_EDGE_FLOOR, Math.min(MAX_EDGE, BASE_EDGE));
}
