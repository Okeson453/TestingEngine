/**
 * Adaptive selectivity edge — raises the minimum edge when recent *emitted*
 * signals underperform the quality target, lowers slightly when they beat it.
 *
 * Goal: push realized win rate on delivered signals toward fair + QUALITY_TARGET_EDGE
 * (default ~82% at 1.30×) without permanently silencing the engine.
 */

const FAIR_130 = 1 / 1.3;

/** Target realized hit rate on emitted signals (absolute). */
const TARGET_HIT =
  Number(process.env.SIGNAL_TARGET_HIT_RATE ?? FAIR_130 + 0.05); // ~0.819

/** Base edge from env / predictor MIN_SIGNAL_EDGE default. */
const BASE_EDGE = Number(process.env.MIN_SIGNAL_EDGE ?? 0.03);

const MAX_EDGE = Number(process.env.SIGNAL_MAX_EDGE ?? 0.08);
const MIN_EDGE_FLOOR = Number(process.env.SIGNAL_MIN_EDGE_FLOOR ?? 0.02);

const WINDOW = Math.max(20, Number(process.env.SIGNAL_EDGE_WINDOW ?? 40));

type Outcome = { win: boolean; at: number };
const outcomes: Outcome[] = [];

let currentEdge = Math.max(MIN_EDGE_FLOOR, Math.min(MAX_EDGE, BASE_EDGE));

export function recordSignalOutcome(win: boolean, at: number = Date.now()): void {
  outcomes.push({ win, at });
  if (outcomes.length > WINDOW * 2) {
    outcomes.splice(0, outcomes.length - WINDOW);
  }
  recompute();
}

function recompute(): void {
  const recent = outcomes.slice(-WINDOW);
  if (recent.length < 12) {
    // Not enough evidence — stay near base
    currentEdge = Math.max(MIN_EDGE_FLOOR, Math.min(MAX_EDGE, BASE_EDGE));
    return;
  }
  const hits = recent.filter((o) => o.win).length;
  const hitRate = hits / recent.length;
  const gap = TARGET_HIT - hitRate;
  // gap > 0 → underperforming → raise edge; gap < 0 → overperforming → ease
  const adjustment = Math.max(-0.02, Math.min(0.05, gap * 0.6));
  currentEdge = Math.max(
    MIN_EDGE_FLOOR,
    Math.min(MAX_EDGE, BASE_EDGE + adjustment),
  );
}

export function getAdaptiveMinEdge(): number {
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
  currentEdge = Math.max(MIN_EDGE_FLOOR, Math.min(MAX_EDGE, BASE_EDGE));
}
