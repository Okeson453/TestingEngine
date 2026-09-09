/**
 * Rolling performance windows for sheath / learning circuit-breaker.
 * Detects deterioration without relying only on consecutive-loss streaks.
 */

export type LearningMode = "NORMAL" | "MONITOR" | "DEGRADED" | "LEARNING_RESTRICTED" | "FROZEN";

type Sample = { hit: 0 | 1; p: number; at: number };

const samples: Sample[] = [];
const MAX_SAMPLES = 300;

let learningMode: LearningMode = "NORMAL";
let frozenUntil = 0;

export function recordOutcome(p: number, hit: 0 | 1): void {
  samples.push({ hit, p, at: Date.now() });
  if (samples.length > MAX_SAMPLES) samples.shift();
  recomputeMode();
}

function windowStats(n: number): {
  n: number;
  winRate: number;
  brier: number;
  avgP: number;
  actualRate: number;
} {
  const slice = samples.slice(-n);
  if (slice.length === 0) {
    return { n: 0, winRate: 0, brier: 0, avgP: 0, actualRate: 0 };
  }
  let hits = 0;
  let brier = 0;
  let sumP = 0;
  for (const s of slice) {
    hits += s.hit;
    brier += (s.p - s.hit) ** 2;
    sumP += s.p;
  }
  const m = slice.length;
  return {
    n: m,
    winRate: hits / m,
    brier: brier / m,
    avgP: sumP / m,
    actualRate: hits / m,
  };
}

function recomputeMode(): void {
  if (Date.now() < frozenUntil) {
    learningMode = "FROZEN";
    return;
  }
  const w50 = windowStats(50);
  const w100 = windowStats(100);
  // Need enough samples
  if (w50.n < 25) {
    learningMode = "MONITOR";
    return;
  }
  // Significant underperformance vs predicted rate or absolute floor
  const under50 = w50.winRate < 0.55 && w50.avgP > 0.62;
  const under100 = w100.n >= 50 && w100.winRate < 0.58;
  const severe = w50.winRate < 0.48 || w50.brier > 0.28;
  if (severe) {
    learningMode = "FROZEN";
    frozenUntil = Date.now() + 15 * 60_000;
    return;
  }
  if (under50 || under100) {
    learningMode = "LEARNING_RESTRICTED";
    return;
  }
  if (w50.winRate < 0.62) {
    learningMode = "DEGRADED";
    return;
  }
  learningMode = "NORMAL";
}

export function getLearningMode(): LearningMode {
  if (Date.now() < frozenUntil) return "FROZEN";
  return learningMode;
}

export function allowAcieLearning(): boolean {
  const m = getLearningMode();
  return m === "NORMAL" || m === "MONITOR" || m === "DEGRADED";
}

export function rollingSnapshot(): Record<string, unknown> {
  return {
    mode: getLearningMode(),
    w25: windowStats(25),
    w50: windowStats(50),
    w100: windowStats(100),
    w250: windowStats(250),
    frozenUntil: frozenUntil || null,
  };
}

export function _resetRollingPerformance(): void {
  samples.length = 0;
  learningMode = "NORMAL";
  frozenUntil = 0;
}
