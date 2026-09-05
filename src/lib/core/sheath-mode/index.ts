/**
 * Sheath mode — automatic throttle when ahead-of-time late rate is high.
 * Spec: Final Implementation Recommendation §7.1 / 6.10
 *
 * When the rolling late rate exceeds the warn threshold, predictions are
 * still generated but operators are warned. Above the halt threshold the
 * predictor skips new generation until the rate recovers.
 */
export interface SheathMode {
  enabled: boolean;
  /** Late rate (0–1) above which we warn */
  warnThreshold: number;
  /** Late rate (0–1) above which we stop generating */
  haltThreshold: number;
  /** Rolling window in ms */
  windowMs: number;
}

export const defaultSheathMode: SheathMode = {
  enabled: true,
  warnThreshold: Number(process.env.SHEATH_WARN_RATE ?? 0.05),
  haltThreshold: Number(process.env.SHEATH_HALT_RATE ?? 0.2),
  windowMs: Number(process.env.SHEATH_WINDOW_MS ?? 60_000),
};

interface LateSample {
  at: number;
  late: boolean;
}

const samples: LateSample[] = [];
const MAX_SAMPLES = 500;

export function recordPredictionOutcome(late: boolean, at: number = Date.now()): void {
  samples.push({ at, late });
  if (samples.length > MAX_SAMPLES) samples.shift();
}

export function getLateRate(windowMs: number = defaultSheathMode.windowMs): {
  rate: number;
  total: number;
  late: number;
} {
  const cutoff = Date.now() - windowMs;
  let total = 0;
  let late = 0;
  for (const s of samples) {
    if (s.at < cutoff) continue;
    total += 1;
    if (s.late) late += 1;
  }
  return { rate: total === 0 ? 0 : late / total, total, late };
}

export type SheathDecision = "allow" | "warn" | "halt";

export function evaluateSheath(
  mode: SheathMode = defaultSheathMode,
): { decision: SheathDecision; rate: number; total: number } {
  if (!mode.enabled) return { decision: "allow", rate: 0, total: 0 };
  const { rate, total } = getLateRate(mode.windowMs);
  if (total < 5) return { decision: "allow", rate, total }; // not enough data
  if (rate >= mode.haltThreshold) return { decision: "halt", rate, total };
  if (rate >= mode.warnThreshold) return { decision: "warn", rate, total };
  return { decision: "allow", rate, total };
}
