import {
  HIGH_THRESHOLD,
  RANGE_DEFS,
  type CrashRound,
  type CrashStats,
  type RangeBucket,
  type StreakSnapshot,
} from "./types";

export function formatMultiplier(value: number): string {
  if (!Number.isFinite(value)) return "—";
  if (value >= 100) return value.toFixed(2);
  return value.toFixed(2);
}

export function median(values: number[]): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid] ?? null;
  const a = sorted[mid - 1];
  const b = sorted[mid];
  if (a === undefined || b === undefined) return null;
  return (a + b) / 2;
}

export function computeStats(rounds: CrashRound[]): CrashStats {
  const values = rounds.map((r) => r.multiplier);
  if (values.length === 0) {
    return { count: 0, average: null, median: null, highest: null, lowest: null };
  }
  const sum = values.reduce((acc, n) => acc + n, 0);
  return {
    count: values.length,
    average: sum / values.length,
    median: median(values),
    highest: Math.max(...values),
    lowest: Math.min(...values),
  };
}

export function computeRanges(rounds: CrashRound[]): RangeBucket[] {
  const total = rounds.length;
  return RANGE_DEFS.map((def) => {
    const count = rounds.filter((r) => {
      if (r.multiplier < def.min) return false;
      if (def.max === null) return true;
      return r.multiplier < def.max;
    }).length;
    return {
      ...def,
      count,
      pct: total === 0 ? 0 : (count / total) * 100,
    };
  });
}

export function computeStreaks(
  roundsNewestFirst: CrashRound[],
  threshold = HIGH_THRESHOLD,
): StreakSnapshot {
  if (roundsNewestFirst.length === 0) {
    return {
      currentKind: "none",
      currentCount: 0,
      maxLow: 0,
      maxHigh: 0,
      threshold,
    };
  }

  const first = roundsNewestFirst[0]!;
  const currentIsLow = first.multiplier < threshold;
  let currentCount = 0;
  for (const round of roundsNewestFirst) {
    const isLow = round.multiplier < threshold;
    if (isLow === currentIsLow) currentCount += 1;
    else break;
  }

  let maxLow = 0;
  let maxHigh = 0;
  let runLow = 0;
  let runHigh = 0;
  for (let i = roundsNewestFirst.length - 1; i >= 0; i -= 1) {
    const round = roundsNewestFirst[i]!;
    if (round.multiplier < threshold) {
      runLow += 1;
      runHigh = 0;
      if (runLow > maxLow) maxLow = runLow;
    } else {
      runHigh += 1;
      runLow = 0;
      if (runHigh > maxHigh) maxHigh = runHigh;
    }
  }

  return {
    currentKind: currentIsLow ? "low" : "high",
    currentCount,
    maxLow,
    maxHigh,
    threshold,
  };
}

export function bandForMultiplier(value: number): "low" | "high" | "moon" {
  if (value >= 10) return "moon";
  if (value >= HIGH_THRESHOLD) return "high";
  return "low";
}
