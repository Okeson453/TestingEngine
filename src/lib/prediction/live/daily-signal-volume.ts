/**
 * Daily signal volume — 1500 is both TARGET and hard MAX.
 *
 * Target: pace issued betting signals toward 1500/day.
 * Max: never exceed 1500 issued signals/day.
 */

const TARGET = Math.max(
  1,
  Number(process.env.DAILY_SIGNAL_TARGET ?? process.env.DAILY_SIGNAL_LIMIT ?? 1500),
);
const LIMIT = Math.max(
  TARGET,
  Number(process.env.DAILY_SIGNAL_LIMIT ?? 1500),
);

let dayKey = "";
let issuedToday = 0;

function utcDayKey(d = new Date()): string {
  return d.toISOString().slice(0, 10);
}

function rollDay(): void {
  const k = utcDayKey();
  if (k !== dayKey) {
    dayKey = k;
    issuedToday = 0;
  }
}

/** Call after a prediction outbox row is durably enqueued. */
export function recordDailySignalIssued(): void {
  rollDay();
  issuedToday += 1;
}

export function getDailySignalVolume(): {
  issued: number;
  target: number;
  limit: number;
  day: string;
  /** 0–1+ progress toward target (may exceed 1 if over target before hard stop). */
  progress: number;
  /** True when wall-clock fraction of day exceeds issued/target (behind pace). */
  behindPace: boolean;
  atLimit: boolean;
} {
  rollDay();
  const now = new Date();
  const start = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  const dayFrac = Math.min(1, Math.max(0, (now.getTime() - start) / 86_400_000));
  const expected = TARGET * dayFrac;
  return {
    issued: issuedToday,
    target: TARGET,
    limit: LIMIT,
    day: dayKey,
    progress: TARGET > 0 ? issuedToday / TARGET : 1,
    behindPace: issuedToday < expected - 1,
    atLimit: issuedToday >= LIMIT,
  };
}

/** Risk-state fields for ACIE strategy daily pacing. */
export function dailyVolumeRiskFields(): {
  dailyEntriesUsed: number;
  dailyEntriesLimit: number;
} {
  const v = getDailySignalVolume();
  return {
    dailyEntriesUsed: v.issued,
    dailyEntriesLimit: v.limit,
  };
}

/** Test helper */
export function _resetDailySignalVolumeForTests(): void {
  dayKey = "";
  issuedToday = 0;
}
