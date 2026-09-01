export const HIGH_THRESHOLD = 2;
export const MOON_THRESHOLD = 10;
export const SOURCE_URL = "https://bc.game/game/crash";
export const HISTORY_LIMIT = 200;
export const STATS_LIMIT = 1_000_000; // unlimited — load all rounds for stats

export type CrashRound = {
  gameId: string;
  multiplier: number;
  hash: string | null;
  salt: string | null;
  beganAt: string | null;
  crashedAt: string;
};

export type RangeBucket = {
  key: string;
  label: string;
  min: number;
  max: number | null;
  count: number;
  pct: number;
};

export type StreakSnapshot = {
  currentKind: "low" | "high" | "none";
  currentCount: number;
  maxLow: number;
  maxHigh: number;
  threshold: number;
};

export type CrashStats = {
  count: number;
  average: number | null;
  median: number | null;
  highest: number | null;
  lowest: number | null;
};

export type FeedStatus = {
  ok: boolean;
  lastSyncAt: string;
  fetched: number;
  inserted: number;
  error: string | null;
  onlinePlayers: number | null;
};

export type CrashDaily = {
  date: string; // YYYY-MM-DD
  totalRounds: number;
  avgMultiplier: number | null;
  medianMultiplier: number | null;
  highestMultiplier: number | null;
  lowestMultiplier: number | null;
  lowCount: number;
  highCount: number;
  moonCount: number;
  updatedAt: string;
};

export type DailyPayload = {
  days: CrashDaily[];
  overall: CrashStats;
};

export type DashboardPayload = {
  latest: CrashRound | null;
  rounds: CrashRound[];
  chart: CrashRound[];
  stats: CrashStats;
  ranges: RangeBucket[];
  streaks: StreakSnapshot;
  feed: FeedStatus;
};

export const RANGE_DEFS: Omit<RangeBucket, "count" | "pct">[] = [
  { key: "1-1.2", label: "1.00–1.20×", min: 1, max: 1.2 },
  { key: "1.2-1.5", label: "1.20–1.50×", min: 1.2, max: 1.5 },
  { key: "1.5-2", label: "1.50–2.00×", min: 1.5, max: 2 },
  { key: "2-3", label: "2.00–3.00×", min: 2, max: 3 },
  { key: "3-5", label: "3.00–5.00×", min: 3, max: 5 },
  { key: "5-10", label: "5.00–10.00×", min: 5, max: 10 },
  { key: "10-50", label: "10.00–50.00×", min: 10, max: 50 },
  { key: "50+", label: "50.00×+", min: 50, max: null },
];
