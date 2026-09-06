/**
 * Phase 9 — Bounded recent-round in-memory cache.
 *
 * Stores only hot operational state (game IDs, timestamps, crash values).
 * - Bounded size with deterministic LRU eviction
 * - Duplicate detection before DB queries
 * - Restart-safe: empty on boot; PostgreSQL remains source of truth
 * - No authority over durable state
 */

export const featureHotCache = new Map<string, unknown>();
export const predictionHotCache = new Map<string, unknown>();

export type RecentRoundEntry = {
  gameId: string;
  multiplier: number;
  crashedAt: string;
  seenAt: number;
};

const DEFAULT_MAX = Math.max(
  50,
  Math.min(500, Number(process.env.RECENT_ROUND_CACHE_MAX ?? 200) || 200),
);

/**
 * LRU-style recent-round cache keyed by game_id.
 * Used by ingest/poll to skip already-known rounds without a DB round-trip.
 */
export class RecentRoundCache {
  private readonly map = new Map<string, RecentRoundEntry>();
  private readonly maxSize: number;

  constructor(maxSize = DEFAULT_MAX) {
    this.maxSize = Math.max(16, maxSize);
  }

  has(gameId: string): boolean {
    return this.map.has(gameId);
  }

  get(gameId: string): RecentRoundEntry | undefined {
    const e = this.map.get(gameId);
    if (!e) return undefined;
    // Touch for LRU: re-insert moves to end in Map insertion order
    this.map.delete(gameId);
    this.map.set(gameId, e);
    return e;
  }

  set(entry: Omit<RecentRoundEntry, "seenAt"> & { seenAt?: number }): void {
    const gameId = entry.gameId;
    if (this.map.has(gameId)) this.map.delete(gameId);
    this.map.set(gameId, {
      gameId,
      multiplier: entry.multiplier,
      crashedAt: entry.crashedAt,
      seenAt: entry.seenAt ?? Date.now(),
    });
    this.evictIfNeeded();
  }

  /** Record multiple rounds; returns how many were newly added to cache. */
  addMany(
    rounds: Array<{ gameId: string; multiplier: number; crashedAt: string | Date }>,
  ): number {
    let added = 0;
    for (const r of rounds) {
      if (this.map.has(r.gameId)) continue;
      const crashedAt =
        r.crashedAt instanceof Date ? r.crashedAt.toISOString() : String(r.crashedAt);
      this.set({ gameId: r.gameId, multiplier: r.multiplier, crashedAt });
      added += 1;
    }
    return added;
  }

  /** Filter out gameIds already in cache (still present in DB source of truth). */
  filterUnknown<T extends { gameId: string }>(rounds: T[]): T[] {
    return rounds.filter((r) => !this.map.has(r.gameId));
  }

  size(): number {
    return this.map.size;
  }

  clear(): void {
    this.map.clear();
  }

  private evictIfNeeded(): void {
    while (this.map.size > this.maxSize) {
      const oldest = this.map.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.map.delete(oldest);
    }
  }
}

/** Process-wide singleton used by ingest + poll-worker. */
export const globalRecentRoundCache = new RecentRoundCache();
