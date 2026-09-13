/**
 * Process-local set of target_game_ids with an undelivered prediction outbox
 * row. Used to skip the BG temporal-kill DB round-trip when nothing can be
 * killed (production measured kill_query_ms≈177–222 with killed=0 every time).
 *
 * DB remains the source of truth for durability; this is a hot-path filter only.
 * Misses (restart before rehydrate) fall through to the existing SQL kill —
 * never skip kill when the set is unknown/empty after boot until first persist.
 */

const undelivered = new Set<string>();
let bootHydrated = false;

export function notePredictionOutboxEnqueued(targetGameId: string): void {
  if (!targetGameId) return;
  undelivered.add(String(targetGameId));
}

export function notePredictionOutboxCleared(targetGameId: string): void {
  if (!targetGameId) return;
  undelivered.delete(String(targetGameId));
}

/** True when this process knows of an undelivered prediction for the target. */
export function hasUndeliveredPredictionOutbox(targetGameId: string): boolean {
  return undelivered.has(String(targetGameId));
}

/**
 * After restart the set is empty — force DB kill until at least one enqueue
 * or explicit hydrate. Avoids missing kills for pre-restart pending rows.
 */
export function shouldSkipBgTemporalKillDb(targetGameId: string): boolean {
  if (!bootHydrated && undelivered.size === 0) return false;
  return !undelivered.has(String(targetGameId));
}

export function markOutboxPendingTargetsHydrated(): void {
  bootHydrated = true;
}

/** Test helper. */
export function resetOutboxPendingTargetsForTests(): void {
  undelivered.clear();
  bootHydrated = false;
}

export function undeliveredPredictionOutboxCount(): number {
  return undelivered.size;
}
