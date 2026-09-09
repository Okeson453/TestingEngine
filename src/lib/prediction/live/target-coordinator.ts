/**
 * In-memory single-owner claim for target game N+1 prediction.
 * First claimant (ED preferred) owns model compute; duplicates reconcile only.
 * DB unique constraint remains the durability backstop.
 */
export type ClaimResult =
  | { owned: true; claimedAt: number }
  | { owned: false; reason: "duplicate" | "completed"; owner?: string };

type Entry = {
  owner: string;
  claimedAt: number;
  completed: boolean;
};

const claims = new Map<string, Entry>();
const MAX_ENTRIES = 500;

function prune(): void {
  if (claims.size <= MAX_ENTRIES) return;
  const cutoff = Date.now() - 30 * 60_000;
  for (const [k, v] of claims) {
    if (v.completed || v.claimedAt < cutoff) claims.delete(k);
  }
  if (claims.size > MAX_ENTRIES) {
    const keys = [...claims.keys()].slice(0, claims.size - MAX_ENTRIES);
    for (const k of keys) claims.delete(k);
  }
}

export function claimTarget(targetGameId: string, owner: string): ClaimResult {
  prune();
  const existing = claims.get(targetGameId);
  if (existing) {
    if (existing.completed) return { owned: false, reason: "completed", owner: existing.owner };
    if (existing.owner === owner) return { owned: true, claimedAt: existing.claimedAt };
    return { owned: false, reason: "duplicate", owner: existing.owner };
  }
  const claimedAt = Date.now();
  claims.set(targetGameId, { owner, claimedAt, completed: false });
  return { owned: true, claimedAt };
}

export function completeTarget(targetGameId: string, owner?: string): void {
  const e = claims.get(targetGameId);
  if (!e) return;
  if (owner && e.owner !== owner) return;
  e.completed = true;
}

export function releaseTarget(targetGameId: string, owner?: string): void {
  const e = claims.get(targetGameId);
  if (!e) return;
  if (owner && e.owner !== owner) return;
  if (!e.completed) claims.delete(targetGameId);
}

export function hasCompletedTarget(targetGameId: string): boolean {
  return claims.get(targetGameId)?.completed === true;
}

/** Test helper */
export function _resetTargetCoordinatorForTests(): void {
  claims.clear();
}
