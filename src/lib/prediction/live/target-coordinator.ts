/**
 * Priority-ordered ownership for target game N+1 prediction.
 *
 * Architecture (source of truth):
 *   BG(N) PRIMARY  → reserve/claim N+1 immediately on BG receipt
 *   ED(N) FALLBACK → only when BG is genuinely absent / failed / recoverable
 *   Poll RECOVERY  → last resort only
 *
 * Priority: BG > ED > RECOVERY
 * First-arrival does NOT win when a higher-priority source is active.
 * NO_BET is terminal and never released for recomputation.
 *
 * Layers:
 *   1. This coordinator (process-local fast path with explicit priority)
 *   2. pending_predictions unique constraint (distributed durability backstop)
 *
 * DEPLOYMENT: production runs one worker replica. Multi-replica would need
 * Postgres advisory locks for the reservation step; the DB unique constraint
 * still prevents corruption.
 */

export type OwnershipState =
  | "RESERVED_BG"
  | "BG_RUNNING"
  | "SIGNAL_READY"
  | "NO_BET"
  | "BG_FAILED_RECOVERABLE"
  | "ED_RUNNING"
  | "RECOVERY_RUNNING"
  | "COMPLETED";

export type ClaimResult =
  | { owned: true; claimedAt: number; state: OwnershipState }
  | {
      owned: false;
      reason:
        | "duplicate"
        | "completed"
        | "bg_reserved"
        | "bg_running"
        | "bg_owned"
        | "priority_blocked"
        | "no_bet_terminal";
      owner?: string;
      state?: OwnershipState;
      /** True when the completed claim was an EVALUATED NO_BET decision. */
      noBet?: boolean;
      blockedByBg?: boolean;
    };

type Entry = {
  owner: string;
  source: "BG" | "ED" | "RECOVERY";
  claimedAt: number;
  state: OwnershipState;
  completed: boolean;
  noBet?: boolean;
};

const claims = new Map<string, Entry>();
const MAX_ENTRIES = 500;

const SOURCE_PRIORITY: Record<"BG" | "ED" | "RECOVERY", number> = {
  BG: 3,
  ED: 2,
  RECOVERY: 1,
};

function parseSource(owner: string): "BG" | "ED" | "RECOVERY" {
  if (owner.startsWith("bg:")) return "BG";
  if (owner.startsWith("poll:") || owner.startsWith("recovery:")) return "RECOVERY";
  return "ED";
}

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

/**
 * Immediate BG reservation for target N+1. Call at BG receipt BEFORE any
 * await (reconcile, prediction). Prevents ED from claiming while BG is in
 * the reconciliation window.
 */
export function reserveTargetForBg(
  targetGameId: string,
  sourceGameId: string,
): ClaimResult {
  prune();
  const owner = `bg:${sourceGameId}`;
  const existing = claims.get(targetGameId);

  if (existing) {
    if (existing.completed) {
      return {
        owned: false,
        reason: existing.noBet ? "no_bet_terminal" : "completed",
        owner: existing.owner,
        state: existing.state,
        noBet: existing.noBet,
        blockedByBg: existing.source === "BG",
      };
    }
    if (existing.owner === owner || existing.source === "BG") {
      // Same BG or already reserved by BG — keep ownership, promote to RUNNING if reserved
      if (existing.state === "RESERVED_BG") {
        existing.state = "BG_RUNNING";
      }
      return { owned: true, claimedAt: existing.claimedAt, state: existing.state };
    }
    // Lower priority (ED/RECOVERY) holds incomplete claim — BG takes over
    if (SOURCE_PRIORITY.BG > SOURCE_PRIORITY[existing.source]) {
      const claimedAt = Date.now();
      claims.set(targetGameId, {
        owner,
        source: "BG",
        claimedAt,
        state: "RESERVED_BG",
        completed: false,
      });
      return { owned: true, claimedAt, state: "RESERVED_BG" };
    }
    return {
      owned: false,
      reason: "duplicate",
      owner: existing.owner,
      state: existing.state,
    };
  }

  const claimedAt = Date.now();
  claims.set(targetGameId, {
    owner,
    source: "BG",
    claimedAt,
    state: "RESERVED_BG",
    completed: false,
  });
  return { owned: true, claimedAt, state: "RESERVED_BG" };
}

/**
 * Promote RESERVED_BG → BG_RUNNING when prediction compute starts.
 */
export function markBgRunning(targetGameId: string, owner: string): void {
  const e = claims.get(targetGameId);
  if (!e || e.owner !== owner) return;
  if (e.state === "RESERVED_BG") e.state = "BG_RUNNING";
}

/**
 * Unified claim used by predictor / attempt path.
 * Respects priority: BG can claim over incomplete ED/RECOVERY;
 * ED/RECOVERY cannot steal from active BG reservation/running.
 */
export function claimTarget(targetGameId: string, owner: string): ClaimResult {
  prune();
  const source = parseSource(owner);
  const existing = claims.get(targetGameId);

  if (existing) {
    if (existing.completed) {
      return {
        owned: false,
        reason: existing.noBet ? "no_bet_terminal" : "completed",
        owner: existing.owner,
        state: existing.state,
        noBet: existing.noBet,
        blockedByBg: existing.source === "BG",
      };
    }

    // Same owner re-entry (idempotent)
    if (existing.owner === owner) {
      if (existing.state === "RESERVED_BG" && source === "BG") {
        existing.state = "BG_RUNNING";
      }
      return { owned: true, claimedAt: existing.claimedAt, state: existing.state };
    }

    // Active BG reservation/running blocks ED and RECOVERY
    if (
      existing.source === "BG" &&
      (existing.state === "RESERVED_BG" ||
        existing.state === "BG_RUNNING" ||
        existing.state === "SIGNAL_READY")
    ) {
      return {
        owned: false,
        reason:
          existing.state === "RESERVED_BG"
            ? "bg_reserved"
            : existing.state === "BG_RUNNING"
              ? "bg_running"
              : "bg_owned",
        owner: existing.owner,
        state: existing.state,
        blockedByBg: true,
      };
    }

    // Higher priority may take over incomplete lower-priority claim
    if (SOURCE_PRIORITY[source] > SOURCE_PRIORITY[existing.source]) {
      const claimedAt = Date.now();
      const state: OwnershipState =
        source === "BG"
          ? "BG_RUNNING"
          : source === "ED"
            ? "ED_RUNNING"
            : "RECOVERY_RUNNING";
      claims.set(targetGameId, {
        owner,
        source,
        claimedAt,
        state,
        completed: false,
      });
      return { owned: true, claimedAt, state };
    }

    return {
      owned: false,
      reason: "priority_blocked",
      owner: existing.owner,
      state: existing.state,
      blockedByBg: existing.source === "BG",
    };
  }

  const claimedAt = Date.now();
  const state: OwnershipState =
    source === "BG"
      ? "BG_RUNNING"
      : source === "ED"
        ? "ED_RUNNING"
        : "RECOVERY_RUNNING";
  claims.set(targetGameId, {
    owner,
    source,
    claimedAt,
    state,
    completed: false,
  });
  return { owned: true, claimedAt, state };
}

export function completeTarget(
  targetGameId: string,
  owner?: string,
  opts?: { decision?: "PREDICTED" | "NO_BET" },
): void {
  const e = claims.get(targetGameId);
  if (!e) return;
  if (owner && e.owner !== owner) return;
  e.completed = true;
  if (opts?.decision === "NO_BET") {
    e.noBet = true;
    e.state = "NO_BET";
  } else {
    e.state = "SIGNAL_READY";
  }
}

/**
 * Release only recoverable failures. NO_BET / SIGNAL_READY stay closed.
 * Marks BG_FAILED_RECOVERABLE when a BG owner releases without completing.
 */
export function releaseTarget(targetGameId: string, owner?: string): void {
  const e = claims.get(targetGameId);
  if (!e) return;
  if (owner && e.owner !== owner) return;
  if (e.completed) return; // never reopen terminal states
  if (e.source === "BG") {
    // Leave a recoverable marker briefly so ED can observe and take over
    e.state = "BG_FAILED_RECOVERABLE";
    e.completed = false;
    // Delete so ED/RECOVERY can claim immediately
    claims.delete(targetGameId);
    return;
  }
  claims.delete(targetGameId);
}

export function hasCompletedTarget(targetGameId: string): boolean {
  return claims.get(targetGameId)?.completed === true;
}

/** True when any owner currently holds (or completed) a claim for target. */
export function hasActiveOrCompletedClaim(targetGameId: string): boolean {
  return claims.has(targetGameId);
}

/** Peek claim without mutating — for poll recovery gates and ED eligibility. */
export function peekClaim(
  targetGameId: string,
): {
  owner: string;
  completed: boolean;
  claimedAt: number;
  state?: OwnershipState;
  source?: "BG" | "ED" | "RECOVERY";
  noBet?: boolean;
} | null {
  const e = claims.get(targetGameId);
  if (!e) return null;
  return {
    owner: e.owner,
    completed: e.completed,
    claimedAt: e.claimedAt,
    state: e.state,
    source: e.source,
    noBet: e.noBet,
  };
}

/**
 * ED/Poll eligibility: true only when BG has not reserved/run/completed,
 * or BG explicitly failed recoverably (no entry).
 */
export function isBgBlocking(targetGameId: string): {
  blocked: boolean;
  reason?: string;
  state?: OwnershipState;
  owner?: string;
} {
  const e = claims.get(targetGameId);
  if (!e) return { blocked: false };
  if (e.source !== "BG") return { blocked: false };
  if (e.state === "BG_FAILED_RECOVERABLE") return { blocked: false };
  if (
    e.state === "RESERVED_BG" ||
    e.state === "BG_RUNNING" ||
    e.state === "SIGNAL_READY" ||
    e.state === "NO_BET" ||
    e.completed
  ) {
    return {
      blocked: true,
      reason: e.state,
      state: e.state,
      owner: e.owner,
    };
  }
  return { blocked: false };
}

/** Test helper */
export function _resetTargetCoordinatorForTests(): void {
  claims.clear();
}
