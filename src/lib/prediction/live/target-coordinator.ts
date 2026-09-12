/**
 * Priority-ordered ownership for target game N+1 prediction.
 *
 * Architecture (source of truth) — directive 2026-09-12 PR-primary:
 *   PR(N) PRIMARY  → reserve/claim N+1 at betting-open (~7s before BG)
 *   BG(N) CONFIRM  → reconciliation only when PR already owns; primary
 *                    trigger only if PR missed (fallback within primary tier)
 *   ED(N) FALLBACK → only when primary (PR/BG) is absent / failed / recoverable
 *   Poll RECOVERY  → last resort only
 *
 * Priority: PR = BG (primary tier) > ED > RECOVERY
 * First primary arrival wins; ED never steals primary ownership.
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
  | "RESERVED_PR"
  | "PR_RUNNING"
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
        | "pr_reserved"
        | "pr_running"
        | "pr_owned"
        | "priority_blocked"
        | "no_bet_terminal";
      owner?: string;
      state?: OwnershipState;
      /** True when the completed claim was an EVALUATED NO_BET decision. */
      noBet?: boolean;
      /** True when blocked by primary-tier owner (PR or BG). */
      blockedByBg?: boolean;
    };

type PrimarySource = "PR" | "BG";
type EntrySource = PrimarySource | "ED" | "RECOVERY";

type Entry = {
  owner: string;
  source: EntrySource;
  claimedAt: number;
  state: OwnershipState;
  completed: boolean;
  noBet?: boolean;
};

const claims = new Map<string, Entry>();
const MAX_ENTRIES = 500;

/** PR and BG share the primary tier (priority 3). ED=2, RECOVERY=1. */
const SOURCE_PRIORITY: Record<EntrySource, number> = {
  PR: 3,
  BG: 3,
  ED: 2,
  RECOVERY: 1,
};

function parseSource(owner: string): EntrySource {
  if (owner.startsWith("pr:")) return "PR";
  if (owner.startsWith("bg:")) return "BG";
  if (owner.startsWith("poll:") || owner.startsWith("recovery:")) return "RECOVERY";
  return "ED";
}

function isPrimarySource(source: EntrySource): source is PrimarySource {
  return source === "PR" || source === "BG";
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
 * Immediate PR reservation for target N+1. Call at PR (betting-open) receipt
 * BEFORE any await. Primary path — fires ~7s before BG.
 */
export function reserveTargetForPr(
  targetGameId: string,
  sourceGameId: string,
): ClaimResult {
  return reservePrimary(targetGameId, `pr:${sourceGameId}`, "PR", "RESERVED_PR");
}

/**
 * Immediate BG reservation for target N+1. Call at BG receipt BEFORE any
 * await. Used when PR missed; no-op (duplicate) when PR already reserved.
 */
export function reserveTargetForBg(
  targetGameId: string,
  sourceGameId: string,
): ClaimResult {
  return reservePrimary(targetGameId, `bg:${sourceGameId}`, "BG", "RESERVED_BG");
}

function reservePrimary(
  targetGameId: string,
  owner: string,
  source: PrimarySource,
  reservedState: "RESERVED_PR" | "RESERVED_BG",
): ClaimResult {
  prune();
  const existing = claims.get(targetGameId);
  if (existing) {
    if (existing.completed) {
      return {
        owned: false,
        reason: existing.noBet ? "no_bet_terminal" : "completed",
        owner: existing.owner,
        state: existing.state,
        noBet: existing.noBet,
        blockedByBg: isPrimarySource(existing.source),
      };
    }
    // PASS 8 FIX (e0d2d5b regression): same-owner re-entry (a duplicate
    // PR/BG frame for the SAME source round) is IDEMPOTENT — owned:true,
    // ownership unchanged, never a second claim. e0d2d5b folded this into
    // the double-reserve reject below, so duplicate frames of the owning
    // event returned owned:false. The DB backstop (bare ON CONFLICT DO
    // NOTHING) absorbs any recompute — exactly-once is preserved.
    if (existing.owner === owner) {
      return { owned: true, claimedAt: existing.claimedAt, state: existing.state };
    }
    // A DIFFERENT primary (PR/BG) already holds the target — do not
    // double-reserve. Single authoritative prediction per target: a second
    // primary source confirms/reconciles, never owns.
    if (isPrimarySource(existing.source)) {
      return {
        owned: false,
        reason:
          existing.state === "RESERVED_PR" || existing.state === "RESERVED_BG"
            ? existing.source === "PR"
              ? "pr_reserved"
              : "bg_reserved"
            : existing.source === "PR"
              ? "pr_owned"
              : "bg_owned",
        owner: existing.owner,
        state: existing.state,
        blockedByBg: true,
      };
    }
    // Primary may take over incomplete lower-priority claim.
    if (SOURCE_PRIORITY[source] > SOURCE_PRIORITY[existing.source]) {
      const claimedAt = Date.now();
      claims.set(targetGameId, {
        owner,
        source,
        claimedAt,
        state: reservedState,
        completed: false,
      });
      return { owned: true, claimedAt, state: reservedState };
    }
    return {
      owned: false,
      reason: "priority_blocked",
      owner: existing.owner,
      state: existing.state,
      blockedByBg: isPrimarySource(existing.source),
    };
  }
  const claimedAt = Date.now();
  claims.set(targetGameId, {
    owner,
    source,
    claimedAt,
    state: reservedState,
    completed: false,
  });
  return { owned: true, claimedAt, state: reservedState };
}


export function markBgRunning(targetGameId: string, owner: string): void {
  const e = claims.get(targetGameId);
  if (!e || e.owner !== owner) return;
  if (e.state === "RESERVED_BG") e.state = "BG_RUNNING";
}

export function markPrRunning(targetGameId: string, owner: string): void {
  const e = claims.get(targetGameId);
  if (!e || e.owner !== owner) return;
  if (e.state === "RESERVED_PR") e.state = "PR_RUNNING";
}

export function peekTargetClaim(
  targetGameId: string,
): {
  source: EntrySource;
  state: OwnershipState;
  completed: boolean;
  noBet?: boolean;
} | null {
  const e = claims.get(targetGameId);
  if (!e) return null;
  return {
    source: e.source,
    state: e.state,
    completed: e.completed,
    noBet: e.noBet,
  };
}

/** True when primary (PR or BG) already reserved/owns or target is terminal. */
export function isBgOwnedOrTerminal(targetGameId: string): boolean {
  return isPrimaryOwnedOrTerminal(targetGameId);
}

export function isPrimaryOwnedOrTerminal(targetGameId: string): boolean {
  const e = claims.get(targetGameId);
  if (!e) return false;
  if (e.completed) return true;
  return isPrimarySource(e.source);
}

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
        blockedByBg: isPrimarySource(existing.source),
      };
    }

    // Same owner re-entry (idempotent) — promote RESERVED_* → *_RUNNING
    if (existing.owner === owner) {
      if (existing.state === "RESERVED_BG" && source === "BG") {
        existing.state = "BG_RUNNING";
      }
      if (existing.state === "RESERVED_PR" && source === "PR") {
        existing.state = "PR_RUNNING";
      }
      return { owned: true, claimedAt: existing.claimedAt, state: existing.state };
    }

    // Active primary reservation/running blocks ED and RECOVERY
    if (
      isPrimarySource(existing.source) &&
      (existing.state === "RESERVED_BG" ||
        existing.state === "BG_RUNNING" ||
        existing.state === "RESERVED_PR" ||
        existing.state === "PR_RUNNING" ||
        existing.state === "SIGNAL_READY")
    ) {
      return {
        owned: false,
        reason:
          existing.state === "RESERVED_PR"
            ? "pr_reserved"
            : existing.state === "PR_RUNNING"
              ? "pr_running"
              : existing.state === "RESERVED_BG"
                ? "bg_reserved"
                : existing.state === "BG_RUNNING"
                  ? "bg_running"
                  : existing.source === "PR"
                    ? "pr_owned"
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
        source === "PR"
          ? "PR_RUNNING"
          : source === "BG"
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
      blockedByBg: isPrimarySource(existing.source),
    };
  }

  const claimedAt = Date.now();
  const state: OwnershipState =
    source === "PR"
      ? "PR_RUNNING"
      : source === "BG"
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
  owner: string,
  opts?: { decision?: "NO_BET" | "SIGNAL" },
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
 * Marks recoverable when a primary owner releases without completing.
 */
export function releaseTarget(targetGameId: string, owner?: string): void {
  const e = claims.get(targetGameId);
  if (!e) return;
  if (owner && e.owner !== owner) return;
  if (e.completed) return;
  if (isPrimarySource(e.source)) {
    e.state = "BG_FAILED_RECOVERABLE";
    e.completed = false;
    claims.delete(targetGameId);
    return;
  }
  claims.delete(targetGameId);
}

export function hasCompletedTarget(targetGameId: string): boolean {
  return claims.get(targetGameId)?.completed === true;
}

export function hasActiveOrCompletedClaim(targetGameId: string): boolean {
  return claims.has(targetGameId);
}

export function peekClaim(
  targetGameId: string,
): {
  owner: string;
  completed: boolean;
  claimedAt: number;
  state?: OwnershipState;
  source?: EntrySource;
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
 * ED/Poll eligibility: true only when primary has not reserved/run/completed,
 * or primary explicitly failed recoverably (no entry).
 */
export function isBgBlocking(targetGameId: string): {
  blocked: boolean;
  reason?: string;
  state?: OwnershipState;
  owner?: string;
} {
  const e = claims.get(targetGameId);
  if (!e) return { blocked: false };
  if (!isPrimarySource(e.source)) return { blocked: false };
  if (e.state === "BG_FAILED_RECOVERABLE") return { blocked: false };
  if (
    e.state === "RESERVED_BG" ||
    e.state === "BG_RUNNING" ||
    e.state === "RESERVED_PR" ||
    e.state === "PR_RUNNING" ||
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
