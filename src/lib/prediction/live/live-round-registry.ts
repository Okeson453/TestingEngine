/**
 * Zero-RTT in-memory live-round registry.
 *
 * WHY THIS EXISTS (sep 11 temporal-gate investigation): the dispatcher's
 * pre-send authorization reads `live_round_state` / `crash_rounds`, but both
 * tables LAG the real round phase:
 *   - `crash_rounds(N)` is written by edHandler(N)'s DETACHED persist, which
 *     only runs after attemptNPlusOnePrediction(N+1) completes (1-3s after the
 *     crash).
 *   - `live_round_state(N)` is written by bgHandler — only when/if the BG
 *     event arrives (WS flakiness is documented).
 * In that lag window a late signal passes the DB gate and delivers into a
 * round that has ALREADY crashed.
 *
 * FIX: the ED/BG handlers write this registry SYNCHRONOUSLY at entry (before
 * any await), so the dispatcher consults the true round phase with zero DB
 * round trips. Worker fencing guarantees the handlers and the dispatcher run
 * in the same process — this is same-process state by design, not a cache of
 * the DB.
 *
 * NOTE: only a real BG (round start) writes `startedAt`. The `pr`
 * (prepare/betting-open) event deliberately does NOT — treating betting-open
 * as "round started" is the defect that killed valid signals (see
 * game-event-handlers.ts pr/bg decoupling).
 */

interface RoundPhase {
  startedAt?: number;
  endedAt?: number;
}

const registry = new Map<string, RoundPhase>();

/** Entries older than this are pruned; predictions die at their 5s deadline
 * long before this horizon, so retention only bounds memory. */
const RETENTION_MS = 10 * 60_000;
const MAX_ENTRIES = 512;

function prune(now: number): void {
  for (const [gameId, phase] of registry) {
    const latest = Math.max(phase.startedAt ?? 0, phase.endedAt ?? 0);
    if (now - latest > RETENTION_MS) registry.delete(gameId);
  }
  // Insertion-order eviction fallback for pathological event floods.
  while (registry.size > MAX_ENTRIES) {
    const oldest = registry.keys().next().value;
    if (oldest === undefined) break;
    registry.delete(oldest);
  }
}

/** Round N actually STARTED (real BG event). Called synchronously at
 * bgHandler entry — MUST NOT be called from pr (betting-open) paths. */
export function noteRoundStarted(gameId: string, startedAt: number = Date.now()): void {
  const phase = registry.get(gameId) ?? {};
  phase.startedAt = startedAt;
  registry.set(gameId, phase);
  prune(startedAt);
}

/** Round N ENDED (crash). Called synchronously at edHandler entry — before
 * any await — so the registry never lags the crash. */
export function noteRoundEnded(gameId: string, endedAt: number = Date.now()): void {
  const phase = registry.get(gameId) ?? {};
  phase.endedAt = endedAt;
  registry.set(gameId, phase);
  prune(endedAt);
}

/** True when the dispatcher must refuse a signal for this target: the round
 * has started (betting closed) or crashed. Undefined phase = no knowledge,
 * fall through to the DB gates (fail-open there is by design — the DB gates
 * remain the contract; this is an additive zero-RTT hard gate). */
export function isTargetPastBettingWindow(gameId: string): boolean {
  const phase = registry.get(gameId);
  return phase !== undefined && (phase.startedAt !== undefined || phase.endedAt !== undefined);
}

/** Test/observability hook. */
export function getRoundPhase(gameId: string): RoundPhase | undefined {
  const phase = registry.get(gameId);
  return phase ? { ...phase } : undefined;
}

/** Test hook — clear all state. */
export function resetRoundRegistry(): void {
  registry.clear();
}
