/**
 * Worker authority registry (fix plan Phase 1+2 — fencing).
 *
 * "Locks coordinate workers; fencing prevents stale workers from causing
 * damage." The DB lock decides WHO may run; this module decides whether THIS
 * process is still allowed to mutate authoritative state, and fans out an
 * immediate cancellation cascade when authority is lost.
 *
 * Contract:
 *  - Before boot calls setWorkerAuthority(), the registry is UNINITIALIZED and
 *    isAuthoritative() returns true — tests and non-boot callers are unaffected.
 *  - After setWorkerAuthority(epoch), only markAuthorityLost() /
 *    setWorkerAuthority(null) can flip authority off (e.g. heartbeat confirmed
 *    the lock was taken over).
 *  - onAuthorityLost(fn) registers cascade callbacks (stop dispatcher, poll
 *    worker, sockets). Returns an unsubscribe function.
 */
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("worker-fencing");

let epoch: number | null = null;
let lost = false;
const lossListeners = new Set<() => void>();

/** Called by boot after acquiring the lease (with epoch) or on clean shutdown (null). */
export function setWorkerAuthority(newEpoch: number | null): void {
  epoch = newEpoch;
  if (newEpoch == null) lost = true;
  logger.info(
    { component: "worker-fencing", workerEpoch: newEpoch, authoritative: newEpoch != null },
    "worker authority updated",
  );
}

/**
 * True when this process may mutate authoritative state. Uninitialized
 * registries (tests, non-boot processes) are authoritative by default.
 */
export function isAuthoritative(): boolean {
  return !lost;
}

/** Current fencing epoch (null = uninitialized). Observability only. */
export function getWorkerEpoch(): number | null {
  return epoch;
}

/** Called by the supervisor when lock loss is confirmed. Fires the cascade ONCE. */
export function markAuthorityLost(reason: string): void {
  if (lost) return;
  lost = true;
  logger.error(
    { component: "worker-fencing", workerEpoch: epoch, reason },
    "WORKER_AUTHORITY_LOST: firing cancellation cascade",
  );
  for (const fn of lossListeners) {
    try {
      fn();
    } catch (e) {
      logger.error(
        { component: "worker-fencing", error: String(e) },
        "authority-lost listener failed",
      );
    }
  }
  lossListeners.clear();
}

/** Register a cascade callback. Returns an unsubscribe function. */
export function onAuthorityLost(fn: () => void): () => void {
  if (lost) {
    // Authority already gone — run the callback immediately so late
    // subscribers still stop.
    try {
      fn();
    } catch { /* caller's problem, not the registry's */ }
    return () => undefined;
  }
  lossListeners.add(fn);
  return () => lossListeners.delete(fn);
}

/** Test helper. */
export function _resetFencingForTests(): void {
  epoch = null;
  lost = false;
  lossListeners.clear();
}
