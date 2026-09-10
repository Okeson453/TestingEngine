/**
 * Outbox wake channel — process-level, stateful, coalescing.
 *
 * After a prediction or validation row is committed to `notification_outbox`,
 * producers call `notifyOutbox()` so the dispatcher can drain immediately
 * instead of waiting the full tick for the next timer.
 *
 * Fix plan Phase 4: the previous implementation registered a fresh `once`
 * EventEmitter listener per scheduling cycle. If the timer won the race, the
 * listener leaked — and all leaked listeners later fired, producing
 * overlapping drain ticks. This version keeps AT MOST ONE waiter and
 * coalesces wake events:
 *
 *   - notifyOutbox() with no waiter  -> wakePending = true (next wait returns
 *     immediately)
 *   - notifyOutbox() with a waiter   -> that waiter resolves (exactly one)
 *   - multiple notifyOutbox() bursts -> single drain, never N overlapping
 *
 * The dispatcher is the only waiter; the single-waiter contract is enforced
 * by construction (a second wait would replace the first — there is only one
 * drain loop per worker by invariant).
 */

let wakePending = false;
const waiters = new Set<() => void>();

/** Signal that at least one new outbox row is ready to claim. */
export function notifyOutbox(): void {
  if (waiters.size > 0) {
    wakePending = false;
    for (const w of [...waiters]) w();
  } else {
    // No waiter right now — LATCH the wake so the next wait returns
    // immediately instead of sleeping a full tick. Losing this wake is
    // exactly the dropped-notification bug the channel exists to prevent.
    wakePending = true;
  }
}

/**
 * Wait for a wake, a timeout, or both — whichever first. With no timeout,
 * waits until the next wake. Never throws. Multiple concurrent waiters are
 * supported; a wake resolves all of them (coalesced — one event, one resolve
 * per waiter).
 */
export function waitForOutboxWake(timeoutMs?: number): Promise<void> {
  if (wakePending) {
    wakePending = false;
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    let timer: ReturnType<typeof setTimeout> | null = null;
    let settled = false;
    const settle = (): void => {
      if (settled) return;
      settled = true;
      waiters.delete(settle);
      if (timer) clearTimeout(timer);
      resolve();
    };
    waiters.add(settle);
    if (timeoutMs != null) {
      timer = setTimeout(settle, timeoutMs);
      timer.unref?.();
    }
  });
}

/** Test helper. */
export function _resetOutboxWakeForTests(): void {
  wakePending = false;
  for (const w of [...waiters]) w();
  waiters.clear();
}
