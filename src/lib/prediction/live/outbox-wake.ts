/**
 * Outbox wake channel — process-level EventEmitter.
 *
 * After a prediction or validation row is committed to `notification_outbox`,
 * producers call `notifyOutbox()` so the dispatcher can drain immediately
 * instead of waiting up to TICK_MS (default 25 ms) for the next timer.
 *
 * Extracted to its own module to avoid a circular import between
 * predictor / validator and notification-worker.
 */
import { EventEmitter } from "node:events";

const bus = new EventEmitter();
// Unlimited listeners: many concurrent ED handlers may wake the same drain.
bus.setMaxListeners(0);

const WAKE_EVENT = "outbox-wake";

/** Signal that at least one new outbox row is ready to claim. */
export function notifyOutbox(): void {
  bus.emit(WAKE_EVENT);
}

/**
 * Resolve on the next wake (or never, until aborted via stop).
 * Used by the dispatcher to race against the periodic setTimeout.
 */
export function waitForOutboxWake(): Promise<void> {
  return new Promise((resolve) => {
    bus.once(WAKE_EVENT, () => resolve());
  });
}

/** Test helper. */
export function _resetOutboxWakeForTests(): void {
  bus.removeAllListeners(WAKE_EVENT);
}
