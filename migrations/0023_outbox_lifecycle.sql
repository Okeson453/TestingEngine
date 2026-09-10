-- Migration 0023: Outbox lifecycle timestamps (P0 observability)
-- Traces every notification row through the dispatch pipeline:
--   created_at (INSERT) -> dispatch_claimed_at (CLAIM) -> send_started_at (SEND)
--   -> telegram_accepted_at (ACCEPT) -> delivered_at (COMPLETE)
-- Durations (queue_wait_ms, dispatch_ms, telegram_send_ms, total_delivery_ms)
-- are computed by the dispatcher and logged as OUTBOX_DISPATCH per row.

ALTER TABLE notification_outbox
  ADD COLUMN IF NOT EXISTS dispatch_claimed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS send_started_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS telegram_accepted_at TIMESTAMPTZ;

-- Lifecycle forensics: age at claim, and how long rows sit in send.
CREATE INDEX IF NOT EXISTS notification_outbox_claimed_idx
  ON notification_outbox (dispatch_claimed_at)
  WHERE status = 'inflight';

COMMENT ON COLUMN notification_outbox.dispatch_claimed_at IS 'When the dispatcher claimed this row (status pending -> inflight)';
COMMENT ON COLUMN notification_outbox.send_started_at IS 'When the Telegram send request was started for the latest attempt';
COMMENT ON COLUMN notification_outbox.telegram_accepted_at IS 'When Telegram accepted the message (2xx) for the latest attempt';
