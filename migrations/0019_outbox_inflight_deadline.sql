-- Migration 0019: Per-row Telegram deadline + INFLIGHT outbox state.
-- Spec: TestingEngine-Complete-Issues-and-Recommendations.md §6.9 + §6.12
--
-- Adds:
--   * `notification_outbox.telegram_deadline_at` — set by the predictor at
--     enqueue time, gated on the target round's `began_at`. The
--     dispatcher's claim query refuses rows whose deadline has passed,
--     so a backed-up dispatcher can never deliver a "predicts the past"
--     Telegram message after the target round has begun.
--   * `status = 'inflight'` is now a first-class state. The claim
--     transaction flips `pending → inflight` atomically; a crash mid-send
--     leaves the row visible to `recoverStale` immediately rather than
--     waiting for the 30s `next_attempt_at` window.
--
-- Backward compatibility:
--   * Existing rows have `telegram_deadline_at = NULL` and the dispatcher's
--     claim query treats NULL as "no deadline" (deliver).
--   * `status = 'pending' | 'delivered' | 'failed' | 'dead_letter'`
--     remain; `inflight` is added.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_name = 'notification_outbox'
       AND column_name = 'telegram_deadline_at'
  ) THEN
    ALTER TABLE notification_outbox
      ADD COLUMN telegram_deadline_at timestamptz;
  END IF;
END $$;

-- Index so the dispatcher's "give me rows whose deadline has not passed"
-- query stays cheap as the backlog grows.
CREATE INDEX IF NOT EXISTS notification_outbox_deadline_idx
  ON notification_outbox (telegram_deadline_at)
  WHERE status = 'pending' AND telegram_deadline_at IS NOT NULL;

-- Loosen the status CHECK to admit 'inflight'. The original migration
-- created the constraint with an inline column CHECK; we drop and
-- recreate it.
ALTER TABLE notification_outbox
  DROP CONSTRAINT IF EXISTS notification_outbox_status_check;

ALTER TABLE notification_outbox
  ADD CONSTRAINT notification_outbox_status_check
  CHECK (status IN ('pending', 'inflight', 'delivered', 'failed', 'dead_letter'));
