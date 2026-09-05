-- P0/P1 production hardening:
-- 1. Allow status='inflight' on notification_outbox (explicit claim state)
-- 2. Add telegram_deadline_at (per-row delivery deadline)
-- 3. Ensure delivered_at column exists
-- 4. DB-level temporal CHECK on pending_predictions

-- --- notification_outbox status CHECK expansion ---
-- Drop and recreate the status check to include 'inflight'.
DO $$
DECLARE
  conname text;
BEGIN
  SELECT c.conname INTO conname
  FROM pg_constraint c
  JOIN pg_class t ON c.conrelid = t.oid
  WHERE t.relname = 'notification_outbox'
    AND c.contype = 'c'
    AND pg_get_constraintdef(c.oid) ILIKE '%status%';
  IF conname IS NOT NULL THEN
    EXECUTE format('ALTER TABLE notification_outbox DROP CONSTRAINT %I', conname);
  END IF;
END $$;

ALTER TABLE notification_outbox
  ADD COLUMN IF NOT EXISTS telegram_deadline_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMPTZ;

-- Re-apply status CHECK including inflight
ALTER TABLE notification_outbox
  ADD CONSTRAINT notification_outbox_status_check
  CHECK (status IN ('pending', 'inflight', 'delivered', 'failed', 'dead_letter'));

CREATE INDEX IF NOT EXISTS notification_outbox_deadline_idx
  ON notification_outbox (telegram_deadline_at)
  WHERE status = 'pending' AND telegram_deadline_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS notification_outbox_inflight_idx
  ON notification_outbox (status, updated_at)
  WHERE status = 'inflight';

-- --- temporal invariant on pending_predictions ---
-- Only add if no existing violating rows (defensive).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pending_predictions
    WHERE target_round_started_at IS NOT NULL
      AND generated_at >= target_round_started_at + interval '1 second'
  ) THEN
    BEGIN
      ALTER TABLE pending_predictions
        ADD CONSTRAINT pending_predictions_temporal_check
        CHECK (
          target_round_started_at IS NULL
          OR generated_at < target_round_started_at + interval '1 second'
        );
    EXCEPTION WHEN duplicate_object THEN
      NULL; -- already present
    END;
  ELSE
    RAISE NOTICE 'Skipping temporal CHECK: existing violating rows present';
  END IF;
END $$;
