-- Phase 10: allow durable ED receipt markers and prediction skip reasons.
ALTER TABLE live_event_log
  DROP CONSTRAINT IF EXISTS live_event_log_event_kind_check;

ALTER TABLE live_event_log
  ADD CONSTRAINT live_event_log_event_kind_check
  CHECK (event_kind IN (
    'BG', 'ED', 'ED_RECEIVED', 'PG', 'POLL_RECONCILE', 'BOOT_BACKFILL',
    'PREDICT', 'VALIDATE'
  ));
