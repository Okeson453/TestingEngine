-- Migration 0014: Expand live_event_log.event_kind to include PREDICT and VALIDATE.
-- Spec: TestingEngine_Deep_Diagnosis.md §3.9
-- Previous CHECK only allowed BG/ED/PG/POLL_RECONCILE/BOOT_BACKFILL, which
-- caused PREDICT observability rows from onGameEndPredict to fail silently.

ALTER TABLE live_event_log
  DROP CONSTRAINT IF EXISTS live_event_log_event_kind_check;

ALTER TABLE live_event_log
  ADD CONSTRAINT live_event_log_event_kind_check
  CHECK (event_kind IN (
    'BG', 'ED', 'PG', 'POLL_RECONCILE', 'BOOT_BACKFILL', 'PREDICT', 'VALIDATE'
  ));
