-- Migration 0039: BG-primary / ED-fallback N+1 prediction provenance.
--
-- SEP 11 ARCHITECTURE CHANGE: BG(N) is now the PRIMARY N+1 prediction
-- trigger (prediction generated while round N runs); ED(N) is the fallback;
-- poll stays recovery-only. Every prediction records which trigger created
-- it so the "was N+1 actually generated ahead of its round?" question is
-- answerable from SQL, not log archaeology:
--
--   trigger_event   'BG' | 'ED' | 'POLL'  (NULL = legacy / bg-only path)
--   trigger_round_id the round whose event triggered the prediction
--                    (= source_round_id for BG/ED triggers)
--
-- Timing columns (requested_at = attempt start, generated_at = model
-- completion, target_round_started_at = stamped by BG(N+1) on arrival)
-- already exist; the invariant generated_at < target_round_started_at is
-- what proves ahead-of-time generation.

ALTER TABLE pending_predictions
  ADD COLUMN IF NOT EXISTS trigger_event text,
  ADD COLUMN IF NOT EXISTS trigger_round_id text;

ALTER TABLE pending_predictions
  DROP CONSTRAINT IF EXISTS pending_predictions_trigger_event_check;

ALTER TABLE pending_predictions
  ADD CONSTRAINT pending_predictions_trigger_event_check
  CHECK (trigger_event IS NULL OR trigger_event IN ('BG', 'ED', 'POLL'));

-- Provenance audit: per-trigger production mix over recent predictions.
CREATE INDEX IF NOT EXISTS idx_pending_predictions_trigger_event
  ON pending_predictions (trigger_event, requested_at DESC);
