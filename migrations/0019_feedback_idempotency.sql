-- Durable feedback idempotency (Phase 2 remediation).
-- Mark when closed-loop learning has been applied for a validation so
-- restarts / duplicate ed / poll recovery cannot re-apply the same outcome.

ALTER TABLE prediction_validations
  ADD COLUMN IF NOT EXISTS feedback_applied_at timestamptz NULL;

CREATE INDEX IF NOT EXISTS prediction_validations_feedback_pending_idx
  ON prediction_validations (prediction_id)
  WHERE feedback_applied_at IS NULL;

COMMENT ON COLUMN prediction_validations.feedback_applied_at IS
  'Set when processResolvedPredictionFeedback successfully claimed this validation. NULL means feedback not yet applied.';
