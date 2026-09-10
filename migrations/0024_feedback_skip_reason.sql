-- 0024: feedback skip reason.
--
-- The TEMPORALLY_INVALID branch in live/validator.ts intentionally never
-- applies closed-loop feedback (a prediction generated after its target round
-- started must never train the models). Before this column, those rows kept
-- feedback_applied_at IS NULL forever and tripped the one_feedback_per_validation
-- invariant (invariants.ts) — a false positive indistinguishable from genuinely
-- stuck feedback.
--
-- Rows with feedback_skip_reason set are excluded from the invariant and from
-- the stuck-feedback recovery sweep. NULL = feedback must still be applied.

ALTER TABLE prediction_validations
  ADD COLUMN IF NOT EXISTS feedback_skip_reason TEXT;

COMMENT ON COLUMN prediction_validations.feedback_skip_reason IS
  'Set when closed-loop feedback was intentionally skipped (e.g. TEMPORALLY_INVALID). NULL means feedback is still owed.';
