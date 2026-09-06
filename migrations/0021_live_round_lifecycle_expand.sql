-- Phase 11: Expand live_round_state lifecycle for explicit prediction/feedback stages
-- and recovery states. Transitions remain monotonic / idempotent in application code.

ALTER TABLE live_round_state
  DROP CONSTRAINT IF EXISTS live_round_state_lifecycle_check;

ALTER TABLE live_round_state
  ADD CONSTRAINT live_round_state_lifecycle_check
  CHECK (lifecycle IN (
    'DISCOVERED',
    'STARTED',
    'RUNNING',
    'ENDED',
    'PREDICTION_RESOLVED',
    'FEEDBACK_APPLIED',
    'NEXT_PREDICTION_GENERATED',
    'RECONCILED',
    'MISSING_END',
    'LATE_END',
    'ORPHANED',
    'RECONCILING',
    'FAILED'
  ));

COMMENT ON COLUMN live_round_state.lifecycle IS
  'Explicit round state machine (Phase 11). Socket.IO, poll, validation, and recovery converge here.';
