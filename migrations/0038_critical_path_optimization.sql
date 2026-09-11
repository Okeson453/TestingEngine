-- Migration 0038: Critical path query optimization

-- 1. Add index for crash_rounds.began_at (temporal queries)
CREATE INDEX IF NOT EXISTS crash_rounds_began_at_idx ON crash_rounds (began_at DESC);

-- 2. Add composite index for live_round_state
CREATE INDEX IF NOT EXISTS live_round_state_game_lifecycle_idx 
ON live_round_state (game_id, lifecycle) WHERE lifecycle IN ('STARTED', 'RUNNING', 'ENDED', 'RECONCILED');

-- 3. Add index for pending_predictions.source_round_id
CREATE INDEX IF NOT EXISTS pending_predictions_source_round_idx 
ON pending_predictions (source_round_id) WHERE source_round_id IS NOT NULL;

-- 4. Add partial index for prediction_validations by game_id (faster lookup)
CREATE INDEX IF NOT EXISTS prediction_validations_game_id_partial_idx 
ON prediction_validations (game_id) WHERE game_id IS NOT NULL;

-- 5. Add index for notification_outbox target_game_id (dispatch filtering)
CREATE INDEX IF NOT EXISTS notification_outbox_target_game_idx 
ON notification_outbox (target_game_id) WHERE target_game_id IS NOT NULL;

-- 6. Add index for pending_predictions decision field for NO_BET tracking
CREATE INDEX IF NOT EXISTS pending_predictions_decision_idx 
ON pending_predictions (decision) WHERE decision IS NOT NULL;

-- 7. Add decision column to pending_predictions if it doesn't exist
ALTER TABLE pending_predictions 
ADD COLUMN IF NOT EXISTS decision text;

-- 8. Add comment for decision column
COMMENT ON COLUMN pending_predictions.decision IS 'Canonical decision state: ENTRY, REDUCED_ENTRY, SKIP, or NO_BET';
