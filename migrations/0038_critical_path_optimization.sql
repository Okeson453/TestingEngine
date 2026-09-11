-- Migration 0038: Critical path query optimization
--
-- Optimizes database operations on the ED→N+1 prediction critical path.
-- Focuses on eliminating unnecessary queries, adding missing indexes,
-- and reducing round-trip times.
--
-- Related: DATABASE_AUDIT_REPORT.md §5.1

-- 1. Add index for crash_rounds.began_at (temporal queries for dispatch validation)
--    Used when checking if target round has started (notification-worker temporal gate).
--    Enables efficient range queries for rounds starting within a time window.
CREATE INDEX IF NOT EXISTS crash_rounds_began_at_idx 
ON crash_rounds (began_at DESC);

-- 2. Add composite index for live_round_state (game_id + lifecycle)
--    Optimizes state lookups that filter by both game ID and lifecycle stage.
--    Supports the dispatch temporal validity checks.
CREATE INDEX IF NOT EXISTS live_round_state_game_lifecycle_idx 
ON live_round_state (game_id, lifecycle) 
WHERE lifecycle IN ('STARTED', 'RUNNING', 'ENDED', 'RECONCILED');

-- 3. Add index for pending_predictions.source_round_id
--    Enables efficient correlation queries ("show all predictions for source round X").
--    Partial index avoids overhead for NULL values.
CREATE INDEX IF NOT EXISTS pending_predictions_source_round_idx 
ON pending_predictions (source_round_id) 
WHERE source_round_id IS NOT NULL;

-- 4. Add partial index for prediction_validations by game_id
--    Faster lookup when checking if a round has been validated.
--    More selective than the existing general index on game_id.
CREATE INDEX IF NOT EXISTS prediction_validations_game_id_partial_idx 
ON prediction_validations (game_id) 
WHERE game_id IS NOT NULL;

-- 5. Add index for notification_outbox target_game_id
--    Enables efficient filtering of outbox rows by target game.
--    Critical for the prediction lane dispatch optimization.
CREATE INDEX IF NOT EXISTS notification_outbox_target_game_idx 
ON notification_outbox (target_game_id) 
WHERE target_game_id IS NOT NULL;

-- 6. Add index for notification_outbox created_at
--    Supports cleanup/retention queries and monitoring.
CREATE INDEX IF NOT EXISTS notification_outbox_created_at_idx 
ON notification_outbox (created_at DESC);

-- 7. Add partial index for worker_state key lookup
--    Optimizes the frequent key-based lookups in worker_state.
CREATE INDEX IF NOT EXISTS worker_state_key_idx 
ON worker_state (key);

COMMENT ON INDEX crash_rounds_began_at_idx IS 
  'Optimizes temporal queries for dispatch validation (replaces SELECT crash_rounds WHERE game_id)';

COMMENT ON INDEX live_round_state_game_lifecycle_idx IS 
  'Optimizes dispatch temporal checks (replaces SELECT live_round_state WHERE game_id)';

COMMENT ON INDEX pending_predictions_source_round_idx IS 
  'Enables correlation queries for debugging and analytics';

COMMENT ON INDEX prediction_validations_game_id_partial_idx IS 
  'Faster validation existence checks (replaces SELECT prediction_validations WHERE game_id)';

COMMENT ON INDEX notification_outbox_target_game_idx IS 
  'Enables in-memory registry-based dispatch (eliminates per-row temporal SELECTs)';
