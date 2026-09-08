-- Migration: 0022_temporal_invariant_check
-- Description: Add CHECK constraint to enforce temporal invariant on pending_predictions
-- Created: 2026-09-08

-- Enforce the hard temporal invariant at the database level:
-- prediction_generated_at (requested_at) < target_round_started_at < target_round.crashed_at
--
-- For pending_predictions, we enforce that requested_at < target_round_started_at
-- when both are NOT NULL. This prevents temporal violations at the DB level.

ALTER TABLE pending_predictions 
ADD CONSTRAINT check_temporal_invariant 
CHECK (target_round_started_at IS NULL OR requested_at < target_round_started_at);

-- Also ensure target_game_id is NOT NULL (should already be enforced by 0013)
-- This is a belt-and-suspenders check
ALTER TABLE pending_predictions 
ADD CONSTRAINT check_target_game_id_not_null 
CHECK (target_game_id IS NOT NULL);

COMMENT ON CONSTRAINT check_temporal_invariant ON pending_predictions IS 
  'Enforces prediction_generated_at < target_round_started_at invariant';

COMMENT ON CONSTRAINT check_target_game_id_not_null ON pending_predictions IS 
  'Ensures all predictions have a valid target_game_id from live bg event';
