-- Migration 0008: Add target_game_id and target_round_started_at for next-round prediction
-- Required for: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §3, §12
-- This migration establishes the strict temporal invariant:
--   prediction_generated_at < target_round_started_at < target_round.crashed_at

-- Add durable target_game_id anchor to pending_predictions
-- This ensures each prediction is tied to a specific BC.Game round
ALTER TABLE pending_predictions 
ADD COLUMN IF NOT EXISTS target_game_id text;

-- Add the authoritative start time of the target round from BC.Game
-- This is the "beganAt" timestamp from the bg (begin) event
ALTER TABLE pending_predictions 
ADD COLUMN IF NOT EXISTS target_round_started_at timestamptz;

-- Add index for efficient lookup of predictions by target game
CREATE INDEX IF NOT EXISTS pending_predictions_target_game_id_idx 
ON pending_predictions (target_game_id) WHERE target_game_id IS NOT NULL;

-- Add index for temporal invariant verification
CREATE INDEX IF NOT EXISTS pending_predictions_temporal_idx 
ON pending_predictions (target_round_started_at, requested_at) 
WHERE target_game_id IS NOT NULL AND target_round_started_at IS NOT NULL;

-- Add constraint to ensure target_game_id is set when target_round_started_at is set
-- This enforces that predictions are always anchored to a real round
ALTER TABLE pending_predictions 
ADD CONSTRAINT IF NOT EXISTS pending_predictions_target_anchor_check 
CHECK (
  target_game_id IS NULL AND target_round_started_at IS NULL 
  OR 
  target_game_id IS NOT NULL AND target_round_started_at IS NOT NULL
);

-- Create a view for temporal invariant verification (spec §9.1)
CREATE OR REPLACE VIEW temporal_invariant_violations AS
SELECT 
  pp.prediction_id,
  pp.requested_at as prediction_generated_at,
  pp.target_game_id,
  pp.target_round_started_at,
  cr.crashed_at as target_round_crashed_at,
  CASE 
    WHEN pp.target_round_started_at IS NULL THEN 'missing_target_started_at'
    WHEN cr.crashed_at IS NULL THEN 'missing_crashed_at'
    WHEN pp.requested_at >= pp.target_round_started_at THEN 'prediction_after_round_start'
    WHEN pp.target_round_started_at >= cr.crashed_at THEN 'round_start_after_crash'
    ELSE 'ok'
  END as violation_type
FROM pending_predictions pp
LEFT JOIN crash_rounds cr ON pp.target_game_id = cr.game_id
WHERE 
  pp.target_game_id IS NOT NULL 
  AND (
    pp.target_round_started_at IS NULL 
    OR cr.crashed_at IS NULL 
    OR pp.requested_at >= pp.target_round_started_at 
    OR pp.target_round_started_at >= cr.crashed_at
  );

-- Create a function to check temporal invariant violations
CREATE OR REPLACE FUNCTION check_temporal_invariant()
RETURNS TABLE (
  prediction_id text,
  violation_type text,
  prediction_generated_at timestamptz,
  target_game_id text,
  target_round_started_at timestamptz,
  target_round_crashed_at timestamptz
) AS $$
BEGIN
  RETURN QUERY
  SELECT 
    pp.prediction_id,
    CASE 
      WHEN pp.target_round_started_at IS NULL THEN 'missing_target_started_at'
      WHEN cr.crashed_at IS NULL THEN 'missing_crashed_at'
      WHEN pp.requested_at >= pp.target_round_started_at THEN 'prediction_after_round_start'
      WHEN pp.target_round_started_at >= cr.crashed_at THEN 'round_start_after_crash'
      ELSE 'ok'
    END as violation_type,
    pp.requested_at as prediction_generated_at,
    pp.target_game_id,
    pp.target_round_started_at,
    cr.crashed_at as target_round_crashed_at
  FROM pending_predictions pp
  LEFT JOIN crash_rounds cr ON pp.target_game_id = cr.game_id
  WHERE 
    pp.target_game_id IS NOT NULL 
    AND (
      pp.target_round_started_at IS NULL 
      OR cr.crashed_at IS NULL 
      OR pp.requested_at >= pp.target_round_started_at 
      OR pp.target_round_started_at >= cr.crashed_at
    );
END;
$$ LANGUAGE plpgsql;

COMMENT ON COLUMN pending_predictions.target_game_id IS 'The specific BC.Game round this prediction targets (from bg event gameId)';
COMMENT ON COLUMN pending_predictions.target_round_started_at IS 'The start timestamp of the target round from BC.Game (beganAt from bg event)';