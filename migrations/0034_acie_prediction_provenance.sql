-- Migration 0034: ACIE prediction provenance columns on pending_predictions
-- Supports forensic proof that every signal came from fresh shared ACIE state.

ALTER TABLE pending_predictions
  ADD COLUMN IF NOT EXISTS acie_instance_id text,
  ADD COLUMN IF NOT EXISTS acie_observation_count integer,
  ADD COLUMN IF NOT EXISTS acie_state_version integer,
  ADD COLUMN IF NOT EXISTS feature_hash text,
  ADD COLUMN IF NOT EXISTS prediction_mode text,
  ADD COLUMN IF NOT EXISTS execution_path text,
  ADD COLUMN IF NOT EXISTS strategy_action text;

CREATE INDEX IF NOT EXISTS idx_pending_predictions_feature_hash
  ON pending_predictions (feature_hash)
  WHERE feature_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pending_predictions_acie_obs
  ON pending_predictions (acie_observation_count)
  WHERE acie_observation_count IS NOT NULL;

COMMENT ON COLUMN pending_predictions.acie_instance_id IS 'Shared ACIE singleton id that produced this prediction';
COMMENT ON COLUMN pending_predictions.acie_observation_count IS 'ACIE online.observationCount at emission';
COMMENT ON COLUMN pending_predictions.acie_state_version IS 'Monotonic ACIE state version (observationCount)';
COMMENT ON COLUMN pending_predictions.feature_hash IS 'SHA-256 fingerprint of ACIE input features';
COMMENT ON COLUMN pending_predictions.prediction_mode IS 'NORMAL_ACIE | FALLBACK_BASELINE | ADVANCED_ACIE | SAFE_BASELINE | STALE_REJECTED';
COMMENT ON COLUMN pending_predictions.execution_path IS 'Code path that produced the decision';
COMMENT ON COLUMN pending_predictions.strategy_action IS 'ACIE strategy action ENTRY/SKIP/REDUCED_ENTRY';
