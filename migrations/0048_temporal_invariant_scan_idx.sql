-- Migration 0048: speed up production-invariant temporal violation probe.
--
-- The probe selects recent rows where requested_at >= target_round_started_at.
-- Cross-column inequality cannot use a plain btree efficiently; without a
-- time bound the scan was measured ~524ms. Application now also filters
-- requested_at >= now() - 24h. This partial index supports that windowed
-- ORDER BY requested_at DESC LIMIT n path.

CREATE INDEX IF NOT EXISTS pending_predictions_temporal_violation_recent_idx
  ON pending_predictions (requested_at DESC)
  WHERE target_round_started_at IS NOT NULL
    AND requested_at >= target_round_started_at;

COMMENT ON INDEX pending_predictions_temporal_violation_recent_idx IS
  'Invariant probe: recent predictions generated at/after target start (LIMIT + time bound)';
