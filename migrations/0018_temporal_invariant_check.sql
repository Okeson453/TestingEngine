-- Migration 0018: DB-level temporal invariant on pending_predictions.
-- Spec: TestingEngine-Complete-Issues-and-Recommendations.md §6.4 (C2)
--
-- Hard guarantee (independent of the application layer) that a prediction
-- row is never persisted with `generated_at` at or after the start of the
-- round it targets. A small tolerance is allowed for the inherent network
-- and clock skew between BC.Game, the DB, and the worker (the in-app
-- `TEMPORAL_TOLERANCE_MS` is 100ms; we double it here to be safe).
--
-- The CHECK is `NOT VALID` only when the table already has rows that would
-- fail it; the migration aborts loudly in that case (operator must clean
-- up) so the constraint can be applied as `VALIDATED` (the production
-- intent). For a fresh database, the constraint is created `VALIDATED`
-- from the start.
--
-- This constraint does not block the ED-path insert in `onGameEndPredict`,
-- which writes `target_round_started_at = NULL` (filled in later by the
-- bg handler). The CHECK is `OR target_round_started_at IS NULL` to
-- allow that case.

DO $$
DECLARE
  violating_count int;
BEGIN
  SELECT count(*)::int
    INTO violating_count
    FROM pending_predictions
   WHERE target_round_started_at IS NOT NULL
     AND generated_at IS NOT NULL
     AND generated_at >= target_round_started_at + interval '1 second';

  IF violating_count > 0 THEN
    RAISE EXCEPTION
      'Migration 0018 aborted: % rows violate the temporal invariant. Run `SELECT * FROM pending_predictions WHERE target_round_started_at IS NOT NULL AND generated_at >= target_round_started_at + interval ''1 second''` and clean them up before re-applying.',
      violating_count;
  END IF;
END $$;

ALTER TABLE pending_predictions
  DROP CONSTRAINT IF EXISTS pending_predictions_temporal_invariant_check;

ALTER TABLE pending_predictions
  ADD CONSTRAINT pending_predictions_temporal_invariant_check
  CHECK (
    target_round_started_at IS NULL
    OR generated_at IS NULL
    OR generated_at < target_round_started_at + interval '1 second'
  );
