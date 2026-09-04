-- Migration 0013: prediction target anchoring + status enum + active-target index.
-- Spec context: critical fix set per the operator-supplied diagnosis. Establishes
-- the durable link between a prediction row and the specific BC.Game round it
-- targets, with a CHECK constraint on `status` and a partial UNIQUE index that
-- prevents two active predictions from targeting the same game_id.
--
-- Compatibility note: migration 0008 already added `target_game_id` and
-- `target_round_started_at` columns with a `target_anchor_check` constraint
-- that requires BOTH columns to be NULL or BOTH NOT NULL. This migration
-- must therefore set both columns together (or set neither) on any row
-- it touches. The earlier attempt used `COALESCE(matched_game_id, '')`
-- which set `target_game_id = ''` while leaving `target_round_started_at`
-- NULL, violating the 0008 constraint. The corrected approach below
-- cancels legacy rows that have no `matched_game_id` and only then
-- applies NOT NULL to the remaining non-empty rows.

-- 1. Add new columns (nullable first; NOT NULL applied after cleanup).
ALTER TABLE pending_predictions
  ADD COLUMN IF NOT EXISTS source_game_id text;

ALTER TABLE pending_predictions
  ADD COLUMN IF NOT EXISTS generated_at timestamptz;

ALTER TABLE pending_predictions
  ADD COLUMN IF NOT EXISTS status text DEFAULT 'PENDING'
    CHECK (status IN ('PENDING', 'MATCHED', 'EXPIRED', 'CANCELLED'));

-- 2. Cancel legacy rows that have no matched_game_id. These are
--    unrecoverable (no way to know which round they targeted) and would
--    break the 0008 `target_anchor_check` constraint if we tried to
--    backfill an empty target. Cancelling them also keeps the active-
--    target unique index clean.
UPDATE pending_predictions
SET status = 'CANCELLED',
    matched = true,
    target_game_id = NULL,
    target_round_started_at = NULL
WHERE matched_game_id IS NULL
  AND (target_game_id IS NULL OR target_game_id = '');

-- 3. Backfill: rows with a matched_game_id and no target_game_id get
--    their target anchored. BOTH target_game_id AND target_round_started_at
--    must be set together (0008 target_anchor_check). Use the
--    requested_at as the beganAt stand-in: the prediction was made
--    AFTER the round began (the legacy "next" path was not event-driven
--    and never recorded the round's authoritative beginTime). This is
--    the best approximation available; the new event-driven path writes
--    the real beginTime.
UPDATE pending_predictions
SET target_game_id = matched_game_id,
    target_round_started_at = COALESCE(target_round_started_at, requested_at),
    status = CASE WHEN matched = true THEN 'MATCHED' ELSE 'PENDING' END
WHERE matched_game_id IS NOT NULL
  AND (target_game_id IS NULL OR target_game_id = '');

-- 4. Now make target_game_id NOT NULL (only rows with real values remain).
ALTER TABLE pending_predictions ALTER COLUMN target_game_id SET NOT NULL;

-- 5. Make status NOT NULL (every row now has a real status).
ALTER TABLE pending_predictions ALTER COLUMN status SET NOT NULL;

-- 6. Prevent duplicate active predictions for the same target round. The
--    partial index only covers PENDING rows so MATCHED/EXPIRED/CANCELLED
--    do not block subsequent cycles from re-predicting the same game_id.
--    The UNIQUE index is the canonical idempotency gate; the SELECT in
--    generateAndQueuePrediction is a fast-path that lets us skip the
--    model call when one already exists.
CREATE UNIQUE INDEX IF NOT EXISTS pending_predictions_active_target_uidx
  ON pending_predictions (target_game_id) WHERE status = 'PENDING';
