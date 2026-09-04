-- Migration 0008: prediction target anchoring + status enum + active-target index.
-- Spec context: critical fix set per the operator-supplied diagnosis. Establishes
-- the durable link between a prediction row and the specific BC.Game round it
-- targets, with a CHECK constraint on `status` and a partial UNIQUE index that
-- prevents two active predictions from targeting the same game_id.
ALTER TABLE pending_predictions
  ADD COLUMN IF NOT EXISTS source_game_id text;

ALTER TABLE pending_predictions
  ADD COLUMN IF NOT EXISTS generated_at timestamptz;

ALTER TABLE pending_predictions
  ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'PENDING'
    CHECK (status IN ('PENDING', 'MATCHED', 'EXPIRED', 'CANCELLED'));

-- Backfill: rows with no target_game_id are stale (the old "next" path); mark
-- them MATCHED so they don't pollute the active-target index, and copy
-- matched_game_id into target_game_id so the unique index is consistent.
UPDATE pending_predictions
SET target_game_id = COALESCE(matched_game_id, ''),
    status = CASE WHEN matched = true THEN 'MATCHED' ELSE 'PENDING' END
WHERE target_game_id IS NULL OR target_game_id = '';

-- Now enforce NOT NULL on target_game_id (the column was added nullable in
-- 0007 because the old "next" path did not set it).
ALTER TABLE pending_predictions ALTER COLUMN target_game_id SET NOT NULL;

-- Prevent duplicate active predictions for the same target round. The
-- partial index only covers PENDING rows so MATCHED/EXPIRED/CANCELLED do
-- not block subsequent cycles from re-predicting the same game_id.
CREATE UNIQUE INDEX IF NOT EXISTS pending_predictions_active_target_uidx
  ON pending_predictions (target_game_id) WHERE status = 'PENDING';
