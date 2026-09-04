-- Migration 0013: prediction target anchoring + status enum + active-target index.
-- Spec context: critical fix set per the operator-supplied diagnosis. Establishes
-- the durable link between a prediction row and the specific BC.Game round it
-- targets, with a CHECK constraint on `status` and a partial UNIQUE index that
-- prevents two active predictions from targeting the same game_id.
--
-- Compatibility note: migration 0008 already added `target_game_id` and
-- `target_round_started_at`. An earlier attempt used COALESCE(matched_game_id, '')
-- which produced empty-string targets and violated constraints. Corrected flow:
--   1. Add columns nullable first
--   2. Backfill real targets from matched_game_id (never empty string)
--   3. Cancel legacy rows that still have no target (unrecoverable)
--   4. For cancelled rows that lack a target, assign a unique synthetic id so
--      the column can safely become NOT NULL without data loss
--   5. Apply NOT NULL only after every row has a non-empty target
-- There is intentionally NO CHECK (target_game_id <> '') — the partial UNIQUE
-- index on PENDING rows is the canonical idempotency gate.

-- 1. Add new columns (nullable first; NOT NULL applied after cleanup).
ALTER TABLE pending_predictions
  ADD COLUMN IF NOT EXISTS source_game_id text;

ALTER TABLE pending_predictions
  ADD COLUMN IF NOT EXISTS generated_at timestamptz;

ALTER TABLE pending_predictions
  ADD COLUMN IF NOT EXISTS status text DEFAULT 'PENDING'
    CHECK (status IN ('PENDING', 'MATCHED', 'EXPIRED', 'CANCELLED'));

-- 2. Backfill: set target from matched_game_id where available.
--    Both target_game_id AND target_round_started_at are set together so any
--    residual "both or neither" invariant from 0008 stays satisfied.
UPDATE pending_predictions
SET target_game_id = matched_game_id,
    target_round_started_at = COALESCE(target_round_started_at, requested_at),
    status = CASE WHEN matched = true THEN 'MATCHED' ELSE COALESCE(status, 'PENDING') END
WHERE matched_game_id IS NOT NULL
  AND (target_game_id IS NULL OR target_game_id = '');

-- 3. Cancel legacy rows that still have no target (can't validate them).
UPDATE pending_predictions
SET status = 'CANCELLED',
    matched = true
WHERE (target_game_id IS NULL OR target_game_id = '')
  AND (status IS NULL OR status = 'PENDING');

-- 4. Give cancelled (or any remaining empty) rows a unique synthetic target so
--    the column can become NOT NULL without violating constraints or losing rows.
UPDATE pending_predictions
SET target_game_id = 'cancelled-' || prediction_id
WHERE target_game_id IS NULL OR target_game_id = '';

-- 5. Now every row has a non-empty target_game_id → safe to tighten.
ALTER TABLE pending_predictions ALTER COLUMN target_game_id SET NOT NULL;

-- 6. Make status NOT NULL (every row now has a real status).
UPDATE pending_predictions SET status = 'PENDING' WHERE status IS NULL;
ALTER TABLE pending_predictions ALTER COLUMN status SET NOT NULL;

-- 7. Prevent duplicate active predictions for the same target round.
--    Partial index only covers PENDING so MATCHED/EXPIRED/CANCELLED do not
--    block later cycles. This UNIQUE is the canonical idempotency gate.
CREATE UNIQUE INDEX IF NOT EXISTS pending_predictions_active_target_uidx
  ON pending_predictions (target_game_id) WHERE status = 'PENDING';
