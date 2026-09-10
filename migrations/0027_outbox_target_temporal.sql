-- 0027: hard temporal delivery contract for prediction signals.
--
-- Business rule: a prediction signal for target round N+1 is only valid
-- BEFORE round N+1 starts. "target already started — delivering late signal
-- anyway" is removed; a signal that misses its window must be EXPIRED, never
-- delivered. target_game_id is lifted out of metadata into a real column so
-- BG(N+1) can atomically kill pending signals targeting the round that just
-- started, and the invariant "delivered_at < target_started_at" is queryable.

ALTER TABLE notification_outbox
  ADD COLUMN IF NOT EXISTS target_game_id TEXT;

-- Backfill from metadata (best effort; rows missing metadata stay NULL).
UPDATE notification_outbox
SET target_game_id = COALESCE(
  metadata->>'targetGameId',
  metadata->>'target_game_id'
)
WHERE type = 'prediction'
  AND target_game_id IS NULL;

-- BG-kill index: atomic invalidation of undelivered signals for a target.
CREATE INDEX IF NOT EXISTS outbox_prediction_pending_target_idx
  ON notification_outbox (target_game_id)
  WHERE type = 'prediction'
    AND status IN ('pending', 'inflight');

COMMENT ON COLUMN notification_outbox.target_game_id IS
  'Prediction signals only: the round this signal trades. Delivery MUST complete before that round starts — enforced by dispatcher gate + BG kill + prediction_delivered_after_target_start invariant.';
