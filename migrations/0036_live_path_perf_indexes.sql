-- Migration 0036: Live-path performance indexes for poll recovery + outbox.
--
-- Observed patterns (poll-worker maybePredictNewest + notification claim):
--   1. pending_predictions WHERE status='PENDING' ORDER BY requested_at DESC LIMIT 1
--      — used to decide whether ED is current before poll recovery defers.
--      Existing partial unique is on (target_game_id) WHERE status='PENDING'
--      which does not help a global "latest PENDING by time" lookup.
--   2. Keep indexes minimal; do not re-add full-table status indexes already
--      present from 0010 (notification_outbox_status_idx).

CREATE INDEX IF NOT EXISTS pending_predictions_pending_requested_desc_idx
  ON pending_predictions (requested_at DESC)
  WHERE status = 'PENDING';

COMMENT ON INDEX pending_predictions_pending_requested_desc_idx IS
  'Poll recovery: latest PENDING prediction by time without scanning matched/expired rows';
