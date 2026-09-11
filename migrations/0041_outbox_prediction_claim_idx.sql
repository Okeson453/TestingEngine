-- Prediction-lane claim path (notification-worker processLane "prediction"):
--   WHERE status = 'pending' AND type = 'prediction'
--     AND next_attempt_at <= now()
--     AND (telegram_deadline_at IS NULL OR telegram_deadline_at > now())
--   ORDER BY priority DESC, next_attempt_at ASC, id ASC
--   FOR UPDATE SKIP LOCKED
--
-- Existing notification_outbox_pending_idx is (status, next_attempt_at, priority)
-- WHERE status = 'pending' and does not lead with type, so prediction claims
-- scanned mixed pending rows. This partial index matches the prediction filter
-- and sort order exactly.
CREATE INDEX IF NOT EXISTS notification_outbox_prediction_claim_idx
  ON notification_outbox (priority DESC, next_attempt_at ASC, id ASC)
  WHERE status = 'pending' AND type = 'prediction';
