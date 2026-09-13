-- Migration 0050: forensic REPAIR index must include AWAITING_TARGET_START.
--
-- 0040 partial index only covered NULL/UNKNOWN. After 0049, most delivered
-- PR-path rows sit in AWAITING_TARGET_START until target start is known —
-- the REPAIR scan then missed the index and paid a sequential scan + 3-way
-- join (~528–541ms general slow_query in prod).

DROP INDEX IF EXISTS notification_outbox_forensic_repair_idx;

CREATE INDEX IF NOT EXISTS notification_outbox_forensic_repair_idx
  ON notification_outbox (delivered_at DESC NULLS LAST)
  WHERE type = 'prediction'
    AND status = 'delivered'
    AND telegram_accepted_at IS NOT NULL
    AND (delivery_outcome IS NULL
         OR delivery_outcome = 'UNKNOWN'
         OR delivery_outcome = 'AWAITING_TARGET_START');
