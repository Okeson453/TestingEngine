-- Migration 0031: add EARLY to the delivery_outcome vocabulary.
--
-- classifyDelivery now distinguishes a delivery with comfortable margin
-- (lead_time_ms >= DELIVERY_EARLY_LEAD_MS, default 4000) from a hairline
-- ON_TIME delivery. Both are valid deliveries; EARLY is the healthy one.
-- MISSED is deliberately NOT a stored outcome — it is the reporting union
-- of EXPIRED ∪ FAILED and needs no schema representation.
--
-- Migration 0029 declared the CHECK inline on ADD COLUMN, which Postgres
-- auto-named notification_outbox_delivery_outcome_check. Drop by that name
-- and re-add with the widened vocabulary.

ALTER TABLE notification_outbox
  DROP CONSTRAINT IF EXISTS notification_outbox_delivery_outcome_check;

ALTER TABLE notification_outbox
  ADD CONSTRAINT notification_outbox_delivery_outcome_check
  CHECK (delivery_outcome IS NULL OR delivery_outcome IN (
    'EARLY', 'ON_TIME', 'LATE', 'EXPIRED', 'FAILED', 'UNKNOWN'
  ));

COMMENT ON COLUMN notification_outbox.delivery_outcome IS
  'Forensics: EARLY if accepted >= 4s before target start; ON_TIME if accepted before target start; LATE if after; EXPIRED if killed; FAILED if send failed; UNKNOWN if target start unknown';
