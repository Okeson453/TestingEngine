-- Migration 0049: AWAITING_TARGET_START forensics outcome.
--
-- PR-primary delivery is accepted by Telegram ~7s BEFORE target N+1 starts.
-- That is not UNKNOWN (missing data) — it is a deterministic interim state:
-- accepted_at known, target_round_started_at not yet known.
-- BG reclassify upgrades AWAITING_TARGET_START → EARLY/ON_TIME/LATE.
-- UNKNOWN is reserved for genuine missing/invalid transitions only.

ALTER TABLE notification_outbox
  DROP CONSTRAINT IF EXISTS notification_outbox_delivery_outcome_check;

ALTER TABLE notification_outbox
  ADD CONSTRAINT notification_outbox_delivery_outcome_check
  CHECK (delivery_outcome IS NULL OR delivery_outcome IN (
    'EARLY', 'ON_TIME', 'LATE', 'EXPIRED', 'FAILED',
    'AWAITING_TARGET_START', 'UNKNOWN'
  ));

COMMENT ON COLUMN notification_outbox.delivery_outcome IS
  'Forensics: EARLY/ON_TIME/LATE vs target start; EXPIRED killed; FAILED send fail; AWAITING_TARGET_START accepted but target start not yet known (PR path); UNKNOWN only for genuine missing/invalid transition';
