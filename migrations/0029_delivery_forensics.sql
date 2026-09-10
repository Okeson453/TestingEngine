-- Migration 0029: Prediction delivery forensics columns + enriched timeline view
-- NOTE: CREATE OR REPLACE VIEW cannot change column names/order in Postgres.
-- We DROP + CREATE so inserting delivery_outcome/lead_time_ms does not try to
-- "rename" last_error → delivery_outcome (worker crash loop).

ALTER TABLE notification_outbox
  ADD COLUMN IF NOT EXISTS delivery_outcome TEXT
    CHECK (delivery_outcome IS NULL OR delivery_outcome IN (
      'ON_TIME', 'LATE', 'EXPIRED', 'FAILED', 'UNKNOWN'
    )),
  ADD COLUMN IF NOT EXISTS lead_time_ms INTEGER;

COMMENT ON COLUMN notification_outbox.delivery_outcome IS
  'Forensics: ON_TIME if telegram_accepted < target start; LATE if after; EXPIRED if killed; UNKNOWN if target start unknown';
COMMENT ON COLUMN notification_outbox.lead_time_ms IS
  'target_round_started_at - telegram_accepted_at in ms (positive = ON_TIME)';

CREATE INDEX IF NOT EXISTS notification_outbox_delivery_outcome_idx
  ON notification_outbox (delivery_outcome)
  WHERE delivery_outcome IS NOT NULL;

DROP VIEW IF EXISTS prediction_delivery_timeline;

CREATE VIEW prediction_delivery_timeline AS
SELECT
  p.prediction_id,
  p.source_round_id                    AS source_game_id,
  p.target_game_id,
  p.correlation_id,
  p.generated_at,
  p.requested_at,
  p.target_round_started_at,
  p.status                             AS pending_status,
  p.probability,
  p.confidence,
  o.notification_id,
  o.id                                 AS outbox_row_id,
  o.type                               AS outbox_type,
  o.status                             AS outbox_status,
  o.created_at                         AS queued_at,
  o.dispatch_claimed_at                AS dispatch_started_at,
  o.send_started_at                    AS telegram_send_started_at,
  o.telegram_accepted_at,
  o.delivered_at,
  o.telegram_deadline_at,
  o.delivery_outcome,
  o.lead_time_ms,
  o.last_error,
  o.priority,
  o.attempt_count,
  lrs.began_at                         AS target_round_started_at_live,
  lrs.lifecycle                        AS target_lifecycle,
  CASE
    WHEN o.dispatch_claimed_at IS NOT NULL AND o.created_at IS NOT NULL
      THEN EXTRACT(EPOCH FROM (o.dispatch_claimed_at - o.created_at)) * 1000
  END                                  AS queue_wait_ms,
  CASE
    WHEN o.send_started_at IS NOT NULL AND o.dispatch_claimed_at IS NOT NULL
      THEN EXTRACT(EPOCH FROM (o.send_started_at - o.dispatch_claimed_at)) * 1000
  END                                  AS dispatch_ms,
  CASE
    WHEN o.telegram_accepted_at IS NOT NULL AND o.send_started_at IS NOT NULL
      THEN EXTRACT(EPOCH FROM (o.telegram_accepted_at - o.send_started_at)) * 1000
  END                                  AS send_ms,
  CASE
    WHEN o.telegram_accepted_at IS NOT NULL AND o.created_at IS NOT NULL
      THEN EXTRACT(EPOCH FROM (o.telegram_accepted_at - o.created_at)) * 1000
  END                                  AS total_delivery_ms,
  CASE
    WHEN COALESCE(p.target_round_started_at, lrs.began_at) IS NOT NULL
         AND o.telegram_accepted_at IS NOT NULL
      THEN EXTRACT(EPOCH FROM (
        COALESCE(p.target_round_started_at, lrs.began_at) - o.telegram_accepted_at
      )) * 1000
  END                                  AS lead_time_computed_ms,
  CASE
    WHEN o.delivery_outcome IS NOT NULL THEN o.delivery_outcome
    WHEN o.status = 'dead_letter' THEN 'EXPIRED'
    WHEN o.status = 'failed' THEN 'FAILED'
    WHEN o.telegram_accepted_at IS NULL THEN 'UNKNOWN'
    WHEN COALESCE(p.target_round_started_at, lrs.began_at) IS NULL THEN 'UNKNOWN'
    WHEN o.telegram_accepted_at < COALESCE(p.target_round_started_at, lrs.began_at)
      THEN 'ON_TIME'
    ELSE 'LATE'
  END                                  AS delivery_status
FROM pending_predictions p
LEFT JOIN LATERAL (
  SELECT *
  FROM notification_outbox o2
  WHERE o2.type = 'prediction'
    AND (
      o2.target_game_id = p.target_game_id
      OR o2.metadata->>'predictionId' = p.prediction_id
    )
  ORDER BY o2.created_at DESC
  LIMIT 1
) o ON true
LEFT JOIN live_round_state lrs
  ON lrs.game_id = p.target_game_id;

COMMENT ON VIEW prediction_delivery_timeline IS
  'End-to-end forensics: prediction_id, notification_id, timeline stamps, lead_time, ON_TIME|LATE|EXPIRED|FAILED|UNKNOWN';
