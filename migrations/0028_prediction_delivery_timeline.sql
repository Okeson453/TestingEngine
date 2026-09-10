-- Migration 0028: End-to-end prediction delivery correlation view
-- Maps prediction_id / source / target / notification timeline fields for ops.

CREATE OR REPLACE VIEW prediction_delivery_timeline AS
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
         AND p.generated_at IS NOT NULL
      THEN EXTRACT(EPOCH FROM (
        COALESCE(p.target_round_started_at, lrs.began_at) - p.generated_at
      )) * 1000
  END                                  AS lead_time_before_target_start_ms
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
  'End-to-end correlation: prediction_id, source/target game ids, notification_id, queued/dispatch/send/accepted timestamps';
