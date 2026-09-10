-- Migration 0033: delivery_status is timestamp-authoritative.
--
-- Defect: prediction_delivery_timeline preferred stored delivery_outcome
-- over the raw timeline whenever the cache was non-NULL. A failed or
-- stale forensic write (e.g. ON_TIME while telegram_accepted_at >= target
-- start) then permanently masked LATE into a healthy count.
--
-- Fix: derive delivery_status from authoritative timestamps whenever they
-- exist. Stored delivery_outcome is only a CACHE used when the timeline
-- cannot yet decide (missing acceptance or missing target start).
--
-- Also expands target-start resolution to include crash_rounds.began_at
-- (same order as the worker reconcile path).

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
    WHEN COALESCE(p.target_round_started_at, lrs.began_at, cr.began_at) IS NOT NULL
         AND o.telegram_accepted_at IS NOT NULL
      THEN EXTRACT(EPOCH FROM (
        COALESCE(p.target_round_started_at, lrs.began_at, cr.began_at) - o.telegram_accepted_at
      )) * 1000
  END                                  AS lead_time_computed_ms,
  -- AUTHORITATIVE classification (remediation §3/§4):
  -- timestamps win whenever they fully determine the outcome. The stored
  -- delivery_outcome cache is consulted only when the timeline is incomplete.
  CASE
    WHEN o.status = 'dead_letter' THEN 'EXPIRED'
    WHEN o.status = 'failed' THEN 'FAILED'
    WHEN o.telegram_accepted_at IS NULL
         AND o.status IN ('pending', 'inflight') THEN 'UNKNOWN'
    WHEN o.telegram_accepted_at IS NULL THEN COALESCE(o.delivery_outcome, 'UNKNOWN')
    WHEN COALESCE(p.target_round_started_at, lrs.began_at, cr.began_at) IS NULL
      THEN COALESCE(o.delivery_outcome, 'UNKNOWN')
    WHEN o.telegram_accepted_at >= COALESCE(p.target_round_started_at, lrs.began_at, cr.began_at)
      THEN 'LATE'
    WHEN EXTRACT(EPOCH FROM (
        COALESCE(p.target_round_started_at, lrs.began_at, cr.began_at) - o.telegram_accepted_at
      )) * 1000 >= 4000
      THEN 'EARLY'
    ELSE 'ON_TIME'
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
  ON lrs.game_id = p.target_game_id
LEFT JOIN LATERAL (
  SELECT began_at
  FROM crash_rounds
  WHERE game_id = p.target_game_id
    AND began_at IS NOT NULL
  ORDER BY began_at DESC
  LIMIT 1
) cr ON true;

COMMENT ON VIEW prediction_delivery_timeline IS
  'End-to-end forensics: delivery_status is derived from authoritative timestamps; delivery_outcome is a cache only.';

-- Repair stored cache where raw timestamps disagree (MASKED LATE / drift).
-- Idempotent: only rewrites rows whose cache is wrong or NULL while the
-- timeline is fully determinate.
UPDATE notification_outbox o
SET delivery_outcome = CASE
      WHEN EXTRACT(EPOCH FROM (tgt.started_at - o.telegram_accepted_at)) * 1000 >= 4000
      THEN 'EARLY'
      WHEN o.telegram_accepted_at < tgt.started_at THEN 'ON_TIME'
      ELSE 'LATE'
    END,
    lead_time_ms = ROUND(
      EXTRACT(EPOCH FROM (tgt.started_at - o.telegram_accepted_at)) * 1000
    )::integer
FROM (
  SELECT o2.id AS outbox_id,
    (
      SELECT s.started_at FROM (
        SELECT p.target_round_started_at AS started_at
        FROM pending_predictions p
        WHERE p.prediction_id = o2.metadata->>'predictionId'
          AND p.target_round_started_at IS NOT NULL
        UNION ALL
        SELECT lrs.began_at
        FROM live_round_state lrs
        WHERE lrs.game_id = coalesce(o2.target_game_id, o2.metadata->>'targetGameId')
          AND lrs.began_at IS NOT NULL
        UNION ALL
        SELECT cr.began_at
        FROM crash_rounds cr
        WHERE cr.game_id = coalesce(o2.target_game_id, o2.metadata->>'targetGameId')
          AND cr.began_at IS NOT NULL
      ) s
      WHERE s.started_at IS NOT NULL
      ORDER BY s.started_at
      LIMIT 1
    ) AS started_at
  FROM notification_outbox o2
  WHERE o2.type = 'prediction'
    AND o2.status = 'delivered'
    AND o2.telegram_accepted_at IS NOT NULL
) tgt
WHERE o.id = tgt.outbox_id
  AND tgt.started_at IS NOT NULL
  AND (
    o.delivery_outcome IS NULL
    OR o.delivery_outcome = 'UNKNOWN'
    OR (o.telegram_accepted_at >= tgt.started_at AND o.delivery_outcome <> 'LATE')
    OR (o.telegram_accepted_at < tgt.started_at
        AND EXTRACT(EPOCH FROM (tgt.started_at - o.telegram_accepted_at)) * 1000 >= 4000
        AND o.delivery_outcome NOT IN ('EARLY'))
    OR (o.telegram_accepted_at < tgt.started_at
        AND EXTRACT(EPOCH FROM (tgt.started_at - o.telegram_accepted_at)) * 1000 < 4000
        AND o.delivery_outcome NOT IN ('ON_TIME', 'EARLY'))
  );
