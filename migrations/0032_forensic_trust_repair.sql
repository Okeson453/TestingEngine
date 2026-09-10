-- Migration 0032: forensic trust repair.
--
-- Two defects this fixes (dashboard-trust remediation):
--
-- 1. NO BACKFILL: migration 0029 added delivery_outcome/lead_time_ms as
--    nullable columns with no backfill. Every delivered row whose forensic
--    write failed (or that predates 0029) is stuck at NULL even when the
--    authoritative raw timestamps (telegram_accepted_at vs target start)
--    fully determine the outcome. Those NULLs were being counted as
--    UNKNOWN by the dashboard, manufacturing a fake "PENDING" population.
--
-- 2. THE VIEW'S DERIVED BRANCH DID NOT KNOW ABOUT EARLY: delivery_status
--    derived ON_TIME for any accepted-before-start row, collapsing the
--    EARLY/ON_TIME distinction the worker now persists (migration 0031).
--
-- Backfill rule — deterministic classification from raw timestamps ONLY
-- where they permit it; genuinely indeterminable rows stay NULL so the
-- reconciliation sweep can classify them when target start becomes known.

-- 1) Terminal statuses are determinate from status alone.
UPDATE notification_outbox
SET delivery_outcome = 'EXPIRED'
WHERE delivery_outcome IS NULL
  AND type = 'prediction'
  AND status = 'dead_letter';

UPDATE notification_outbox
SET delivery_outcome = 'FAILED'
WHERE delivery_outcome IS NULL
  AND type = 'prediction'
  AND status = 'failed';

-- 2) Delivered rows with accepted + target start: classify from the raw
--    timeline. Target start resolution order: pending_predictions ->
--    live_round_state -> crash_rounds (same as the worker).
--    NOTE: correlated lateral references to the UPDATE target alias are not
--    allowed in Postgres UPDATE...FROM, so target-start resolution lives in
--    a self-contained derived table keyed by outbox id.
UPDATE notification_outbox o
SET delivery_outcome = CASE
      WHEN EXTRACT(EPOCH FROM (tgt.started_at - o.telegram_accepted_at)) * 1000
        >= 4000
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
    AND o2.delivery_outcome IS NULL
    AND o2.telegram_accepted_at IS NOT NULL
) tgt
WHERE o.id = tgt.outbox_id
  AND tgt.started_at IS NOT NULL;

-- 3) Refresh the timeline view: the derived delivery_status branch gains
--    EARLY. Raw-timestamp derivation remains the fallback that protects
--    the dashboard from a failed stored-outcome write.
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
    WHEN o.telegram_accepted_at >= COALESCE(p.target_round_started_at, lrs.began_at)
      THEN 'LATE'
    WHEN EXTRACT(EPOCH FROM (
        COALESCE(p.target_round_started_at, lrs.began_at) - o.telegram_accepted_at
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
  ON lrs.game_id = p.target_game_id;

COMMENT ON VIEW prediction_delivery_timeline IS
  'End-to-end forensics: stored delivery_outcome is a CACHE; delivery_status derives from the authoritative raw timestamps when the cache is missing';
