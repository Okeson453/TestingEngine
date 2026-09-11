-- Migration 0040: bounded forensic reconcile scans + stuck-feedback SLA scan.
--
-- PRODUCTION 2026-09-11: slow_query_ms=700-757 on the general pool for the
-- reconcileForensicOutcomes() sweep, recurring every ~60s, with
-- pool_acquire_ms=135-164 and waiting=1 on a 5-connection general pool —
-- directly delaying BG→N+1 prediction persistence.
--
-- The sweep is now TWO bounded scans (see delivery-forensics.ts):
--   REPAIR: NULL/UNKNOWN delivery_outcome within 24h
--   AUDIT:  ON_TIME/EARLY delivery_outcome within 15 minutes
--           (classification is immutable ~1 min after delivery, once
--           BG(N) backfills target start; longer re-checks are waste)
--
-- Partial indexes match each scan exactly so both are index-ordered
-- range scans with LIMIT applied directly.

CREATE INDEX IF NOT EXISTS notification_outbox_forensic_repair_idx
  ON notification_outbox (delivered_at DESC NULLS LAST)
  WHERE type = 'prediction'
    AND status = 'delivered'
    AND telegram_accepted_at IS NOT NULL
    AND (delivery_outcome IS NULL OR delivery_outcome = 'UNKNOWN');

CREATE INDEX IF NOT EXISTS notification_outbox_forensic_audit_idx
  ON notification_outbox (delivered_at DESC NULLS LAST)
  WHERE type = 'prediction'
    AND status = 'delivered'
    AND telegram_accepted_at IS NOT NULL
    AND delivery_outcome IN ('ON_TIME', 'EARLY');

-- Stuck-feedback sweep (sweepStuckFeedback, every 30s) + the
-- feedback_not_applied_within_sla invariant both scan:
--   WHERE feedback_applied_at IS NULL AND feedback_skip_reason IS NULL
--     AND resolved_at < now() - SLA
--   ORDER BY resolved_at ASC
-- Without a matching index this degrades to a full scan of
-- prediction_validations on the general pool, twice a minute.
CREATE INDEX IF NOT EXISTS prediction_validations_stuck_feedback_idx
  ON prediction_validations (resolved_at)
  WHERE feedback_applied_at IS NULL
    AND feedback_skip_reason IS NULL;
