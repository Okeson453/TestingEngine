-- Migration 0035: Index for the forensic reconciliation sweep (introduced 4fbd759).
--
-- reconcileForensicOutcomes() queries notification_outbox with
--   type = 'prediction' AND status = 'delivered'
--   AND telegram_accepted_at IS NOT NULL
--   AND (delivery_outcome IS NULL OR delivery_outcome IN (...))
--   ORDER BY delivered_at DESC NULLS LAST
--   LIMIT 200
--
-- Migration 0030 indexed (target_game_id, telegram_accepted_at) for the
-- PER-TARGET delivered lookup — a different query shape. The global sweep
-- orders by delivered_at, which no index supports, so every pass degrades
-- to a full scan + sort of all delivered predictions. Observed
-- slow_query_ms=720-725 on the general pool, recurring every 30 ticks
-- (~60s) in the Railway worker log — a standing general-pool occupant that
-- directly competes with recovery/maintenance lanes.
--
-- Partial index matches the sweep exactly (delivered rows are the only
-- unbounded growth); with LIMIT 200 the sweep becomes a top-200 index scan.

CREATE INDEX IF NOT EXISTS notification_outbox_reconcile_idx
  ON notification_outbox (delivered_at DESC NULLS LAST)
  WHERE type = 'prediction' AND status = 'delivered' AND telegram_accepted_at IS NOT NULL;
