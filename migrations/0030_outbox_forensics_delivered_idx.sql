-- Migration 0030: Index for delivery-forensics delivered-row lookup.
--
-- delivery-forensics.ts queries notification_outbox with
--   type = 'prediction' AND target_game_id = $1
--   AND status = 'delivered' AND telegram_accepted_at IS NOT NULL
-- per crash event (BG reconcile). The only existing index on target_game_id
-- (0027, outbox_prediction_pending_target_idx) is partial on
-- status IN ('pending','inflight') and deliberately excludes delivered rows,
-- so this query fell back to a status-based scan that degrades as delivered
-- predictions accumulate. Observed slow_query_ms=533 on the general pool
-- (max=5) with waiting=1 — a direct pool-exhaustion contributor.
--
-- Partial index matches the query exactly; delivered predictions are the
-- only rows that grow unboundedly, and they are the only rows indexed here.

CREATE INDEX IF NOT EXISTS notification_outbox_forensics_idx
  ON notification_outbox (target_game_id, telegram_accepted_at)
  WHERE type = 'prediction' AND status = 'delivered';
