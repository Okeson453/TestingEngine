-- Migration 0042: index for the RESULT-AFTER-SIGNAL co-delivery gate
-- (notification-worker.ts, "normal" lane claim query, added post-0041).
--
-- The gate's NOT EXISTS subquery is:
--   SELECT 1 FROM notification_outbox p
--   WHERE p.type = 'prediction'
--     AND p.status IN ('pending', 'inflight')
--     AND p.created_at > now() - interval '30 seconds'
--     AND p.metadata ->> 'sourceGameId' = v.metadata ->> 'gameId'
--
-- No existing index leads with `type`. notification_outbox_pending_idx
-- (status, next_attempt_at, priority) only covers status = 'pending' — the
-- 'inflight' half of this subquery's IN-list cannot use it at all, and even
-- the 'pending' half needs a filter/scan since type isn't a leading column.
-- This runs on every "normal" lane claim tick, so an unindexed access path
-- here degrades as notification_outbox grows, independent of anything else
-- already tuned on this table.
CREATE INDEX IF NOT EXISTS notification_outbox_result_gate_idx
  ON notification_outbox (type, status, created_at)
  WHERE type = 'prediction' AND status IN ('pending', 'inflight');
