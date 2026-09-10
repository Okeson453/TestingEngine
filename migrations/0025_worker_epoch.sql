-- 0025: worker fencing epoch (fix plan Phase 1).
--
-- Locks coordinate workers; fencing prevents stale workers from causing
-- damage. epoch is monotonically incremented on every ownership change
-- (acquire / takeover), so ownership history is durable and auditable.
-- The takeover UPDATE must PROVE the current lease is expired — an
-- unconditional DELETE FROM worker_locks is no longer used anywhere.

ALTER TABLE worker_locks
  ADD COLUMN IF NOT EXISTS epoch BIGINT NOT NULL DEFAULT 0;

-- expires_at continues to serve as the lease expiry (extended by heartbeats).
COMMENT ON COLUMN worker_locks.epoch IS
  'Fencing token: +1 on every ownership change. Stale workers detected via heartbeat zero-row updates.';
