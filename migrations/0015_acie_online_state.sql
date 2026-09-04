-- Migration 0015: Durable ACIE online adaptive state (Postgres).
-- Spec: ACIE_Combined_Upgrade_Recommendations.md §5.1
-- Single current row + optional history for rollback. Best-effort writes.

CREATE TABLE IF NOT EXISTS acie_online_state (
  id                  bigserial PRIMARY KEY,
  snapshot_version    int NOT NULL DEFAULT 1,
  observation_count   int NOT NULL DEFAULT 0,
  ewma_hit_rate       double precision,
  ewma_brier          double precision,
  last_drift_detected boolean NOT NULL DEFAULT false,
  consecutive_losses  int NOT NULL DEFAULT 0,
  payload             jsonb NOT NULL,
  created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS acie_online_state_created_at_idx
  ON acie_online_state (created_at DESC);

-- Keep only recent history (operator can prune; soft guidance via comment).
COMMENT ON TABLE acie_online_state IS
  'ACIE OnlineAdaptiveState snapshots. Latest row is loaded on worker boot.';
