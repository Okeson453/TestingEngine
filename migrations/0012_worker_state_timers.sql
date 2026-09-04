-- Migration 0011: Worker state latency + correlation columns.
-- Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.2, §7.9, §11
ALTER TABLE worker_state
  ADD COLUMN IF NOT EXISTS last_prediction_latency_ms      int,
  ADD COLUMN IF NOT EXISTS last_persistence_latency_ms     int,
  ADD COLUMN IF NOT EXISTS last_notify_queue_latency_ms    int,
  ADD COLUMN IF NOT EXISTS last_telegram_api_latency_ms    int,
  ADD COLUMN IF NOT EXISTS last_total_delivery_latency_ms  int,
  ADD COLUMN IF NOT EXISTS last_target_game_id             text,
  ADD COLUMN IF NOT EXISTS last_target_began_at            timestamptz,
  ADD COLUMN IF NOT EXISTS last_bg_event_received_at       timestamptz,
  ADD COLUMN IF NOT EXISTS last_sla_violation_at           timestamptz,
  ADD COLUMN IF NOT EXISTS last_bg_to_recv_lag_ms_p50      int,
  ADD COLUMN IF NOT EXISTS last_bg_to_recv_lag_ms_p95      int,
  ADD COLUMN IF NOT EXISTS last_prediction_correlation_id  text;

ALTER TABLE worker_locks ADD COLUMN IF NOT EXISTS correlation_id text;

-- Index for correlation_id lookups on live_event_log (idempotent).
CREATE INDEX IF NOT EXISTS live_event_log_correlation_idx
  ON live_event_log (correlation_id);

-- Index for recent BG event monitoring.
CREATE INDEX IF NOT EXISTS live_event_log_event_kind_received_idx
  ON live_event_log (event_kind, received_at DESC);

-- Outbox dispatch throughput index.
CREATE INDEX IF NOT EXISTS notification_outbox_status_next_attempt_idx
  ON notification_outbox (status, next_attempt_at) WHERE status = 'pending';

-- Worker state by key + value for dashboard.
CREATE INDEX IF NOT EXISTS worker_state_key_idx
  ON worker_state (key);
