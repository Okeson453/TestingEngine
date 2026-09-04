-- Migration 0010: live_event_log (append-only observability).
-- Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §9.2, §9.8
CREATE TABLE IF NOT EXISTS live_event_log (
  id                  bigserial PRIMARY KEY,
  correlation_id      text NOT NULL,
  event_kind          text NOT NULL CHECK (event_kind IN ('BG','ED','PG','POLL_RECONCILE','BOOT_BACKFILL')),
  game_id             text NOT NULL,
  payload             jsonb NOT NULL,
  received_at         timestamptz NOT NULL DEFAULT now(),
  processed_at        timestamptz,
  processor_latency_ms int,
  sla_violated        boolean NOT NULL DEFAULT false,
  notes               text
);
CREATE INDEX IF NOT EXISTS live_event_log_game_id_idx ON live_event_log (game_id);
CREATE INDEX IF NOT EXISTS live_event_log_correlation_id_idx ON live_event_log (correlation_id);
CREATE INDEX IF NOT EXISTS live_event_log_received_at_idx ON live_event_log (received_at DESC);
