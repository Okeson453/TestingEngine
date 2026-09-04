-- Migration 0010: Notification Outbox Table
-- Required for: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §10
-- Implements durable, asynchronous notification delivery with:
-- - Idempotency (no duplicate notifications)
-- - Retry with exponential backoff
-- - Dead-letter handling
-- - Delivery latency tracking
-- - Transactional atomicity with prediction persistence

-- Notification outbox table for durable Telegram delivery
CREATE TABLE IF NOT EXISTS notification_outbox (
  id SERIAL PRIMARY KEY,
  notification_id UUID NOT NULL UNIQUE,
  type TEXT NOT NULL CHECK (type IN ('prediction', 'validation', 'alert', 'summary')),
  content TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}',
  status TEXT NOT NULL CHECK (status IN ('pending', 'delivered', 'failed', 'dead_letter')) DEFAULT 'pending',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  delivered_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  priority INTEGER NOT NULL DEFAULT 0
);

-- Index for efficient retrieval of pending notifications
CREATE INDEX IF NOT EXISTS notification_outbox_pending_idx 
ON notification_outbox (status, next_attempt_at, priority) 
WHERE status = 'pending';

-- Index for notification lookup by ID
CREATE INDEX IF NOT EXISTS notification_outbox_id_idx 
ON notification_outbox (notification_id);

-- Index for status-based queries
CREATE INDEX IF NOT EXISTS notification_outbox_status_idx 
ON notification_outbox (status);

-- Index for priority-based ordering
CREATE INDEX IF NOT EXISTS notification_outbox_priority_idx 
ON notification_outbox (priority DESC, created_at ASC) 
WHERE status = 'pending';

-- Index for created_at for cleanup and analytics
CREATE INDEX IF NOT EXISTS notification_outbox_created_idx 
ON notification_outbox (created_at);

-- Index for correlation with predictions
CREATE INDEX IF NOT EXISTS notification_outbox_prediction_idx 
ON notification_outbox ((metadata->>'predictionId')) 
WHERE metadata ? 'predictionId';

-- Trigger to update updated_at on changes
CREATE OR REPLACE FUNCTION update_notification_outbox_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS notification_outbox_updated_trigger ON notification_outbox;
CREATE TRIGGER notification_outbox_updated_trigger
  BEFORE UPDATE ON notification_outbox
  FOR EACH ROW
  EXECUTE FUNCTION update_notification_outbox_updated_at();

-- View for monitoring outbox health
CREATE OR REPLACE VIEW notification_outbox_stats AS
SELECT 
  status,
  COUNT(*) as count,
  MIN(created_at) as oldest_created,
  MAX(created_at) as newest_created,
  AVG(attempt_count) as avg_attempts,
  AVG(EXTRACT(EPOCH FROM (delivered_at - created_at)) * 1000) as avg_delivery_latency_ms
FROM notification_outbox
GROUP BY status;

-- View for pending notifications that need attention
CREATE OR REPLACE VIEW notification_outbox_pending_old AS
SELECT 
  id,
  notification_id,
  type,
  created_at,
  next_attempt_at,
  attempt_count,
  last_error,
  priority
FROM notification_outbox
WHERE status = 'pending' 
  AND next_attempt_at < now() - INTERVAL '5 minutes'
ORDER BY next_attempt_at ASC;

-- Function to get outbox health summary
CREATE OR REPLACE FUNCTION get_notification_outbox_health()
RETURNS JSONB AS $$
DECLARE
  result JSONB;
  stats JSONB;
  pending_old JSONB;
BEGIN
  -- Get overall stats
  SELECT jsonb_agg(
    jsonb_build_object(
      'status', status,
      'count', count,
      'oldest_created', oldest_created,
      'newest_created', newest_created,
      'avg_attempts', COALESCE(avg_attempts::numeric, 0),
      'avg_delivery_latency_ms', COALESCE(avg_delivery_latency_ms::numeric, 0)
    )
  ) INTO stats
  FROM notification_outbox_stats;

  -- Get old pending notifications (stuck)
  SELECT jsonb_agg(
    jsonb_build_object(
      'id', id,
      'notification_id', notification_id,
      'type', type,
      'created_at', created_at,
      'next_attempt_at', next_attempt_at,
      'attempt_count', attempt_count,
      'last_error', last_error,
      'priority', priority
    )
  ) INTO pending_old
  FROM notification_outbox_pending_old;

  result := jsonb_build_object(
    'stats', stats,
    'pending_old', pending_old,
    'healthy', (
      SELECT COUNT(*) = 0 
      FROM notification_outbox_pending_old
    )
  );

  RETURN result;
END;
$$ LANGUAGE plpgsql;

COMMENT ON TABLE notification_outbox IS 'Durable outbox for Telegram notifications - ensures notifications are delivered even if the application crashes';
COMMENT ON COLUMN notification_outbox.notification_id IS 'Unique identifier for the notification';
COMMENT ON COLUMN notification_outbox.type IS 'Type of notification: prediction, validation, alert, or summary';
COMMENT ON COLUMN notification_outbox.content IS 'The message content to be sent';
COMMENT ON COLUMN notification_outbox.metadata IS 'Additional metadata for correlation and debugging';
COMMENT ON COLUMN notification_outbox.status IS 'Current status: pending, delivered, failed, or dead_letter';
COMMENT ON COLUMN notification_outbox.attempt_count IS 'Number of delivery attempts made';
COMMENT ON COLUMN notification_outbox.next_attempt_at IS 'When the next delivery attempt should be made';
COMMENT ON COLUMN notification_outbox.priority IS 'Priority level (higher = more important)';