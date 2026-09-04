-- Migration 0009: Socket event discovery table for observability
-- Required for: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §13.5
-- This table captures discovered event types and payloads for verification
-- that we're subscribed to the correct BC.Game events (bg, pg, ed)

CREATE TABLE IF NOT EXISTS socket_event_discovery (
  id          bigserial primary key,
  event_name  text NOT NULL,
  payload     jsonb NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now()
);

-- Index for efficient lookup of events by type
CREATE INDEX IF NOT EXISTS socket_event_discovery_event_name_idx
  ON socket_event_discovery (event_name, received_at DESC);

-- Index for finding the most recent events
CREATE INDEX IF NOT EXISTS socket_event_discovery_received_at_idx
  ON socket_event_discovery (received_at DESC);

-- Unique constraint to avoid duplicate event logging for the same event type
-- We only need to discover each event type once
CREATE UNIQUE INDEX IF NOT EXISTS socket_event_discovery_event_name_unique_idx
  ON socket_event_discovery (event_name)
  WHERE event_name NOT LIKE '\_%' ESCAPE '\';

-- View to see which BC.Game events we've discovered
CREATE OR REPLACE VIEW discovered_bcgame_events AS
SELECT 
  event_name,
  COUNT(*) as discovery_count,
  MAX(received_at) as last_discovered_at,
  jsonb_agg(payload) as sample_payloads
FROM socket_event_discovery
GROUP BY event_name
ORDER BY last_discovered_at DESC;

-- Function to check if we've discovered the required events
CREATE OR REPLACE FUNCTION check_required_events_discovered()
RETURNS TABLE (event_name text, discovered boolean, first_seen timestamptz, last_seen timestamptz) AS $$
BEGIN
  RETURN QUERY
  WITH required_events AS (
    SELECT unnest(ARRAY['bg', 'pg', 'ed']) as event_name
  )
  SELECT 
    re.event_name,
    sed.event_name IS NOT NULL as discovered,
    MIN(sed.received_at) as first_seen,
    MAX(sed.received_at) as last_seen
  FROM required_events re
  LEFT JOIN socket_event_discovery sed ON re.event_name = sed.event_name
  GROUP BY re.event_name, sed.event_name;
END;
$$ LANGUAGE plpgsql;

-- Function to get event discovery summary
CREATE OR REPLACE FUNCTION get_event_discovery_summary()
RETURNS jsonb AS $$
DECLARE
  result jsonb;
  required_events jsonb;
  discovered_events jsonb;
BEGIN
  -- Get required events status
  SELECT jsonb_agg(
    jsonb_build_object(
      'event', event_name,
      'discovered', discovered,
      'first_seen', first_seen,
      'last_seen', last_seen
    )
  ) INTO required_events
  FROM check_required_events_discovered();

  -- Get all discovered events
  SELECT jsonb_agg(
    jsonb_build_object(
      'event', event_name,
      'count', discovery_count,
      'last_seen', last_discovered_at
    )
  ) INTO discovered_events
  FROM discovered_bcgame_events;

  result := jsonb_build_object(
    'required_events', required_events,
    'all_discovered_events', discovered_events,
    'all_required_discovered', (
      SELECT bool_and(discovered) FROM check_required_events_discovered()
    )
  );

  RETURN result;
END;
$$ LANGUAGE plpgsql;

COMMENT ON TABLE socket_event_discovery IS 'Observability table for BC.Game Socket.IO event discovery - tracks which events we receive from the WebSocket connection';
COMMENT ON COLUMN socket_event_discovery.event_name IS 'The name of the Socket.IO event (e.g., bg, pg, ed)';
COMMENT ON COLUMN socket_event_discovery.payload IS 'Sample payload received for this event type';
COMMENT ON COLUMN socket_event_discovery.received_at IS 'When this event was first received';