-- Migration 0043: index for the temporal half of recoverStale's
-- NO-RESURRECT dead-letter sweep (notification-worker.ts).
--
-- The sweep runs every ~5 s (dispatcher loopTick % 10 at a 500 ms loop).
-- The subquery is bounded to a 10-minute lookback (semantics preserved:
-- the age sweep in the same function removes rows older than 2 min
-- before this one runs, and a <2-min-old prediction's target starts
-- within one round duration, ~30 s). With the bound, this index is a
-- selective seek over the recently-started window instead of a scan of
-- an ever-growing table (live_round_state has NO retention — ~93
-- rows/round-hour persist for the life of the deployment, and no index
-- previously led with began_at).
--
-- The crash_rounds half of the UNION was already covered by
-- crash_rounds_crashed_at_idx (0002).
CREATE INDEX IF NOT EXISTS live_round_state_began_at_idx
  ON live_round_state (began_at)
  WHERE began_at IS NOT NULL;
