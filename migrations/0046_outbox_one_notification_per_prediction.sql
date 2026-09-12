-- Migration 0046 (sep 12 pass 5): one logical notification per prediction.
--
-- The 11:23Z window reported prediction 1a8ecff4 "delivered twice with a
-- different notification ID and target/source n/a". That second row is the
-- type='validation' WIN/LOSS result notification (legitimate — one SIGNAL +
-- one RESULT per prediction), but nothing in the schema prevented a genuine
-- duplicate either: the 0045 partial unique only dedupes by target while
-- undelivered, and a re-persist after the pending row flipped matched=true
-- could enqueue a second SIGNAL row for the same prediction.
--
-- These indexes make per-prediction idempotency structural:
--   at most ONE prediction row and ONE validation row may exist per
--   metadata->>'predictionId'. The persist CTE (bare ON CONFLICT DO NOTHING)
--   and the validation enqueue (already ON CONFLICT DO NOTHING) read a
--   conflict as 0 rows — callers already treat that as kind=duplicate/skip.

-- Pre-clean historical duplicates the same way 0045 did: keep the newest
-- undelivered row per (type, predictionId), dead-letter older ones.
WITH ranked AS (
  SELECT id,
         row_number() OVER (
           PARTITION BY type, (metadata->>'predictionId')
           ORDER BY created_at DESC, id DESC
         ) AS rn
  FROM notification_outbox
  WHERE metadata ? 'predictionId'
    AND status IN ('pending', 'inflight')
)
UPDATE notification_outbox o
SET status = 'dead_letter',
    last_error = 'superseded_duplicate: newer undelivered row exists for this prediction'
FROM ranked r
WHERE o.id = r.id AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS notification_outbox_prediction_signal_uidx
  ON notification_outbox ((metadata->>'predictionId'))
  WHERE type = 'prediction' AND status IN ('pending', 'inflight');

CREATE UNIQUE INDEX IF NOT EXISTS notification_outbox_validation_uidx
  ON notification_outbox ((metadata->>'predictionId'))
  WHERE type = 'validation' AND status IN ('pending', 'inflight');
