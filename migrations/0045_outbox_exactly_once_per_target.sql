-- Migration 0045 (sep 12 pass 4): structural exactly-once delivery per target.
--
-- Prod window 10:55Z showed two OUTBOX_DISPATCH deliveries 1.5s apart after an
-- ED "duplicate pending" — the raw logs could not decide whether one logical
-- prediction was delivered twice or two distinct targets were delivered. The
-- pending_predictions partial unique (one UNMATCHED row per target_game_id)
-- is the only dedup gate; the outbox itself has NO uniqueness, so any code
-- path that slips past the pending gate (e.g. the pending row flipping
-- matched=true between two persists for the same target) can enqueue a second
-- deliverable row for the same round.
--
-- This index makes duplicate logical delivery structurally impossible: at
-- most ONE undelivered prediction notification may exist per target round.
-- Retries (attempt_count) and dead-lettered rows are unaffected; the
-- temporal kill (BG(target) arrival) clears pending/inflight rows for a
-- started round, so a legitimate later prediction for the same target can
-- still enqueue after its predecessor expired.
--
-- NOTE on at-least-once: a worker crash BETWEEN Telegram send-accept and
-- finalize can still redeliver one row on recovery (standard outbox
-- semantics — the send already happened, so not delivering again would lose
-- the signal). Consumer-visible exactly-once requires the Telegram side;
-- within-process racing ED/BG/reconcile/dispatcher cannot produce two
-- undelivered rows for one target anymore.

-- Pre-clean: if historical data already violates the invariant, keep the
-- NEWEST undelivered row per (type, target) and dead-letter the older ones
-- (they are superseded duplicates — delivering both would be the exact bug
-- this migration prevents). Fresh/empty databases skip this entirely.
WITH ranked AS (
  SELECT id,
         row_number() OVER (
           PARTITION BY type, target_game_id
           ORDER BY created_at DESC, id DESC
         ) AS rn
  FROM notification_outbox
  WHERE status IN ('pending', 'inflight')
)
UPDATE notification_outbox o
SET status = 'dead_letter',
    last_error = 'superseded_duplicate: newer undelivered prediction exists for this target'
FROM ranked r
WHERE o.id = r.id AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS notification_outbox_type_target_undelivered_uidx
  ON notification_outbox (type, target_game_id)
  WHERE status IN ('pending', 'inflight');
