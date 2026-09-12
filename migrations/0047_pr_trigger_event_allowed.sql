-- Migration 0047: allow trigger_event 'PR' (PR-primary N+1 predictions).
--
-- PASS 8 ROOT CAUSE (17:39:48 prod window): PR-primary durable persistence
-- failed deterministically at ~1 RTT (prediction ready 48.313 → failure
-- 48.493 = one Neon round trip = the server ANSWERED with an error), while
-- the identical statement from the BG path succeeded 7.2s later.
--
-- Cause: migration 0039's CHECK enumerates ('BG','ED','POLL') only. The
-- PR-primary promotion (e0d2d5b) writes trigger_event='PR', so EVERY PR
-- persist was rejected server-side with SQLSTATE 23514 (check_violation).
-- BG recomputing ~7.2s later was the implicit recovery — exactly the
-- 7-second fallback the PR path exists to eliminate.
--
-- Railway strips JSON log fields, so the 23514 never reached the raw logs
-- (failureReason was a JSON field). The predictor catch block now inlines
-- name/message/SQLSTATE in the message string as well.

ALTER TABLE pending_predictions
  DROP CONSTRAINT IF EXISTS pending_predictions_trigger_event_check;

ALTER TABLE pending_predictions
  ADD CONSTRAINT pending_predictions_trigger_event_check
  CHECK (trigger_event IS NULL OR trigger_event IN ('BG', 'ED', 'POLL', 'PR'));
