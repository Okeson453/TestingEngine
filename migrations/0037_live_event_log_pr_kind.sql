-- Migration 0036: allow event_kind 'PR' in live_event_log.
--
-- BUG (found 2026-09-11 audit): prHandler (game-event-handlers.ts) writes an
-- attributable betting-open (prepare) row with event_kind 'PR' on EVERY round.
-- Migration 0014 expanded the CHECK to BG/ED/PG/POLL_RECONCILE/BOOT_BACKFILL/
-- PREDICT/VALIDATE but never added 'PR'. Every PR insert was therefore
-- rejected by live_event_log_event_kind_check: zero PR observability rows in
-- production plus one failed round-trip and a warn log per round (~93/hour).
--
-- PR stays observability-only: no temporal kill, no began_at write, no
-- registry write (sep 11 root-cause fix — see game-event-handlers.ts).

ALTER TABLE live_event_log
  DROP CONSTRAINT IF EXISTS live_event_log_event_kind_check;

ALTER TABLE live_event_log
  ADD CONSTRAINT live_event_log_event_kind_check
  CHECK (event_kind IN (
    'BG', 'ED', 'ED_RECEIVED', 'PG', 'POLL_RECONCILE', 'BOOT_BACKFILL',
    'PREDICT', 'VALIDATE', 'PR'
  ));
