-- Migration 0020: Drop unused notification_outbox priority index.
-- Spec: TestingEngine-Complete-Issues-and-Recommendations.md A7
--
-- The live dispatcher's claim query orders by `next_attempt_at ASC,
-- id ASC` only; the priority index is unused by the live path. The
-- shadow outbox path in `src/lib/notifications/outbox.ts` does set
-- `priority` for compatibility, but no production claim uses it. We
-- drop the index to remove the write-amplification cost on every
-- INSERT (the index was a partial index on `pending` rows, which is
-- rewritten on every outbox insert + every status transition).
-- The `priority` column itself is preserved for forward compatibility.

DROP INDEX IF EXISTS notification_outbox_priority_idx;
