-- Crash database performance & storage optimizations
-- Goals: fast daily aggregation, bounded storage, efficient time-series queries

-- 1. Add sum_multipliers to crash_daily so overall weighted average is exact
--    even after raw rounds are purged.
alter table crash_daily
  add column if not exists sum_multipliers numeric(16, 4);

-- 2. Cleanup function: purge raw rounds older than N days.
--    Daily aggregates in crash_daily are preserved forever.
--    Call via the cleanupOldRounds server function (default 30 days).
create or replace function purge_old_crash_rounds(retention_days int default 30)
returns int as $$
declare
  deleted_count int;
begin
  delete from crash_rounds
  where crashed_at < (now() - (retention_days || ' days')::interval);
  get diagnostics deleted_count = row_count;
  return deleted_count;
end;
$$ language plpgsql;

-- 3. Vacuum hint: after heavy ingestion or cleanup, run:
--    VACUUM ANALYZE crash_rounds;
--    (The cleanup server function does this automatically.)
