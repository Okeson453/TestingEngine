-- Daily aggregated crash stats. One row per calendar day (UTC).
-- Recomputed automatically when new rounds are ingested.
create table if not exists crash_daily (
  date               date primary key,
  total_rounds       int not null default 0,
  avg_multiplier     numeric(12, 4),
  median_multiplier  numeric(12, 4),
  highest_multiplier numeric(12, 4),
  lowest_multiplier  numeric(12, 4),
  low_count          int not null default 0,   -- multiplier < 2
  high_count         int not null default 0,   -- multiplier >= 2 and < 10
  moon_count         int not null default 0,   -- multiplier >= 10
  updated_at         timestamptz not null default now()
);

create index if not exists crash_daily_date_idx
  on crash_daily (date desc);
