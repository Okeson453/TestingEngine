-- Public BC.Game Crash rounds. Unowned: world-readable tracker data, no user_id.
create table if not exists crash_rounds (
  id          serial primary key,
  game_id     text not null unique,
  multiplier  numeric(12, 4) not null,
  hash        text,
  salt        text,
  began_at    timestamptz,
  crashed_at  timestamptz not null,
  ingested_at timestamptz not null default now(),
  constraint crash_rounds_multiplier_valid
    check (multiplier >= 1 and multiplier <= 1000000)
);

create index if not exists crash_rounds_crashed_at_idx
  on crash_rounds (crashed_at desc);
