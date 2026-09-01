-- Prediction validation tables — additive only, no existing table touched.
-- Permanent training/validation dataset for the prediction engine.
-- No automatic deletion, truncation, or retention limits.

-- Pending predictions queue: predictions generated before their outcome is known.
create table if not exists pending_predictions (
  id            serial primary key,
  prediction_id text not null unique,
  target_multiplier numeric(12,4) not null default 1.30,
  probability     numeric(12,6),
  confidence      numeric(12,6),
  regime_name     text,
  regime_confidence numeric(12,6),
  reasoning       text[],
  feature_summary jsonb,
  model_version   text,
  requested_at    timestamptz not null default now(),
  matched         boolean not null default false,
  matched_game_id text,
  matched_at      timestamptz
);

create index if not exists pending_predictions_matched_idx
  on pending_predictions (matched, requested_at);

-- Permanent validation records — insert-only, never updated or deleted.
create table if not exists prediction_validations (
  id                serial primary key,
  prediction_id     text not null unique,
  game_id           text not null,
  target_multiplier numeric(12,4) not null,
  predicted_probability numeric(12,6),
  predicted_confidence  numeric(12,6),
  actual_multiplier     numeric(12,4) not null,
  result            text not null check (result in ('WIN', 'LOSS')),
  model_version     text,
  regime_name       text,
  regime_confidence numeric(12,6),
  reasoning         text[],
  feature_summary   jsonb,
  requested_at      timestamptz not null,
  resolved_at       timestamptz not null default now(),
  created_at        timestamptz not null default now()
);

create index if not exists prediction_validations_resolved_at_idx
  on prediction_validations (resolved_at desc);
create index if not exists prediction_validations_game_id_idx
  on prediction_validations (game_id);
create index if not exists prediction_validations_result_idx
  on prediction_validations (result);

-- Daily target configuration — single row, operator-editable.
create table if not exists validation_config (
  id            serial primary key,
  daily_target  int not null default 100 check (daily_target >= 20 and daily_target <= 500),
  updated_at    timestamptz not null default now()
);

-- Seed default daily target if table is empty.
insert into validation_config (daily_target)
select 100
  where not exists (select 1 from validation_config);
