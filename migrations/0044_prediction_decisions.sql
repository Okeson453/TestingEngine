-- Pass 19: durable decision audit for NO_BET vetoes.
--
-- Emitted signals persist full provenance (pending_predictions.feature_summary),
-- but REJECTED decisions persisted nothing — so "is the edge threshold
-- miscalibrated / is the model's rejected distribution actually profitable?"
-- could never be answered from data. One row per evaluated round here,
-- joined against crash_rounds.multiplier for the realized 1.30x outcome:
--
--   SELECT d.game_id, d.probability, d.veto_reason,
--          (c.multiplier >= d.target_multiplier) AS would_win
--   FROM prediction_decisions d
--   JOIN crash_rounds c ON c.game_id = d.game_id
--   ORDER BY d.decided_at DESC;
--
-- Walk-forward / threshold-sweep evaluation is then a pure query. The
-- worker does NOT require this table at boot (not in REQUIRED_TABLES):
-- before this migration runs, the audit write degrades to log-only.

create table if not exists prediction_decisions (
  id            bigint generated always as identity primary key,
  game_id       text not null unique,
  source_game_id text,
  decided_at    timestamptz not null default now(),
  target_multiplier numeric(12, 4) not null,
  probability   double precision not null,
  confidence    double precision not null,
  fair_probability double precision not null,
  min_edge      double precision not null,
  need_probability double precision not null,
  edge          double precision not null,
  veto_reason   text,
  decision      text not null,
  mode          text,
  regime        text,
  model_probabilities jsonb,
  ensemble_disagreement double precision,
  used_calibrated boolean
);

create index if not exists prediction_decisions_decided_at_idx
  on prediction_decisions (decided_at desc);
