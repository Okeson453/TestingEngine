-- Strengthen the prediction ↔ round ↔ validation correlation.
--
-- The contract enforced here is the durable 1:1:1 invariant the worker relies
-- on for correctness across crashes, restarts, polling duplicates, and
-- cycles that discover more than one new round at a time:
--
--   exactly 1 target game_id  ↔  exactly 1 prediction  ↔  exactly 1 validation
--
--   * `pending_predictions.target_game_id` — set when a pending row is
--     matched to a freshly-ingested round, so the mapping survives a worker
--     restart that runs validation again.  NULL while the prediction is still
--     waiting for its target.
--
--   * `prediction_validations (game_id)` — UNIQUE — a round can only ever
--     validate one prediction.  Combined with the existing UNIQUE(prediction_id)
--     this gives the bidirectional guarantee the spec requires.
--
--   * `prediction_validations (prediction_id)` — already UNIQUE on
--     prediction_id (0005_prediction_validation.sql), kept here as a comment
--     so the contract is grep-able.
--
--   * `pending_predictions (target_game_id)` — partial UNIQUE so at most one
--     pending row targets any given game_id (defends against a future code
--     change that mistakenly points two predictions at the same round).
--
-- These are additive only: no existing rows are rewritten.  Backfill is a
-- best-effort no-op for environments that already have validated rows; new
-- deployments gain the columns in their natural NULL state.
--
-- This migration does NOT change any retention policy.  Prediction-validation
-- history is permanent; the daily target (20–500) is an operating target only.

alter table pending_predictions
  add column if not exists target_game_id text;

-- Bidirectional uniqueness for prediction_validations:
--   * one validation per prediction  (existing)
--   * one validation per game_id      (new — added here)
-- prediction_validations already has UNIQUE(prediction_id); we add
-- UNIQUE(game_id) so a single round cannot validate two predictions.
do $$
begin
  if not exists (
    select 1 from pg_indexes
     where schemaname = current_schema()
       and tablename  = 'prediction_validations'
       and indexname  = 'prediction_validations_game_id_key'
  ) then
    alter table prediction_validations
      add constraint prediction_validations_game_id_key unique (game_id);
  end if;
end $$;

-- Defensive partial unique index on pending_predictions: at most one unmatched
-- pending row can target any given game_id.  After a row is matched we set
-- matched=true so the index ignores it and a re-validated round is free to
-- re-appear (it won't, thanks to the UNIQUE above — defense in depth).
create unique index if not exists pending_predictions_target_game_id_unmatched_uidx
  on pending_predictions (target_game_id)
  where matched = false and target_game_id is not null;

-- Lookup index for the new correlation query (oldest unmatched pending
-- ordered by requested_at).
create index if not exists pending_predictions_unmatched_requested_idx
  on pending_predictions (requested_at)
  where matched = false and target_game_id is null;
