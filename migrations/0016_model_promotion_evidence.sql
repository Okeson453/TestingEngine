-- Migration 0016: Gate for advanced ensemble candidates (ACIE §6.1).
-- Models stay OFF until an approved row exists. Prevents silent enablement.
CREATE TABLE IF NOT EXISTS model_promotion_evidence (
  id              bigserial PRIMARY KEY,
  model_name      text NOT NULL,
  holdout_window  text,
  win_rate_delta  double precision,
  notes           text,
  approved_at     timestamptz,
  created_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (model_name)
);

COMMENT ON TABLE model_promotion_evidence IS
  'ACIE candidate models require an approved_at row before EnsembleFlags enable them.';
