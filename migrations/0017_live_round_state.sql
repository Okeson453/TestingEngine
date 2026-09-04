-- Migration 0017: Explicit live round lifecycle separate from historical crash_rounds.
-- Spec: TestingEngine_Comprehensive_Diagnosis_and_Solution.md §6
--
-- DISCOVERED → STARTED → RUNNING → ENDED → RECONCILED
-- Predictor asks "has target N+1 actually started?" via this table,
-- not merely "does a crash_rounds row exist?"

CREATE TABLE IF NOT EXISTS live_round_state (
  game_id           text PRIMARY KEY,
  lifecycle         text NOT NULL
                    CHECK (lifecycle IN (
                      'DISCOVERED', 'STARTED', 'RUNNING', 'ENDED', 'RECONCILED'
                    )),
  began_at          timestamptz,
  crashed_at        timestamptz,
  multiplier        double precision,
  source            text NOT NULL DEFAULT 'unknown'
                    CHECK (source IN ('socket', 'poll', 'history', 'unknown')),
  correlation_id    text,
  updated_at        timestamptz NOT NULL DEFAULT now(),
  created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS live_round_state_lifecycle_idx
  ON live_round_state (lifecycle, updated_at DESC);

CREATE INDEX IF NOT EXISTS live_round_state_updated_idx
  ON live_round_state (updated_at DESC);

COMMENT ON TABLE live_round_state IS
  'Live lifecycle for crash rounds. Distinct from historical crash_rounds storage.';
