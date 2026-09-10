-- 0026: durable feedback jobs (fix plan Phase 5).
--
-- Previously the feedback claim (feedback_applied_at = now()) doubled as both
-- the claim AND the completion marker — a crash between claim and successful
-- learning marked feedback complete without it having been applied. The
-- process-local processedIds Set was never safe as cross-worker idempotency.
--
-- feedback_jobs is the durable state machine:
--   PENDING -> PROCESSING -> COMPLETED
--                  |-> PENDING (retry, last_error set)
-- The job row is inserted in the SAME transaction as the validation insert,
-- so the job exists durably before any feedback execution begins.

create table if not exists feedback_jobs (
  id            serial primary key,
  prediction_id text not null unique,
  status        text not null default 'PENDING'
                check (status in ('PENDING', 'PROCESSING', 'COMPLETED')),
  attempt_count int not null default 0,
  claimed_at    timestamptz,
  completed_at  timestamptz,
  last_error    text,
  worker_epoch  bigint,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);

create index if not exists feedback_jobs_claim_idx
  on feedback_jobs (status, created_at)
  where status = 'PENDING';

create index if not exists feedback_jobs_stale_processing_idx
  on feedback_jobs (claimed_at)
  where status = 'PROCESSING';

COMMENT ON TABLE feedback_jobs IS
  'Durable feedback work queue. prediction_id UNIQUE anchors 1:1 with prediction_validations. Completion is recorded ONLY after the learning pipeline succeeds.';
