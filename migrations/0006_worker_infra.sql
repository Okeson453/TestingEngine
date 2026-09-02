-- Infrastructure for the autonomous background prediction worker.
-- The worker performs BC.Game polling, prediction generation, WIN/LOSS
-- validation and persistence as a SERVER-SIDE process that does not depend
-- on any browser/dashboard/React-effect/refreshDashboard() call.
--
-- Two tables back that contract:
--   * worker_locks   — distributed lock (TTL + heartbeat) so multiple worker
--                       instances/processes/pods never duplicate predictions
--                       or validate the same round.
--   * worker_state   — heartbeat / last-sync summary written each cycle and
--                       read by the dashboard to show engine health without
--                       triggering any prediction work itself.

-- Distributed lock row. A single row per lock_key; ownership is "claimed"
-- atomically only when the existing lock has expired, which lets a crashed
-- worker be recovered by a successor after the TTL lapses.
create table if not exists worker_locks (
  lock_key     text primary key,
  owner_id     text not null,
  acquired_at  timestamptz not null default now(),
  expires_at   timestamptz not null,
  heartbeat_at timestamptz not null default now()
);

create index if not exists worker_locks_lock_key_idx
  on worker_locks (lock_key);

-- Key/value scratch space for worker heartbeat + last-sync feed data.
-- The dashboard reads these to render live status; it never writes them.
-- Upserts are performed inline from the worker (portable across PGLite/Neon),
-- so no stored procedure is required.
create table if not exists worker_state (
  key        text primary key,
  value      text,
  updated_at timestamptz not null default now()
);
