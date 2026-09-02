# TestingEngine

BcTracker — autonomous prediction engine for BC.Game Crash multiplier rounds.

## Overview

CrashWave records BC.Game Crash multipliers in sequence — live history, stats,
and streaks. Tracking only. The prediction engine runs as an **autonomous
background worker** that polls BC.Game history, detects new rounds, generates
predictions, validates WIN/LOSS outcomes, and persists results to the database
— independent of any browser, dashboard, or Vercel invocation.

## Architecture

Two deployments of the same repository, talking to the same PostgreSQL:

```
   ┌────────────────────┐         ┌────────────────────┐
   │  Railway (worker)  │ writes  │                    │  reads  ┌────────────────────┐
   │  npm run worker    │ ──────► │   PostgreSQL       │ ◄────── │  Vercel (dashboard)│
   │  long-lived, 24/7  │         │   (Neon / Railway) │         │  read-only SSR     │
   │  polls BC.Game 10s │         │                    │         │  TanStack Start    │
   └────────────────────┘         └────────────────────┘         └────────────────────┘
```

### Worker (autonomous background process)

The worker (`scripts/worker.mjs`) runs as a long-lived Node.js process on
Railway (see `railway.toml` / `Procfile`):

- Polls BC.Game history every `PREDICTION_POLL_MS` (default **10 s**)
- Detects new rounds by comparing `game_id` against the DB primary key
- Generates predictions using the existing `PredictionEngine`
- Validates WIN/LOSS outcomes, deterministically 1:1:1 with the round
- Persists all results to PostgreSQL (Neon / Railway Postgres)
- Runs **24/7** — does **not** require a browser, dashboard, or Vercel
  invocation. The dashboard may be closed; the worker keeps going.

```bash
npm run dev          # Local dev (in-process PGLite, worker runs in the Vite dev server)
npm run worker       # Standalone persistent worker (requires DATABASE_URL=Postgres)
```

The Vercel dashboard is **read-only**. It never polls BC.Game, never generates
predictions, never validates — it just reads `worker_state` /
`prediction_validations` / `crash_rounds` and renders them.  No React timer,
no `refreshDashboard` cycle, no client-side execution is required for the
worker to keep operating.

### Database

- **Production**: PostgreSQL via Neon or Railway Postgres (`DATABASE_URL`).
  Both deployments MUST use the same database.
- **Dev/Preview**: PGLite (in-process WASM Postgres).

Migrations live in `migrations/` and are auto-applied on first query
(`src/lib/db.ts` for PGLite, `scripts/migrate.mjs` for `DATABASE_URL`).

### Migrations (in apply order)

| File                            | Purpose                                                  |
|---------------------------------|----------------------------------------------------------|
| `0002_crash_rounds.sql`         | `crash_rounds` table                                     |
| `0003_crash_daily.sql`          | `crash_daily` aggregate table                            |
| `0004_crash_optimize.sql`       | `sum_multipliers`, `purge_old_crash_rounds` (raw only)   |
| `0005_prediction_validation.sql`| `pending_predictions`, `prediction_validations`,         |
|                                 | `validation_config`                                      |
| `0006_worker_infra.sql`         | `worker_locks`, `worker_state`                           |
| `0007_prediction_correlation.sql` | **NEW** — `pending_predictions.target_game_id`,        |
|                                 | `UNIQUE(prediction_validations.game_id)` for the        |
|                                 | durable 1:1:1 invariant                                 |

### Dashboard

Live at `http://localhost:8080/predictions` (dev) or `https://<vercel-app>`:

- Current prediction status (read from `worker_state`)
- Daily progress (target vs. resolved count)
- WIN/LOSS stats and streaks
- Historical validation records
- Worker health (Running/Offline, last sync) — read from `worker_locks` +
  `worker_state`
- Daily target configuration (operator-adjustable, range 20–500; **operating
  target only**, never a database retention limit)

The worker is the **sole owner** of:

- BC.Game polling
- New-round detection
- Prediction generation
- Prediction-to-round correlation
- WIN/LOSS validation
- Persistent state writes (`crash_rounds`, `pending_predictions`,
  `prediction_validations`, `worker_state`)

The dashboard **must never** do any of those — it only reads.

## Prediction ↔ round ↔ validation invariant

```
exactly 1 prediction  ↔  exactly 1 target game_id  ↔  exactly 1 validation
```

Enforced at three layers:

1. **At generation** (`generateAndQueuePrediction`): model input is
   `crash_rounds` with `crashed_at <= now()` (the existing `MAX_HISTORY = 100`
   cap is preserved).  The target round's own multiplier is never fed in.
2. **At correlation** (`validateAgainstNewRounds`): oldest-pending ↔
   oldest-new-round, durable via `pending_predictions.target_game_id`.
   Cycles that discover N rounds in one poll resolve N predictions (capped by
   the number of unmatched pendings).
3. **At persistence** (Postgres): `UNIQUE(prediction_validations.prediction_id)`
   + `UNIQUE(prediction_validations.game_id)` +
   partial unique index on `pending_predictions(target_game_id) WHERE matched = false`.
   A re-run after a crash leaves already-resolved rows untouched
   (`ON CONFLICT DO NOTHING`).

### Error-state contract

- A failed BC.Game / DB cycle leaves `worker_state.last_error` populated and
  `worker_state.last_sync_ok = 0`. It does **not** clear a previous error.
- A fully successful cycle writes `last_sync_at = now`,
  `last_sync_ok = 1`, and clears `last_error` to the empty string.
- A cycle that errored at any step (fetch / insert / validate / generate)
  is recorded as failed even if a later step happened to succeed on an empty
  result set. This prevents a partial cycle from being reported as "ok".

## Server Functions

Server functions are defined in `src/lib/p.ts` and called from client
components via `useMutation` / `useQuery`.

### Important: Server Function ID Base64 Encoding

TanStack Start encodes server function IDs as base64 of
`{"file":"<path>","export":"<name>"}`. The base64 string becomes part of the
URL path (`/_serverFn/<base64id>`).

**Known issue**: If the file path is long enough that the base64 encoding
produces `/` characters, the server's URL parsing (`split('/')[0]`) truncates
the ID, causing "Invalid server function ID" 500 errors for all
browser-side calls.

**Solution**: Keep server function file paths short enough to avoid `/` in the
base64 encoding. The functions must live in `src/lib/p.ts` (or a similarly
short path), not `src/lib/prediction/api.ts`.

### Calling Convention

POST server functions require a `{ data: { ... } }` wrapper:

```typescript
// CORRECT
predictionSetDailyTarget({ data: { target: 50 } })

// INCORRECT
predictionSetDailyTarget({ target: 50 })
```

## Production environment

| Variable                   | Default  | Used by                    | Purpose                                       |
|----------------------------|----------|----------------------------|-----------------------------------------------|
| `DATABASE_URL`             | —        | Vercel **+** Railway       | Postgres connection string                    |
| `PREDICTION_POLL_MS`       | `10000`  | Railway worker only        | BC.Game poll interval (ms)                    |
| `PREDICTION_LOCK_TTL_SEC`  | `60`     | Railway worker only        | Distributed lock TTL (s)                      |
| `PREDICTION_FETCH_PAGES`   | `2`      | Railway worker only        | Pages of BC.Game history per poll             |
| `PG_DATA_PATH`             | `./data/crashwave` | Dev only         | PGLite on-disk dir (never set in production) |

See `.env.example` for the full annotated list.

## Deployment

### Railway (worker)

1. New Railway project from this repository.
2. Add a PostgreSQL database (or use Neon / external Postgres — set
   `DATABASE_URL` in the Railway service's env vars).
3. Set `DATABASE_URL`, `PREDICTION_POLL_MS=10000`,
   `PREDICTION_LOCK_TTL_SEC=60`, `PREDICTION_FETCH_PAGES=2`.
4. `railway.toml` auto-detects the build (`npm install`) and start command
   (`npm run worker`).

### Vercel (dashboard)

1. New Vercel project from this repository.
2. Set `DATABASE_URL` to the same Postgres as Railway.
3. No cron / no worker needed — the worker runs on Railway.
4. `npm run build && npm run db:migrate` applies migrations on each deploy.

## Development

```bash
npm run dev           # Start dev server on port 8080 (PGLite + in-process worker)
npm run typecheck     # TypeScript type checking
npm run test          # Run tests
npm run lint          # Lint code
npm run worker        # Standalone worker (requires DATABASE_URL=Postgres)
```
