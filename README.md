# TestingEngine

BcTracker — autonomous prediction engine for BC.Game Crash multiplier rounds.

## Overview

CrashWave records BC.Game Crash multipliers in sequence — live history, stats, and streaks. Tracking only. The prediction engine runs as an autonomous background worker that polls BC.Game history, detects new rounds, generates predictions, validates WIN/LOSS outcomes, and persists results to the database — independent of any browser session.

## Architecture

### Worker (Autonomous Background Process)

The worker (`scripts/worker.mjs`) runs as a standalone Node.js process:

- Polls BC.Game history every ~500ms
- Detects new rounds by comparing game IDs against the last processed round
- Generates predictions using the prediction engine (feature engine, regime detector, models)
- Validates predictions against resolved rounds (WIN/LOSS)
- Persists results to PostgreSQL (Neon) or PGLite (dev/preview)
- Runs autonomously — does **not** require a browser, dashboard, or Telegram session

```bash
npm run dev          # Start Vite dev server (includes worker via in-process PGLite)
npm run worker       # Standalone worker (requires DATABASE_URL for Neon)
```

### Database

- **Production**: PostgreSQL via Neon (`DATABASE_URL` env var)
- **Dev/Preview**: PGLite (in-process WASM Postgres)

Migrations are in `migrations/` and auto-applied before first query.

### Dashboard

Live at `http://localhost:8080/predictions`:

- Current prediction status
- Daily progress (target vs. resolved count)
- WIN/LOSS stats and streaks
- Historical validation records
- Worker health (Running/Offline, last sync)
- Daily target configuration (adjustable, 20–500)

## Server Functions

Server functions are defined in `src/lib/p.ts` and called from client components via `useMutation`/`useQuery`.

### Important: Server Function ID Base64 Encoding

TanStack Start encodes server function IDs as base64 of `{"file":"<path>","export":"<name>"}`. The base64 string becomes part of the URL path (`/_serverFn/<base64id>`).

**Known issue**: If the file path is long enough that the base64 encoding produces `/` characters, the server's URL parsing (`split('/')[0]`) truncates the ID, causing "Invalid server function ID" 500 errors for all browser-side calls.

**Solution**: Keep server function file paths short enough to avoid `/` in the base64 encoding. The functions must be in a file at a short path (e.g., `src/lib/p.ts`, not `src/lib/prediction/api.ts`).

### Calling Convention

POST server functions require `{ data: { ... } }` wrapper:

```typescript
// CORRECT — TanStack Start v1.x POST calling convention
predictionSetDailyTarget({ data: { target: 50 } })

// INCORRECT — raw args are ignored (opts.data is undefined)
predictionSetDailyTarget({ target: 50 })
```

## Development

```bash
npm run dev          # Start dev server on port 8080
npm run typecheck     # TypeScript type checking
npm run test          # Run tests
npm run lint          # Lint code
```
