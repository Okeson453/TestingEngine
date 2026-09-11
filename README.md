# TestingEngine

**BcTracker** — autonomous live prediction engine for [BC.Game](https://bc.game) Crash rounds (1.30× threshold intelligence).

A long-lived **Railway worker** ingests live crash events, generates N+1 predictions, validates outcomes, and delivers Telegram signals. A **Vercel dashboard** is read-only against the same PostgreSQL database.

---

## Architecture

```
  BC.Game native WS / Socket.IO          REST poll (recovery)
           │                                      │
           ▼                                      ▼
  ┌─────────────────────────────────────────────────────────┐
  │              Railway worker  (npm run worker)            │
  │  ED(N) → observe → predict N+1 → outbox → Telegram      │
  │  BG(N) → started_at + temporal kill of late signals     │
  │  Poll  → recovery when socket path misses a round       │
  │  Feedback → adaptive edge / ACIE online weights         │
  └───────────────────────────┬─────────────────────────────┘
                              │ writes
                              ▼
                     PostgreSQL (Neon / Railway)
                              │ reads
                              ▼
  ┌─────────────────────────────────────────────────────────┐
  │         Vercel dashboard  (TanStack Start SSR)           │
  │         Read-only: rounds, predictions, worker health    │
  └─────────────────────────────────────────────────────────┘
```

### Production prediction path (ED-primary)

| Stage | What happens |
|-------|----------------|
| **ED(N)** | Crash N ends → observe on ACIE → claim target N+1 → in-memory history (includes N) → evaluate → edge/quality gates → durable `pending_predictions` + `notification_outbox` → `notifyOutbox` |
| **Delivery** | Dispatcher claims pending outbox (immediate by default) → pre-send temporal auth → Telegram |
| **BG(N+1)** | Stamps round start; **dead-letters** any undelivered prediction still targeting the started round |
| **Validation** | When N resolves, WIN/LOSS + feedback update online state / adaptive edge |
| **Poll** | Safety net only if the live path missed ownership or events |

Optional legacy: set `BG_PRIMARY_PREDICT=1` to also attempt N+1 at BG(N) (without crash N). **Default is off** — generation uses the completed round result.

### Design invariants

- **One target → one prediction owner** (in-memory claim + DB unique on unmatched `pending_predictions.target_game_id`)
- **No outcome leakage**: features/history never include the target round’s crash
- **Temporal contract**: signal must be generated and delivered **before** the target round starts
- **Quality over volume**: ENTRY only when model P beats fair odds (1/1.30 ≈ 76.9%) + configurable edge
- **Worker authority fencing**: only the lease holder mutates live state

---

## Quick start

### Requirements

- Node.js **≥ 22.6**
- PostgreSQL for production (`DATABASE_URL`)
- Optional: Telegram bot for signal delivery

### Local development

```bash
npm install
npm run dev          # Vite + in-process PGLite; worker runs inside the dev server
```

### Standalone worker (production shape)

```bash
export DATABASE_URL=postgres://...
npm run worker       # migrations + live boot (socket, poll, outbox dispatcher)
```

### Common scripts

| Script | Purpose |
|--------|---------|
| `npm run dev` | Dashboard + local worker (PGLite if no `DATABASE_URL`) |
| `npm run worker` | Production autonomous worker |
| `npm run build` | Vite build + `db:migrate` |
| `npm run db:migrate` | Apply SQL migrations |
| `npm run typecheck` | `tsc --noEmit` |
| `npm run test` | Unit + live-path tests |
| `npm run lint` | ESLint |
| `npm run diagnose` | Engine diagnostic script |

---

## Deployments

| Surface | Role | Notes |
|---------|------|--------|
| **Railway** | Worker 24/7 | `Procfile` / `railway.toml` → `npm run worker` |
| **Vercel** | Read-only dashboard | Same `DATABASE_URL`; never polls BC.Game or generates predictions |

See [DEPLOY.md](./DEPLOY.md), [EDGE_SETUP.md](./EDGE_SETUP.md), and [TELEGRAM.md](./TELEGRAM.md).

Both deployments **must** share the same Postgres instance.

---

## Prediction engine (ACIE)

**ACIE** (Adaptive Crash Intelligence Engine) is the authoritative live scorer when history is warm (≥ 5 observed rounds):

- Multi-model PSI ensemble + online weight updates  
- Optional Platt calibration when it improves rolling Brier  
- Strategy layer: ENTRY / REDUCED_ENTRY / SKIP  
- Live selectivity: `MIN_SIGNAL_EDGE` + **adaptive edge** from realized signal outcomes  
- Ensemble **disagreement gate** (`ACIE_MAX_DISAGREEMENT`)

Fallback: `PredictionEngine` + feature engines when ACIE is unavailable (explicit `FALLBACK_BASELINE` provenance).

Default strategy mode is **quality** (thresholds ≥ fair + edge). High-frequency mode:

```bash
ACIE_STRATEGY_MODE=hf
```

---

## Key environment variables

### Required (production worker)

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | Postgres connection string |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_IDS` | Signal delivery (see TELEGRAM.md) |

### Prediction & delivery

| Variable | Default | Purpose |
|----------|---------|---------|
| `MIN_SIGNAL_EDGE` | `0.02` | Min P − fair before emit; further adapted online |
| `PREDICTION_RESULT_HOLD_MS` | `0` | Outbox hold before claimable (0 = immediate) |
| `BG_PRIMARY_PREDICT` | off | `1` = also predict at BG without crash N |
| `ACIE_STRATEGY_MODE` | `quality` | `quality` \| `hf` |
| `ACIE_QUALITY_EDGE` | `0.025` | Strategy threshold above fair |
| `ACIE_MAX_DISAGREEMENT` | `0.06` | Skip when ensemble disagreement exceeds this |
| `TELEGRAM_DEADLINE_MS` | `5000` | Soft send budget for live signals |
| `USE_NATIVE_BC_WS` | on | Native BC.Game WS primary (`0` to disable) |
| `USE_ADVANCED_PIPELINE` | off | Extra meta/calibration pipeline on fallback path |

### Infrastructure

| Variable | Purpose |
|----------|---------|
| `POLL_WORKER_MS` | REST recovery poll interval |
| `PG_*` / pool sizing | Neon connection tuning (see `src/lib/db.ts`) |
| Socket / edge agent vars | WAF bypass via browser edge — see EDGE_SETUP.md |

---

## Repository layout

```
scripts/worker.mjs          # Process entry → live boot
src/lib/prediction/
  live/                     # Boot, predictor, validator, outbox, poll, feedback
  events/game-event-handlers.ts
  acie/                     # Shared ACIE engine, PSI, strategy, online state
  models/ features/ …       # Fallback PredictionEngine stack
migrations/                 # Ordered SQL (outbox, feedback, fencing, indexes)
docs/                       # Deep-dive investigations
FORENSIC_PREDICTION_ENGINE_REPORT.md
```

---

## Correctness contracts

1. **Correlation** — exactly one unmatched pending prediction per `target_game_id`  
2. **Temporal** — `generated_at` before target start; dispatcher refuses late sends; BG kills stale outbox rows  
3. **History** — N+1 history includes completed source N; never includes the target’s crash  
4. **Idempotency** — ED dedup by game id; feedback claimed once per `prediction_id`  
5. **Authority** — non-authoritative workers drop mutation roles after fence loss  

---

## Ops & diagnostics

| Symptom | Where to look |
|---------|----------------|
| Late / missing Telegram | Outbox status, pre-send temporal reasons, `ed_to_signal_ms` logs |
| No predictions | History READY?, quality skips (`skipped_no_edge`), sheath halt, fencing |
| Worker offline / pool | Neon `max_client_conn`, critical vs general pool logs |
| Socket WAF | `socket_status`, browser edge agent (EDGE_SETUP.md) |

Useful docs:

- [FORENSIC_PREDICTION_ENGINE_REPORT.md](./FORENSIC_PREDICTION_ENGINE_REPORT.md) — path audit & fixed defects  
- [LATENCY_OUTBOX_PREDICTION_DIAGNOSIS.md](./LATENCY_OUTBOX_PREDICTION_DIAGNOSIS.md)  
- [TELEGRAM.md](./TELEGRAM.md)  
- [DATABASE_AUDIT_REPORT.md](./DATABASE_AUDIT_REPORT.md)  

---

## Dashboard / server functions

SSR UI uses TanStack Start. Server functions live in short paths such as `src/lib/p.ts` so base64 function IDs do not embed `/` (see historical path-length constraint).

POST calls use a `{ data: { ... } }` wrapper:

```ts
predictionSetDailyTarget({ data: { target: 50 } })
```

---

## License / status

Private application workspace. Node ≥ 22.6. Production worker is the source of truth for predictions; the dashboard only observes.
