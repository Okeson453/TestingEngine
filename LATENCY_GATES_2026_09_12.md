# Latency Acceptance Gates — 2026-09-12 (pass 2)

Follow-up to `DELIVERY_LATENCY_FIX_2026_09_12.md`. This pass was driven by explicit
production acceptance thresholds and the 09:52–09:56Z production logs (the 1287ms
BG prediction outlier, the ~0.6–1.1s delivery delays, the 1093ms critical
`pool_acquire_ms` with idle=2).

## Change: pinned hot-path lanes (`src/lib/db.ts`)

Root cause of the outlier class: every multi-second acquire since sep 11
(1059, 1062, 1093, 1128, 1159ms) is Neon TLS+auth. When Neon (or anything else)
kills an idle pooled socket, the next pool consumer pays a ~1.0–1.1s rebuild —
and on 09:54:27 that consumer was the N+1 durable handoff: acquire 1093ms +
statement ~190ms = the 1287ms prediction outlier. A pool bounds *average*
acquire cost; the hot path needs a bounded *worst* case.

Two dedicated critical-pool clients ("pinned lanes") are now built at boot and
held forever (never released to the pool):

- **persist lane** (`getPredictionPersistSql()`) — predictor durable handoff
  (both `onGameStart` and `onGameEndPredict`).
- **dispatch lane** (`getDispatchCriticalSql()`) — prediction-lane outbox
  claim + finalize in the dispatcher.

They are pinged by the existing 20s keepalive, validated with `SELECT 1` at
build, rebuilt in the background on connection-class errors, and — while
rebuilding — fall back to the critical pool for that one query (all lane
statements are guard-idempotent: persist CTE conflicts DO NOTHING;
claim/finalize are status-guarded). Under PGLite/no-pool (tests, local) both
lanes return the shared critical Sql — zero behavioural divergence. Pool sizes
unchanged: the 2 pinned clients come out of the existing critical max of 4.

Also: slow pool acquires now log `loop_lag_ms` (event-loop attribution inside
the acquire window) so the 1093ms class is diagnosable from one log line.

## Harness: `scripts/latency-acceptance-gates.mjs`

Production-path load harness (real Postgres + real dual pool + real dispatcher +
stubbed Telegram). Simulates Neon RTT on every query and Neon TLS+auth on every
NEW connection, runs concurrent general/critical pool noise per round, and
periodically destroys idle pooled sockets — reproducing the production kill-and-
rebuild pattern. Reports P50/P95/P99 for every acceptance gate. Run on the fixed
tree and on a `git stash` of the previous tree.

## Results (n=26 predicting rounds per run; the adaptive-edge quality gate is
untouched, so ~20% of rounds legitimately emit — same selectivity as prod)

RTT = 25ms (worker co-located with DB):

| Gate | Before P95 | Before P99 | After P95 | After P99 | Gate limit | After |
|---|---|---|---|---|---|---|
| BG event → decision | 0.8ms | 1.3ms | 1.1ms | 1.2ms | 50/100/200 | PASS |
| BG compute | 0.6ms | 1.1ms | 1.0ms | 1.1ms | 10/25/50 | PASS |
| BG → durable handoff | 142.5ms | **1174.8ms** | 29.5ms | 29.6ms | 50/100/200 | PASS |
| BG event → SIGNAL_READY | 148.2ms | **1192.7ms** | 38.1ms | 41.4ms | 100/200/300 | PASS |
| SIGNAL_READY → claim | 33ms | 41ms | 35ms | 37ms | 100/250/500 | PASS |
| claim → dispatch | 11.4ms | 14.9ms | 7.5ms | 7.6ms | 100/250/500 | PASS |
| SIGNAL_READY → delivery | 70.3ms | 81.1ms | 62.6ms | 69.7ms | 500/750/1000 | PASS |
| E2E BG → delivery | 203.9ms | **1256.2ms** | 92.1ms | 98.5ms | 600/1000/1500 | PASS |
| event-loop max stall | 25.0ms | — | 42.5ms | — | ≤250 | PASS |

RTT = 200ms (measured production Neon RTT from the 09:52 logs: reconcile 180–184ms,
persist 187–219ms — every hot-path statement is already a single round trip):

| Gate | Before P95 | Before P99 | After P95 | After P99 | Limit | After |
|---|---|---|---|---|---|---|
| BG event → decision | 0.9ms | 1.1ms | 0.7ms | 1.2ms | 50/100/200 | PASS |
| BG compute | 0.8ms | 1.0ms | 0.5ms | 1.0ms | 10/25/50 | PASS |
| BG → durable handoff | 817.2ms | **2093.5ms** | 202.9ms | 203.0ms | 50/100/200 | RTT floor |
| BG event → SIGNAL_READY | 819.8ms | **2105.7ms** | 212.9ms | 220.8ms | 100/200/300 | RTT floor |
| SIGNAL_READY → claim | 209ms | 212ms | 211ms | 216ms | 100(P50)/250/500 | RTT floor |
| claim → dispatch | 11.5ms | 19.6ms | 10.6ms | 10.7ms | 100/250/500 | PASS |
| SIGNAL_READY → delivery | 418.9ms | 423.7ms | 421.9ms | 425.5ms | 500/750/1000 | PASS |
| E2E BG → delivery | **1231.6ms** | **2517.6ms** | **623.7ms** | **628.8ms** | 600/1000/1500 | ~RTT floor |
| event-loop max stall | 48.7ms | — | 27.1ms | — | ≤250 | PASS |

## Reading the remaining RTT=200ms "FAIL" rows

Those legs are exactly one Neon round trip (persist commit = 202ms, claim =
211ms) or two RTT + one Telegram round trip (E2E ≈ 613–629ms). One DB round
trip is the physical floor of a durable handoff and of claim-time temporal
authorization — both are safety invariants that must not be weakened. There is
no application-side code left on those legs: the process cost on each is
sub-millisecond (decision 0.7ms, compute 0.5ms, claim→dispatch start 10.6ms).
The root cause of the gap to the thresholds is network distance: the worker
(Railway) and the Neon region are ~185–215ms apart. **Co-locating the worker
with the Neon region (Railway region config) removes ~185ms from every DB leg
and would move E2E P95 from ~620ms to ~65ms** — an infra change, not a code
change. Do not "fix" these legs with timeouts or pool sizes.

## Other log findings (attributed, no action required)

- `event_loop_lag_ms=1536` at 09:52:35 is the first sample of the lag sampler,
  which spans worker ESM module-graph compilation at boot (one-time, before the
  live pipeline starts). No mid-stream stall ≥400ms appears anywhere in the
  09:52–09:56 window.
- `wr_utils` fresh-fetch failures at boot fall back to the `worker_state` cache
  by design (the 546ms SELECT is the multi-hundred-KB bundle read, once, at
  boot; a process-local copy prevents re-reads).
- `general pool_acquire_ms=1062` at 09:52:35 is the boot-time cold seeder (TLS
  +auth), before prewarm completed.

## Validation

- `tsc --noEmit` clean; `eslint` 0 errors; `vite build` clean.
- vitest: 115 passed (unchanged from baseline).
- `bun test` live suite: failure set identical to a clean tree (pre-existing
  bun-shim environmental failures; CI arbitrates on real Node 22).
- New: `src/lib/prediction/live/hot-lanes.test.ts` (lane fallback + cache +
  persistence contract), registered in the `test` script.
- `durable-handoff-ordering.test.ts` source assertion updated to the new
  pinned-lane default (`getPredictionPersistSql` — still never the general pool).
- Pinned-client death verified: destroying a checked-out pinned client logs a
  discard, the query falls back to the pool, the round still delivers, and the
  client rebuilds in the background.
