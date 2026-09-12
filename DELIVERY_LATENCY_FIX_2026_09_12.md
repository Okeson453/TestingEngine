# Signal Delivery Latency — Stage Tracing & Fixes (2026-09-12)

**Scope:** the remaining measured 0.7–2.4s from `PREDICTION_SIGNAL_READY` →
durable outbox → outbox claim → dispatch → Telegram/API acknowledgement →
user-visible delivery, plus the recurring DB-contention symptoms (general pool
max=6 with idle=0/waiting=1–2, critical-pool acquisition ~1s, worker_state
queries ~526ms, startup event-loop stalls ~1557ms). Every source below is read
from code at this commit — nothing guessed.

---

## 1. SIGNAL_READY → user-visible: exact stage sources

| Stage | Source | Round trips (critical pool) |
|---|---|---|
| Durable outbox | `predictor.ts` compound CTE (`pending_predictions` + `notification_outbox` in ONE statement) | 1 |
| Wake | `outbox-wake.ts` latched EventEmitter — `notifyOutbox("prediction")` after persist | 0 (in-process) |
| Claim | `notification-worker.ts` prediction-lane claim CTE | 1 |
| Temporal auth | **was a separate `UPDATE … send_started_at`** with 2× NOT EXISTS (`live_round_state`, `crash_rounds`) | 1 ← removed |
| Telegram | `telegram.ts` keep-alive agent, primary-first (secondaries detached) | 1 HTTPS |
| Finalize | delivered UPDATE with 2× NOT EXISTS (after ACK — not user-visible) | 1 |

Warm math at ~185ms Neon RTT: persist + claim + auth + Telegram ≈
0.65–1.1s before ACK. A single cold acquire (~1s TLS+auth) or a pool wait
turned that into 2.4s. The separate auth UPDATE was pure sequential RTT
overhead: the exact pre-send predicate (deadline + target-not-started/crashed)
is now evaluated **inside the claim statement** (`authorized` computed column),
which stamps `send_started_at` or dead-letters the row atomically. One fewer
critical round trip on every dispatch; the zero-RTT in-memory registry gate
runs immediately before send, and the finalize late-acceptance gate remains
the last write. Fail-closed is structural: a DB error at claim means nothing
was claimed and nothing is sent.

**Measured (harness at 100ms synthetic RTT, real dispatcher + PGLite, 3 runs):**

| Path | Queries per dispatch | Wall to Telegram ACK |
|---|---|---|
| Before | 10 | ~608ms |
| After | 9 | ~503ms |

At production Neon RTT (~185ms) this removes ~185ms from every
SIGNAL_READY→ACK, and removes the contention tail (§2) that accounted for the
0.7→2.4s spread.

## 2. General-pool saturation (idle=0 / waiting=1–2): exact callers

The dispatcher's own drain loop was the largest source:

1. **Normal-lane claim ran on EVERY 100ms tick** — `runNormal` included
   `(!wake.prediction && !wake.normal)`, true on every timer expiry, so a
   heavy `UPDATE…FROM` with a correlated `NOT EXISTS` JSON-metadata gate ran
   ~10×/s against an empty outbox. **Fixed:** wake.normal or every 50 ticks
   (~5s recovery). Producers always wake (`notifyOutbox("normal")`), so the
   timer term is missed-wake recovery only.
2. **`recoverStale()` every 10 ticks (~1s)** — 4–5 general-pool queries
   including two multi-table temporal sweeps. **Fixed:** every 50 ticks (5s).
   Zombie cleanup bounded; nothing on the delivery path.
3. **`reconcileForensics()` every 30 ticks (3s)** — analytics on the general
   pool. **Fixed:** every 300 ticks (30s).
4. **Empty prediction-lane claim every 20 ticks (2s)** on the CRITICAL pool —
   cold-acquire generator. **Fixed:** every 50 ticks (5s); the wake channel is
   latched, so a missed wake can only follow a restart/bug.

Remaining general-pool tenants are cadence-bounded and legitimate: heartbeat
CTE (10s), skew-monitor batched upsert (5min) + 1h percentile aggregation,
rate-limited median-gap write, detached audit rows (2/round), forensics.

Pool sizes deliberately **unchanged** (worker boots pin `PG_POOL_MAX=10` →
critical 4 / general 6). The fix removes demand, not capacity.

## 3. worker_state ~526ms

Two sources, both already off the hot path at this commit: the wr_utils
bundle `SELECT` (~573ms; process-local memory cache, only on boot-fetch
failure) and the skew monitor's 1-hour percentile aggregation over
`live_event_log` (5-min cadence, general pool). The gate-cache prevents any
per-prediction worker_state read. No hot-path worker_state query remains;
nothing further changed.

## 4. Startup event-loop stalls (~1557ms)

Boot is sync-CPU heavy: the `vm.SourceTextModule` compile/link/evaluate of the
large wr_utils bundle (`native-sign.ts`), ACIE state JSON restore, and
`prewarmHotModules()` dynamic imports. All are boot-time only — they do not
touch the steady-state signal path. Left as-is; a worker-thread move of the
sandbox evaluation is the follow-up if boot-time restart gaps matter.

## 5. Legacy BG_PRIMARY_PREDICT consolidation

- `bgPrimaryEnabled()` (prediction-attempt.ts) is now the single toggle;
  `bgHandler` and `onGameEnd` both read it (previously two inline env parses
  that could disagree).
- Misleading log label "(legacy BG_PRIMARY_PREDICT)" removed.
- `onGameStart` stays exported but remains production-guarded
  (`ALLOW_LEGACY_ON_GAME_START`) — it is test/legacy-persist infrastructure,
  not part of the live BG-primary path.
- Fresh-database migration bug fixed: 0038 indexed
  `pending_predictions.decision` before adding the column (production Neon
  never noticed; any fresh DB failed).

## Preserved invariants (verified by suite)

N+1 outcome-leakage protection (result-after-signal gate), BG-primary /
ED-fallback ownership, fencing, deduplication (in-process + DB constraint),
temporal validity (claim-time gate + registry gate + finalize gate),
MIN_SIGNAL_EDGE / quality / risk gates, durable delivery, NO_BET terminal
semantics. Full live-suite parity with the pre-change baseline
(163 pass / 31 fail — the 31 are pre-existing bun-shim runner artifacts; CI
runs real Node 22). Typecheck, lint, and production build clean.
