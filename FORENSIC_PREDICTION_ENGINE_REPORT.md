# Forensic Investigation Report — TestingEngine Prediction Engine

**Date:** 2026-09-11  
**Scope:** Live event ingestion → prediction → outbox → delivery → feedback  
**Method:** Source-code path tracing from `scripts/worker.mjs` → `live/boot.ts` → handlers → predictor → validator → notification-worker → feedback. No documentation-only conclusions.

---

## 1. Confirmed Defects (source evidence)

### DEF-1 — N+1 history dropped completed source round N (HIGH)

| Field | Detail |
|--------|--------|
| **File** | `src/lib/prediction/live/predictor.ts` (`onGameEndPredict`) |
| **Call** | `getPriorRoundsSync(MAX_HISTORY, gameId, crashedAt)` |
| **Expected** | History for N+1 includes completed Crash N |
| **Actual** | `excludeGameId=gameId` **and** `t < crashedAt` both drop N |
| **Mask** | ACIE path still learned N via `observeCrashForACIE` / `observeRound` |
| **Impact** | `FALLBACK_BASELINE` (`PredictionEngine.predict`) scored from N−1 only; provenance `sourceGameId` could cite N−1 |
| **Fix** | `getPriorRoundsSync(MAX_HISTORY, targetGameId)` — exclude target only, no source cutoff |
| **Test** | `src/lib/prediction/live/history-include-source.test.ts` |

### DEF-2 — ACIE `pendingContext.prediction` set before disagreement gate (MEDIUM)

| Field | Detail |
|--------|--------|
| **File** | `src/lib/prediction/acie/engine.ts` (`buildEvaluation`) |
| **Expected** | Learning context matches final ENTRY/SKIP after agreement gate |
| **Actual** | `pendingContext.prediction = strategy.isOpportunity` ran **before** disagreement forced SKIP |
| **Impact** | Online learning could treat a SKIP as a predicted opportunity |
| **Fix** | Agreement gate mutates strategy first; then set `pendingContext` |

### DEF-3 — Strategy thresholds below fair odds (HIGH) — fixed prior commit `1314a90`

| Field | Detail |
|--------|--------|
| **File** | `src/lib/prediction/acie/strategy.ts` |
| **Evidence** | `supportedThreshold` 0.58–0.66; clamp `Math.min(0.75, t)` while fair 1.30× ≈ 0.769 |
| **Impact** | Systematic negative-EV ENTRY candidates |
| **Status** | Fixed in quality-gates commit (fair + edge floors) |

### DEF-4 — Result hold default 30s + BG-primary (HIGH) — fixed prior commits

| Field | Detail |
|--------|--------|
| **Files** | `predictor.ts` hold; `game-event-handlers.ts` BG claim |
| **Impact** | Signals delivered after target start / temporal kill |
| **Status** | ED-primary + `PREDICTION_RESULT_HOLD_MS` default 0 (`a042b91`) |

### DEF-5 — Stale architecture comments (LOW)

| Field | Detail |
|--------|--------|
| **File** | `game-event-handlers.ts` header / BG docblock / `skipPredict` comment |
| **Impact** | Operator/maintainer mis-wiring risk |
| **Fix** | Comments aligned to ED-primary + optional `BG_PRIMARY_PREDICT` |

---

## 2. Runtime-Verification Findings (need live metrics)

| ID | Finding | Why not confirmed in source alone |
|----|---------|-----------------------------------|
| RV-1 | Neon RTT / critical-pool acquire still dominates BG reconcile and persist | Requires production `pool_wait_ms` / stage logs |
| RV-2 | Quality gates may reduce signal rate sharply if models rarely output P ≥ fair+edge | Needs post-deploy hit-rate and skip-kind histograms |
| RV-3 | Inter-round betting window median cold-start uses 4s fallback | OK in source; verify median after ≥3 BG/ED pairs |
| RV-4 | Telegram send latency vs 5s `TELEGRAM_DEADLINE_MS` | Network-dependent |

---

## 3. Working Components (verified reachable)

| Component | Entry | Evidence |
|-----------|--------|----------|
| Worker boot | `scripts/worker.mjs` → `live/boot.ts` | Migrations, ACIE restore, fencing, dispatcher, subscriber, poll |
| Socket wiring | `startEventDrivenPipeline` → `initializeEventHandlers` | `bcGameSocket` + native `ed`/`bg`/`pr` |
| ED primary N+1 | `edHandler` → `attemptNPlusOnePrediction` → `onGameEndPredict` | Observe → evaluate → selectivity → durable outbox → `notifyOutbox` |
| BG temporal | `bgHandler` CTE | `began_at`, `target_round_started_at`, outbox dead_letter for target N |
| Ownership | `target-coordinator.ts` | Memory claim; DB unique on `pending_predictions` backstop |
| ED dedup | `classifyEdReentry` + `completedEdRounds` | Concurrent + post-complete duplicate suppression |
| Dispatcher | `notification-worker.ts` | Claim, pre-send temporal auth, Telegram, finalize |
| Validation | `validator.onGameEnd` | WIN/LOSS, release held rows (legacy), feedback job |
| Feedback | `processResolvedPredictionFeedback` | Durable claim + learning components |
| Adaptive edge | `adaptive-edge.ts` | Wired from feedback + `shouldSkipSignal` |

---

## 4. Risk Areas (not defects, monitor)

1. **Memory-only ownership** — lost on restart; DB unique constraint is the multi-process lock.  
2. **Soft-swallowed catches** — intentional for non-critical telemetry; critical path returns structured `kind`.  
3. **NO_BET terminal claims** — blocks ED/poll recompute by design; genuine failures must `releaseTarget`.  
4. **Sheath / quality volume** — high skip rate is expected under QUALITY mode.  
5. **Concurrent workers** — fencing lease; second worker refuses mutation roles.

---

## 5. Implemented Fixes (this pass)

1. History include source N (`predictor.ts`)  
2. `pendingContext` after disagreement gate (`acie/engine.ts`)  
3. Comment alignment ED-primary (`game-event-handlers.ts`)  
4. Regression test `history-include-source.test.ts`  

Prior related fixes already on main: ED-primary delivery (`a042b91`), quality gates (`1314a90`), BG live_event_log deferral (`916f9a9`).

---

## 6. Remaining Issues

| Issue | Severity | Notes |
|-------|----------|-------|
| No automated end-to-end Neon integration in this sandbox | — | DB init OOM/killed; unit path tests pass |
| Lint/full vitest suite not fully run here | — | Network/npm flakiness; targeted node:test green |
| `USE_ADVANCED_PIPELINE` default off | LOW | Opt-in; ACIE is authoritative when history ≥ 5 |
| Validator still has release CTE for held rows | LOW | Harmless with hold=0; keep for emergency hold |

---

## 7. Lifecycle Map (production)

```
worker.mjs
  └─ live/boot.start
       ├─ cold-start seeder / ACIE restore / history warm
       ├─ OutboxDispatcher.start
       ├─ startEventDrivenPipeline → native/socket handlers
       ├─ PollWorker.start
       └─ LiveSupervisor (fencing heartbeat)

bgHandler(N)
  ├─ noteRoundStarted (sync)
  ├─ [optional BG_PRIMARY_PREDICT] reserve + attemptNPlusOne
  └─ critical CTE: began_at, stamp pending N, temporal kill outbox→N

edHandler(N)
  ├─ classifyEdReentry / noteRoundEnded
  ├─ incremental + appendCompletedRound + observeCrashForACIE
  ├─ attemptNPlusOnePrediction (PRIMARY)
  │    └─ claim → history(incl N) → ACIE evaluate → gates → persist+outbox → notifyOutbox
  └─ async: crash_rounds persist + validator.onGameEnd(skipPredict)
       └─ WIN/LOSS + feedback → adaptive edge / online weights

PollWorker
  └─ recovery attemptNPlusOne when no pending / recoverable claim
```

---

## 8. Test evidence (this environment)

```
history-include-source, adaptive-edge, latency-hotpath, target-ownership, acie-upgrade
→ 26+ unit tests pass (node:test)
```

Full DB-backed integration/concurrency tests require Neon/PGLite in CI.
