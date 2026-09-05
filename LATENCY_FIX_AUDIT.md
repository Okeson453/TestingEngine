# Prediction Signal Latency Fix — Audit & Changes

**Date:** 2026-09-05  
**Repo:** Okeson453/TestingEngine  
**Symptom:** Prediction signal reaches output after BC.Game has advanced 2–3 rounds.

## Root cause (source-traced)

Primary latency was **not** network, poll interval, or model CPU time.

1. **Residual / predictive-deadline gate** in `onGameEndPredict`  
   - Defaults: `MIN_REQUIRED_WINDOW_MS=800`, `SKIP_BELOW_MS=500`,  
     `GENERATION_BUDGET_MS=500 + DELIVERY_BUDGET_MS=200` → effective floor ≈ **700 ms**.  
   - When `remainingMs = medianGap − elapsedSinceEd` fell below that floor,  
     generation was **skipped** (`kind: "skipped_late"`).  
   - No pending row → no signal until recovery.

2. **Poll recovery deferred for 30 s** while socket reported “healthy”  
   - Missed ED predictions were only recovered after ~2–3 full inter-round gaps.

Secondary contributors (smaller):
- Poll interval default 1500 ms  
- Adaptive band clamped to 1000–1500 ms  
- History fetch limited to 100 rows  
- `onGameEnd` ran synchronously on the Socket.IO callback

## Changes implemented

### `src/lib/prediction/live/predictor.ts`
| Constant / logic | Before | After |
|------------------|--------|-------|
| `MAX_HISTORY` | 100 | **50** |
| `MIN_REQUIRED_WINDOW_MS` | 800 | **250** |
| `SKIP_BELOW_MS` | 500 | **150** |
| `GENERATION_BUDGET_MS` | 500 | **150** |
| `DELIVERY_BUDGET_MS` | 200 | **100** |
| Hard skip floor | max(skip, gen+deliv) ≈ 700 | **skipThreshold only (150–250)** |
| Tight residual | hard skip | generate + let outbox expire if needed |

`PREDICT_TIMEOUT_MS=80` already applied via `Promise.race` on the hot path.

### `src/lib/prediction/live/poll-worker.ts`
| Setting | Before | After |
|---------|--------|-------|
| Default `POLL_INTERVAL_MS` | 1500 | **500** |
| Adaptive multiplier | 0.3 / 0.5 | **0.25** |
| Adaptive clamp | 1000–1500 ms | **200–1000 ms** |
| Socket-healthy defer lag | 30_000 ms | **10_000 ms** |

### `src/lib/prediction/events/game-event-handlers.ts`
- `onGameEnd` (validate + N+1 predict) offloaded via `setImmediate` so the Socket.IO event loop is not blocked by DB / model work.  
- Fast path (`markLiveRoundEnded`) remains on the event callback.

### Not changed (intentionally)
- **Validator transaction** — single-tx validate + match preserves the 1:1:1 invariant; splitting would risk races.  
- **History in gate `Promise.all`** — running the history query before the skip decision would waste work on skipped_late paths.  
- **DB index** — `crash_rounds(crashed_at DESC)` already exists in `0002_crash_rounds.sql`.

## Expected end-to-end latency

| Path | Target |
|------|--------|
| Healthy hot ED → outbox claim | **~100–350 ms** |
| Poll recovery after skip / socket lag | **≤ ~10 s** (was 30 s+) |
| Rounds of signal lag | **0–1** (was 2–3) |

## Validation checklist
- [ ] Unit: `predictor.test.ts`, `validator.test.ts`, `poll-worker` paths  
- [ ] Integration / temporal-invariant tests  
- [ ] Runtime: `live_event_log` `processor_latency_ms`, outbox delivery timing  
- [ ] Confirm `skipped_late` rate drops under continuous operation  

