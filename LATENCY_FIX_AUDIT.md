# Prediction Signal Latency Fix — Audit & Changes

**Date:** 2026-09-07  
**Repo:** Okeson453/TestingEngine  
**Symptom:** ~7,000 ms end-to-end lag; signal after Crash round advances.

## Root causes addressed

1. Per-prediction SQL history load (remote DB RTT)
2. Poll healthy-defer 10 s on "socket healthy"
3. Residual gate floors causing skipped_late → recovery lag
4. Outbox dispatcher tick wait after enqueue
5. **Priority inversion**: validation rows (prio 2) claimed before predictions (prio 1/2)
6. **Sequential ED path**: validation TX completed before N+1 prediction started
7. Socket WAF backoff 60 s leaving pure-poll for minutes

## Changes (2026-09-07)

### New modules
- `src/lib/prediction/live/live-history-buffer.ts` — memory history for hot path
- `src/lib/prediction/live/outbox-wake.ts` — EventEmitter wake for immediate drain

### `predictor.ts`
- History: memory buffer first, SQL fallback
- Residual: MIN_REQUIRED 150, SKIP_BELOW 80, GENERATION 100, DELIVERY 80
- **Outbox priority for predictions: 3** (validations stay 2) — eliminates inversion
- `notifyOutbox()` after successful persist

### `poll-worker.ts`
- Healthy defer **10 s → 2.5 s**; pending current window ≤1 game ID

### `validator.ts`
- `skipStateUpdate` support for parallel ED path
- Append to live history buffer; `notifyOutbox` after validation row
- Validations remain priority 2

### `game-event-handlers.ts`
- **Parallel ED path**: incremental state update → `onGameEndPredict` + `onGameEnd({ skipPredict, skipStateUpdate })` concurrently
- Removed `setImmediate` wrapper on critical path

### `notification-worker.ts`
- Wake channel race with timer
- **TICK_MS 25 → 15**; **BATCH_PARALLELISM 4 → 6**

### `socket-client.ts`
- WAF backoff default **60 s → 15 s**; richer connect_error logs

### `boot.ts`
- Warms live history buffer after schema validation

## Expected latency

| Path | Target |
|------|--------|
| Healthy ED → outbox claim | ~80–350 ms |
| Poll recovery | ≤ ~2.5–3 s |
| Outbox enqueue → claim | 0–5 ms |
| Catch-up (no priority inversion) | predictions drain before validation backlog |

## Invariants preserved
- 1:1:1 validator transaction
- Temporal invariant (`prediction_generated_at < target_round_started_at`)
- Outbox deadline expiry
- Sheath mode
