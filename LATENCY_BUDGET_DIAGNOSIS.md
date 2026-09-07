# TestingEngine Latency Budget Diagnosis (2026-09-07)

**Symptom:** ~5,000 ms end-to-end before prediction signal appears (Telegram / dashboard).
**Repo HEAD:** current `main` (parallel ED, history buffer, outbox wake, gate cache, full dispatcher).

---

## 1. Two paths (do not conflate them)

| Path | When | Realistic E2E |
|------|------|----------------|
| **A. Event-driven (Socket.IO `ed`)** | `socketv4.bc.game` connected, `/g/cm` receiving `ed` | **300–800 ms** p50 to outbox claim; Telegram +RTT |
| **B. Poll recovery** | WAF / no socket / ED skipped residual | **1.5–5.0 s** (poll interval + REST + healthy-defer) |

Operator-visible **~5 s** almost always means **path B dominates** (socket blocked or residual skip → wait for poll).

---

## 2. Stage budget — Path A (healthy ED)

```
Live Crash data acquisition (Socket.IO ed)
  → round detection / normalize
  → feature / pattern / prediction
  → validation (parallel)
  → signal generation (TX + outbox insert)
  → outbox wake + claim
  → Telegram delivery
```

| # | Stage | Code | Measured / budget |
|---|--------|------|-------------------|
| 1 | Socket receive → handler | `socket-client` → `onEdEvent` | **0–20 ms** |
| 2 | Round detect + normalize | `game-event-handlers` | **0–5 ms** |
| 3 | Incremental state update | `globalIncrementalState.update` | **<1 ms** (sync) |
| 4 | History for features | `live-history-buffer` (warm) | **0–5 ms** mem; **40–150 ms** SQL cold |
| 5 | Gate eligibility | 1 combined SQL + gate-cache | **20–80 ms** Neon RTT |
| 6 | Feature + model predict | `PredictionEngine` ~50 rounds | **10–40 ms** |
| 7 | Prediction TX + outbox INSERT | `runInTransaction` | **40–150 ms** |
| 8 | Outbox wake → claim | `notifyOutbox` + `TICK_MS=10` | **0–15 ms** |
| 9 | Telegram POST | `sendTelegramMessage` | **100–500 ms** RTT (p95 ≤2s timeout) |

**Path A sum (signal durable):** ~**100–350 ms**  
**Path A sum (Telegram accepted):** ~**250–900 ms** (Telegram is downstream of “released”)

---

## 3. Stage budget — Path B (poll recovery) ← **~5 s root**

| # | Stage | Cost | Why |
|---|--------|------|-----|
| 1 | Wait for next poll tick | **0–500 ms** | `POLL_INTERVAL_MS` default 500 |
| 2 | Healthy-defer when socket looks healthy | **0–2500 ms** | `POLL_HEALTHY_DEFER_MS=2500` — **largest single delay** |
| 3 | BC.Game history fetch (1–2 pages) | **200–1500 ms** | REST RTT; parallel pages help but Cloudflare/Neon egress still slow |
| 4 | Insert rounds + validate batch | **100–400 ms** | DB |
| 5 | `maybePredictNewest` | **100–300 ms** | same predict path |
| 6 | Outbox + Telegram | **50–500 ms** | same as A |

**Critical path B:** defer + poll wait + REST ≈ **2–5 s** before predict even starts.

---

## 4. Root causes of ~5,000 ms

1. **Socket path not live** (Cloudflare WAF / missing `p`/`t` sign) → pure poll.  
2. **`POLL_HEALTHY_DEFER_MS=2500`** still waits 2.5s when lag is “small” but ED was missed.  
3. **Residual / `effective_skip_below_ms`** historically raised floors to 800–3000ms → `skipped_late` → forces path B.  
4. **Measuring “latency” as Telegram arrival** includes provider RTT; durable release is earlier.  
5. *(Historical, fixed)* outbox **stub** never drained — looked like multi-second silence.

---

## 5. What already shipped on `main`

- Parallel ED: predict ∥ validate  
- Live history buffer + outbox wake  
- Prediction outbox priority 3  
- Full `OutboxDispatcher` (not stub) + CI anti-stub guard  
- Gate cache + single eligibility query  
- Parallel history page fetch  
- Namespace `/g/cm`, WAF backoff 15s (cap 60s)  
- Residual floors `MIN_REQUIRED_WINDOW_MS=150`, `SKIP_BELOW_MS=80`

---

## 6. Recommendations (priority order)

### R1 — Ensure Socket.IO is the primary path (ops)
- Set `BCGAME_SOCKET_P` / `BCGAME_SOCKET_T` from browser edge agent when Node IP is WAF-blocked.  
- Monitor `connection.status !== connected` and `waf_blocked`.  
- **Impact:** eliminates path B entirely → ~5s → sub-second.

### R2 — Cap residual skip inflation (code) ✅ this commit
- Never apply `effective_skip_below_ms` above **200 ms**.  
- Prevents systematic skip → poll recovery.

### R3 — Adaptive healthy-defer (ops + code)
- Keep defer only when **Crash-specific** `lastEdAt` is fresh **and** newest game_id matches DB.  
- Optional: `POLL_HEALTHY_DEFER_MS=1000` in Railway if ED miss rate is low.

### R4 — Treat “released” as outbox commit, not Telegram ACK
- Instrument `ed → outbox_insert` and `ed → telegram_accepted` separately.  
- Target: **p95 ed→outbox ≤ 500 ms**; Telegram p95 ≤ 1.5–2 s.

### R5 — Do not lower poll interval blindly
- Sub-200ms polling hammers BC.Game without fixing WAF.  
- Prefer socket health over faster REST.

---

## 7. Minimum safe latency target

| Boundary | Target |
|----------|--------|
| ED → prediction generated | p50 ≤ 150 ms, p95 ≤ 400 ms |
| ED → outbox durable | p95 ≤ 500 ms |
| Outbox → Telegram start | p95 ≤ 50 ms (wake) |
| Telegram accepted | p50 ≤ 400 ms, p95 ≤ 1.5–2 s |
| Poll recovery (no socket) | p95 ≤ 2.5–3.5 s |

**Architecture floor:** ~**250–400 ms** durable signal (Neon RTTs + model). Telegram adds uncontrollable RTT.

---

## 8. Before / after

| Metric | Before (path B / skip) | After (path A + caps) |
|--------|------------------------|------------------------|
| Operator E2E | ~5,000 ms | **~300–900 ms** (Telegram) |
| Durable signal | often after next round | **before** next BG when socket healthy |
| Poll recovery | 5–10 s defer era | ≤ ~2.5–3.5 s |

**Validation:** 100 consecutive rounds with socket `connected`; require `prediction_generated_at < target began_at` ≥ 99% and `edToPredictMs` p95 < 500.
