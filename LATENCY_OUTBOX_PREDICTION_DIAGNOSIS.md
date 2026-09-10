# Latency diagnosis: outbox 2.5–4s + prediction engine 1.5–2s

**Date:** 2026-09-10  
**Repo:** Okeson453/TestingEngine  
**Scope:** ED(N) → N+1 prediction persist → outbox claim → Telegram acceptance

---

## Executive summary

The 1.5–2s “prediction engine” lag and the 2.5–4s outbox delivery lag are **mostly the same class of problem**: sequential Neon round-trips on a **cold or contended critical pool**, not model compute time.

Model / history path is designed to be memory-only and should be tens of milliseconds when the history buffer is READY. When total “prediction” time is 1.5–2s, the bulk is almost always:

1. Critical-pool client acquire (TLS+auth to Neon if cold)  
2. `BEGIN` + single CTE insert + `COMMIT` (persist)  
3. Outbox claim TX + temporal auth UPDATE + Telegram + finalize UPDATE (dispatch)

Each cold Neon leg is on the order of **~700–1500ms**. Two cold legs (persist + dispatch) produce **~2–3s** before Telegram even starts.

---

## Observed budget (what the numbers mean)

| Stage | Expected warm | Observed problem range | Dominant cost when slow |
|-------|---------------|------------------------|-------------------------|
| History load | &lt;5ms (memory) | — | SQL fallback (now blocked) |
| Model predict | &lt;50ms | — | CPU only |
| Persist TX | &lt;100ms warm | **0.7–1.9s** | Pool acquire + Neon RTT |
| Wake → claim | &lt;5ms | up to **500–2000ms** | Missed wake + `TICK_MS` fallback |
| Auth UPDATE | &lt;50ms warm | **0.3–1s** | Extra critical RTT |
| Telegram primary | 100–500ms | up to 2s timeout | Network |
| Finalize UPDATE | &lt;50ms warm | **0.3–1s** | Extra critical RTT |

**Prediction engine 1.5–2s** ≈ persist path (acquire + TX), not the neural/heuristic core.

**Outbox 2.5–4s** ≈ claim + auth + Telegram + finalize, often with cold acquire and/or 2s tick fallback; previously also inflated by a **2.5s intentional validation delay** (ordering fix) if results were included in the measurement.

---

## Confirmed code-level causes

### 1. Critical pool went cold between rounds (P0)

Crash rounds are spaced **10–70s**. Default `PG_IDLE_TIMEOUT_MS = 15_000` closed idle clients long before the next ED.

Source comment in `db.ts` already documented this:

> Every cold acquire paid ~1s of TLS+auth to Neon — the direct cause of the intermittent ~700-1000ms prediction persistence leg and ~1.3-2.0s dispatch leg (warm rounds: ~1ms).

`min: 1` was insufficient under concurrent persist + dispatch, and idle timeout still allowed the pool to shed warmth.

### 2. Connection warmer only heated the **general** pool (P0)

`LiveSupervisor.startConnectionWarmer()` called `getSql()` / `SELECT 1` only.

Prediction persist and outbox dispatch use **`getCriticalSql()`**. The critical pool was **not** kept warm by the 3s probe → first ED after idle paid full Neon connect cost on the hot path.

### 3. Multiple sequential critical-pool RTTs on dispatch (P0 architecture)

Per prediction delivery:

1. Claim TX (`runInTransaction` on critical)  
2. Temporal authorization `UPDATE … send_started_at`  
3. Telegram HTTP  
4. Finalize `UPDATE … delivered`

With Neon RTT hundreds of ms, steps 1+2+4 alone can exceed **1–2s** even when warm-ish; cold multiplies each acquire.

### 4. Outbox recovery tick was 2s (P1)

`OUTBOX_TICK_MS` default **2000**. Wake is primary, but any missed/coalesced wake waited up to 2s before the next drain — a large fraction of a 2.5–4s outbox total.

### 5. Validation dispatch delay was 2.5s (P1, ordering)

`VALIDATION_DISPATCH_DELAY_MS` default **2500** deferred WIN/LOSS so the N+1 signal went first. Correct for ordering; too large if operators measure “outbox” as any Telegram from ED. Reduced to **800ms**.

### 6. What is *not* the primary cause

- Prediction **model** compute (should be ≪ 100ms)  
- History SQL fallback (blocked when buffer not READY)  
- Forensic / dashboard general-pool work (separated from critical)  
- Increasing Telegram timeout (hides lag; does not fix it)

---

## Fixes applied (this change set)

| Change | File | Effect |
|--------|------|--------|
| Critical idle timeout **180s** (env `PG_CRITICAL_IDLE_TIMEOUT_MS`) | `src/lib/db.ts` | Survive inter-round gaps without TLS reconnect |
| Critical pool **min = 2** (env `PG_CRITICAL_POOL_MIN`) | `src/lib/db.ts` | Persist + dispatch overlap without cold slot |
| `allowExitOnIdle: false` on critical pool | `src/lib/db.ts` | Do not drop last clients |
| Warmer hits **critical + general** every 3s | `live-supervisor.ts` | Critical path stays hot |
| Outbox `TICK_MS` default **500** | `notification-worker.ts` | Bound missed-wake latency |
| Validation delay default **800ms** | `validator.ts` | Keep signal-before-result without 2.5s penalty |

---

## Target SLA (N+1 signal)

```
Crash N ends
  ├─ predict compute     ≤ 50ms
  ├─ critical persist    ≤ 150ms (warm)
  ├─ claim + auth        ≤ 150ms (warm)
  └─ Telegram primary    ≤ 300–500ms
  ────────────────────────────────────
  crash_end → user signal ≤ ~800ms warm path
```

Cold path should no longer be the steady state after these pool fixes.

---

## Verification

1. Deploy and confirm logs show dual-pool line with `critical … min=2 idleTimeoutMs=180000`.  
2. On successive EDs, `transaction_acquire_ms` / `pool_wait_ms` should stay low (single-digit to low tens ms), not ~1000ms.  
3. `PREDICTION_SIGNAL_READY` `totalMs` before persist should stay small; persist profile should not dominate with acquire.  
4. Outbox lifecycle logs: `queueWaitMs` + `dispatchMs` + `sendMs` — queue wait should collapse when wake works.  
5. WIN/LOSS still trails N+1 signal by ~800ms, not 2.5s.

---

## Follow-ups (not in this patch)

- Pin one client across claim → auth → finalize for predictions (save 1–2 RTTs).  
- Metric: `critical_pool_cold_acquire_total` when acquire &gt; 200ms.  
- Optional: raise `PG_CRITICAL_POOL_MAX` only after measuring wait queue under load.
