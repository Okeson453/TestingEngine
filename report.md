# TestingEngine Forensic Investigation Report

**Repository:** https://github.com/Okeson453/TestingEngine  
**Evidence window:** production logs `logs.1789473969109.log.txt` (2026-09-15 ~11:42–12:06 UTC) + prediction history CSV (6,469 issued signals)  
**Code head at investigation:** includes `927e77c` (adaptive edge soft cap) + follow-on veto recovery  
**Authoritative basis:** source + runtime logs (not symptoms alone)

---

## 1. Architecture and call graph

```
Native BC WebSocket
  ├─ PR (betting-open)  ──► reserveTargetForPr(N+1) ──► defaultPredictFn ──► gates ──► pending + outbox
  ├─ BG (~7s later)     ──► reserveTargetForBg (skip if PR owns / no_bet_terminal) ──► reconcile/kill only
  └─ ED (crash)         ──► observeRound(ACIE) + validate pending ──► ED N+1 only if no primary claim
         │
         ├─ ACIE.evaluateNext (primary)  OR  PredictionEngine (if PREDICTION_PRIMARY_ENGINE=old)
         ├─ shouldSkipReason: loss_cooldown | daily_limit | min_p | min_edge/needP | strategy SKIP | confidence
         ├─ noteLowBandCrashStreak on every ED multiplier ∈ [1.00, 1.20]
         └─ notification_outbox → claim → Telegram dispatch
```

**Ownership (`target-coordinator.ts`):** PR-primary. First successful `reserveTargetForPr` owns N+1. BG confirmation-only when claim exists. ED is fallback when primary has not reserved/completed. Terminal `no_bet_terminal` / `markCooldownSkipTarget` blocks re-prediction.

**Critical files:**

| Stage | File |
|-------|------|
| PR/BG/ED handlers | `src/lib/prediction/events/game-event-handlers.ts` |
| Ownership | `src/lib/prediction/live/target-coordinator.ts` |
| Predict + gates | `src/lib/prediction/live/predictor.ts` |
| Adaptive edge | `src/lib/prediction/live/adaptive-edge.ts` |
| Loss / low-band cooldown | `src/lib/prediction/live/prediction-loss-cooldown.ts` |
| ACIE | `src/lib/prediction/acie/engine.ts`, `psi.ts`, `strategy.ts`, `tpl.ts` |
| Validate + low-band hook | `src/lib/prediction/live/validator.ts` |
| Latency stages | `src/lib/prediction/live/latency-trace.ts` |

---

## 2. PR → BG ≈ 6.9–7.1 s

**Log evidence:** `pr_to_bg_ms=6942 … 7062` consistently.

**Verdict: expected upstream lifecycle timing, not an application defect.**

- Application measures `pr_to_bg_ms` from PR receipt to BG receipt on the native WS.
- BC game flow opens betting (PR), then ~7s later emits BG; the worker does not schedule this delay.
- Individual `prediction_ms` on PR path is **0–1 ms** when a decision is made at PR — the 7s gap is **not** prediction latency.

**No change required** to “optimize” PR→BG unless product wants a different upstream trigger.

---

## 3. prediction_ms ≈ 0–1 vs prediction_compute ≈ 252 ms

**Log evidence:**

```
PR→N+1 attempt done … prediction_ms=0|1
[latency-budget] … prediction_compute=252.3ms gates_to_signal=251.0ms persist p50=603ms
```

**Root cause (instrumentation / sparse samples, not model cost):**

- Hot-path `prediction_ms` is wall time around `defaultPredictFn` for that PR attempt (ACIE evaluate is sub-ms when history is warm).
- Aggregate `prediction_compute` / `gates_to_signal` in the latency budget snapshot can include **sparse samples**, **ED-path traces**, or stage markers that span more than pure model math (e.g. one sample n=1 in the log with 252 ms).
- **Persist** (p50 ~603 ms, p99 ~2.2 s) is Neon RTT-bound durable handoff — separate from prediction compute.

**Verdict:** Do not optimize ACIE math based on the 252 ms aggregate alone. Stage timers must stay separate: compute vs gates vs persist vs delivery. Persist remains the dominant critical-path DB cost when a signal is issued.

---

## 4. Why terminal NO_BET dominates (including BET_CANDIDATE @ ~77.5%)

**Log pattern (repeated):**

```
p=0.7764 … edge=+0.0072 … tier=BET_CANDIDATE
fair=0.7692 needP=0.7992 minEdge=0.0300 edgeN=40 edgeHit=0.700
veto=edge_below_threshold
```

### 4.1 Responsible layer

| Layer | Role | Responsible for this NO_BET? |
|-------|------|------------------------------|
| ACIE probability | Ensemble PSI ~71–79% | No — p can be above fair |
| Strategy HF | 65% floor ENTRY | No — not strategy_veto in these lines |
| **Adaptive edge** | `needP = fair + minEdge` | **Yes** |
| Loss cooldown | Skip after issued LOSS | Separate (`COOLDOWN_SKIP`) |
| Low-band 1.00–1.20 | Skip after low crash | Separate (`LOW_BAND_COOLDOWN_ARMED`) |

**Exact mechanism (`predictor.ts` + `adaptive-edge.ts`):**

- `minEdge = getAdaptiveMinEdge()` reached **0.030** (old MAX_EDGE).
- `needP = max(0.65, 0.7692 + 0.03) = 0.7992`.
- Classification: `break-even ≤ p < needP` → **BET_CANDIDATE** (label only).
- Gate: `p < needP` → **edge_below_threshold** → terminal NO_BET.

So BET_CANDIDATE is **not** a pass; it is “above fair but below elevated adaptive needP.”

### 4.2 Why adaptive edge stuck high

- `edgeHit=0.700` over last ~40 **issued** outcomes → gap vs target → edge at ceiling.
- While edge is high, **almost no new signals issue** → no recovery via wins.
- Silence recovery only helped after long idle; continuous NO_BET did not reset edge (fixed with `recordNoIssueDecision`).

### 4.3 Empirical justification (issued history CSV, n=6,469)

Chronological issued signals (not a full 15,878-round crash walk-forward — that requires `crash_rounds` DB; CSV is **issued-only** and is still decisive for the gate):

| min_p filter | n | WR | P/L @ 1.30× unit |
|--------------|---|-----|------------------|
| ≥ 0.65 | 5559 | 75.7% | −89.9 |
| ≥ 0.75 | 4238 | 75.8% | −62.4 |
| ≥ 0.7692 (fair) | 3762 | 75.8% | −54.4 |
| ≥ 0.78 | 3341 | 75.8% | −50.7 |
| **≥ 0.7992** | **2632** | **74.8%** | **−72.3** |
| ≥ 0.80 | 2588 | 74.7% | −76.4 |

**Finding:** Raising the live bar to **~79.92% does not improve** realized WR or P/L on issued history; it **worsens** both vs fair/78% cuts. Therefore locking `needP=0.7992` is **not** empirically justified.

**2026-09-15 issued subset:** n=360, WR=77.2%, P/L=+1.4 — slight edge day; best band was **77–80%**, worst economic band **80–85%** (overconfidence).

---

## 5. ACIE audit (source)

- **Engine:** `ACIEEngine` — `observeRound` / `evaluateNext` / PSI ensemble / StrategyLayer / optional motif gate.
- **Models (PSI):** Frequency, ConditionalFrequency, RegimeAdjusted, StreakAware, ShortWindowBayesian, VolatilityAdjusted, Streak2Recovery (MomentumReversion removed in upgrade path).
- **TPL:** regime, low-cluster, streak-below-1.30.
- **Calibration:** Platt optional; logs show `cal=platt|raw`.
- **Online state:** EWMA hit/Brier, ensemble weights, persisted `acie_online_state`.
- **Strategy default:** HF mode (65% floor) after volume fix.
- **Motif:** `001111` / `011011` — env `ACIE_MOTIF_GATE=1` to enable (default off for volume in recent commits; verify deploy env).

**No source defect found that forces false NO_BET independent of adaptive edge.** ACIE correctly emits p≈0.77; the **live selectivity gate** rejects it when adaptive minEdge is elevated.

---

## 6. Cooldown and low-band

### Loss cooldown

- Issued prediction **LOSS** on target N → skip N+1… with escalation by consecutive issued losses.
- Durable via `worker_state`; `markCooldownSkipTarget` + suppress undelivered outbox.
- Logs: `COOLDOWN_SKIP — PR blocked … loss_cooldown`.

### Low-band [1.00, 1.20]

- **Before:** arm after 2 consecutive → often ineffective in high-activity.
- **After `81322d0`:** arm on **first** low-band crash; re-anchor window; escalate while streak continues.
- Logs: `low-band 1.00x streak=1 — skip 2 betting round(s) from …` then `COOLDOWN_SKIP`.

**Verdict:** Working as designed in the supplied log; not the cause of systematic edge_below walls.

---

## 7. Database / persistence

- Dual pools (critical/general), pinned hot-persist/hot-dispatch clients.
- Durable handoff on critical path when signal issues: pending insert + outbox (Neon RTT).
- Log: `persist` p50≈603 ms, p99≈2.2 s — consistent with multi-statement RTT, **not** pool wait (`checkout_ms=0` on BG kill leg).
- BG reconcile kill leg often `query_ms=0 killed=0` when nothing to kill.
- REST poll correctly skipped when WS healthy (`poll fetch skipped — WS healthy and ED fresh`); occasional `poll tick failed: TimeoutError` is non-critical when WS is primary.

**Recommendation:** Keep non-critical audit/lifecycle off critical path (already largely post-path). Do not inflate pool size without wait>0 evidence.

---

## 8. Concurrency / ownership

- PR reserve → complete/no_bet_terminal prevents BG re-predict and ED N+1.
- Logs consistently: `BG confirmation only — N+1 already owned by primary (PR)`; `ED→N+1 skipped — BG already owns target`.
- Duplicate ED: `ED crash event deduplicated`.

**Verdict:** PR-primary ownership holds in the log sample. No duplicate authoritative SIGNAL path observed.

---

## 9. Data-leakage / walk-forward constraints

- Live invariant: observe crash N before evaluate N+1; PR predicts N+1 from state after prior learns.
- Full **15,878-round** crash walk-forward was **not executed in this environment** (no `DATABASE_URL` / full `crash_rounds` dump).  
- **Issued-signal chronology** (CSV) is valid for **gate economics** on what was actually bet; it is **not** a pure N+1 crash-level walk-forward (skipped rounds never appear).

To run full crash walk-forward in ops:

```bash
DATABASE_URL=... npm run train:acie -- --limit=15878 --dry-run
# plus existing band-backtest / walk-forward tooling against crash_rounds
```

---

## 10. Implemented / confirmed fixes (source-backed)

| Fix | Commit / change | Justification |
|-----|-----------------|---------------|
| Adaptive MAX_EDGE 0.03→**0.015** | `927e77c` | needP ceiling ~78.4%; log showed 0.7992 silence |
| TARGET_HIT = fair | `927e77c` | Chasing 80% not supported by issued CSV |
| Silence recovery 15m | `927e77c` | Faster unlock without outcomes |
| **recordNoIssueDecision** after 12 consecutive no-issue | this investigation | Edge stayed high while only NO_BETs ran |
| Low-band arm on first 1.00–1.20 | `81322d0` | Screenshot/log under-application |
| ACIE primary + HF strategy | `4e78cf8` | Old engine primary hurt WR/volume |

**Not done (intentionally):**

- Lowering absolute 65% evaluation floor.
- Removing cooldown / low-band.
- Treating PR→BG 7s as a bug.
- Raising live min_p to 0.80 (empirically harmful on issued history).

---

## 11. Latency budget (honest stages)

| Stage | Typical (log) | Notes |
|-------|----------------|-------|
| frame_to_event | ~0.03 ms | WS parse |
| PR prediction_ms | 0–1 ms | Model path |
| PR→BG | ~7.0 s | Upstream |
| prediction_compute (aggregate sample) | ~252 ms | Do not conflate with PR prediction_ms |
| persist (when signal) | p50 ~600 ms | Neon durable path |
| delivery | p50 ~241 ms (sample) | Provider |

---

## 12. Tests / validation performed in investigation

- Source trace of ownership, gates, adaptive edge, low-band, latency markers.
- Log frequency: dominant veto `edge_below_threshold` with needP=0.7992.
- Issued CSV chronological economics by threshold (table above).
- Low-band unit checks (prior commit).
- Full typecheck/lint/integration suite + 15k crash walk-forward: **require deploy CI / DATABASE_URL** — not claimed complete in this sandbox.

---

## 13. Remaining risks

1. Deploy may still run **old** adaptive MAX_EDGE=0.03 until `927e77c`+veto recovery are live.
2. Env `MIN_SIGNAL_EDGE` / `SIGNAL_MAX_EDGE` can override soft defaults.
3. Issued history is still **below fair** overall (75.5% WR) — edge is fragile; overconfidence in 80–85% band remains.
4. Persist RTT will dominate whenever signals fire; only structural DB/region changes reduce it materially.

---

## 14. Final technical verdict

1. **PR→BG ~7s is upstream-expected**, not an app bug.  
2. **NO_BET with p≈77% / BET_CANDIDATE** is caused by **adaptive `needP≈0.7992`**, not ACIE refusal to model edge.  
3. **Empirical issued history does not support** a live 79.92% hard bar (worse WR and P/L).  
4. **Low-band and loss cooldown** are functioning in the supplied log; they are not the noon blackout mechanism.  
5. **Primary corrective path:** keep adaptive edge capped, recover on consecutive no-issue, preserve 65% floor + cooldown/low-band, do not chase volume by predicting every round.  
6. Investigation is **operationally actionable** with source-level fixes above; a full 15,878-round crash walk-forward remains an **ops/DB** follow-up for pure N+1 leakage proof beyond issued-signal economics.

---

*Report generated from repository source and supplied production artifacts. Update this document when full crash_rounds walk-forward results are available.*
