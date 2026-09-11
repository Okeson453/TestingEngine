# Production Investigation — N+1 Signal/Result Co-Delivery (2026-09-11)

> External production investigation of main @ 55cf9de, added to the repo verbatim
> per operator instruction. Companion to `LATENCY_OUTBOX_PREDICTION_DIAGNOSIS.md`
> and the Teamily report of the same date. Sections 20-23 describe the still-missing
> pre-crash precompute layer (deferred in 55cf9de's commit message); sections 21-22
> (timestamp semantics, source-latency measurement) are the near-term actionable
> items. The source document was truncated mid section 23.

**Repository state investigated:** main, commit 55cf9deef48573e7874912ed8a23ae558ffd09f1.

## Executive finding

The current implementation does have effective prediction-first dispatch ordering, but that is not the primary problem anymore.

The fundamental problem is:

> N+1 prediction generation is still triggered reactively by ED(N) — the end of crash N. The system has no production pre-crash scheduling path that guarantees enough time for N+1 prediction generation + persistence + dispatch + Telegram acceptance before N+1 starts.

The repository itself confirms this architecture: ED(N) owns `attemptNPlusOnePrediction()`, while BG(N+1) only reconciles the target start.

The latest commit even explicitly deferred the architectural pre-crash precompute scheduler. That means the repository currently fixes the ordering after prediction exists, but not the fundamental time at which prediction work begins.

## Root-cause chain

```
Crash N actually ends
       ↓
BC.Game ED event reaches worker
       ↓
edHandler starts
       ↓
ACIE observes N
       ↓
N+1 prediction is synchronously computed
       ↓
critical DB transaction
       ↓
pending_predictions + prediction outbox committed
       ↓
prediction wake
       ↓
prediction dispatcher claims row
       ↓
temporal authorization
       ↓
Telegram HTTP request
       ↓
Telegram accepts prediction
       ↓
ONLY NOW validation/result becomes claimable
       ↓
WIN/LOSS Telegram request
```

If the ED event itself is 2-4 seconds late, or the prediction/persistence path consumes a large fraction of the inter-round window, the prediction is already behind schedule before the outbox system even gets involved.

## 1. Actual production lifecycle

### A. Crash N detection

The native WebSocket receives `ed`.

The native client decodes the packet and invokes every registered event handler synchronously. `edHandler()` is started immediately.

There is an important timing problem here.

The native protocol does not carry an authoritative crash timestamp for `ed`. `fieldsToPayload()` sets `endTime = Date.now()` when the packet is decoded.

So the system's concept of "crash N ended at T" is actually "worker decoded ED at T". That means upstream WebSocket/network/event-delivery latency is not represented as source-event latency. It is absorbed into the apparent crash timestamp.

This is a major forensic defect.

If BC.Game's crash actually ended at 10:00:00.000 but the worker receives/decodes ed at 10:00:03.200, the repository records `crashedAt = 10:00:03.200` and effectively believes it saw the crash immediately. That makes the prediction budget look healthier than it actually was.

### 2. edHandler() does not precompute N+1

At entry, edHandler() correctly writes the in-memory ended state before any await. That part is good.

Then it does `globalIncrementalState.update(N)`, `appendCompletedRound(N)`, `await attemptNPlusOnePrediction(...)`.

The prediction attempt is therefore inside the ED critical path. The handler does not return until N+1 prediction → model execution → persistence → outbox enqueue has completed. The later crash-round persistence and validation are detached, which is good, but it does not change the fact that the prediction itself starts only after N has ended.

### 3. The prediction engine itself is synchronous

`attemptNPlusOnePrediction()` calls `onGameEndPredict()`, which performs: claim target → calculate remaining window → append history → read in-memory history → ACIE.observeRound(N) → ACIE.evaluateNext() → stale-state validation → selectivity checks → durable persistence.

`ACIEEngine.onCrash()` invokes `ingestCrash()`, which updates sequence state, online adaptation, calibration and then calls `buildEvaluation()`. `buildEvaluation()` performs PSI/model inference, evidence processing, calibration and strategy evaluation synchronously.

There is therefore still a CPU/event-loop critical section between ED receipt and signal persistence.

The repository has `PREDICT_TIMEOUT_MS = 80ms` but this is not an actual timeout. It merely logs "prediction exceeded PREDICT_TIMEOUT_MS — consider offloading to worker thread" after the synchronous computation already finished. An 80 ms budget violation does not interrupt, isolate, or parallelize model execution. There is currently no worker-thread inference boundary.

### 4. The most important timing defect: generated timestamp is misleading

`onGameEndPredict()` creates `generatedAt = new Date().toISOString()` **before** ACIE observation and model computation. That timestamp is later persisted as `generated_at`, even though the actual model computation occurs afterward.

So `generated_at` does not mean "model finished generating prediction". It means approximately "prediction attempt started after ED processing reached this point". This can materially under-report prediction-generation latency.

The repository does have a separate PREDICTION_SIGNAL_READY timing trace, but the durable prediction's canonical timestamp is still misleading. This should be fixed immediately.

### 5. The prediction persistence path is now correct but still consumes the window

The current implementation correctly uses the critical pool for prediction persistence and atomically inserts pending_predictions + notification_outbox inside one transaction. The transaction records pool wait, acquisition, begin, statement, commit, total persistence, crash-to-outbox-durable — exactly the right instrumentation.

But the fundamental timing equation is still: ED latency + ACIE/model latency + DB transaction latency + dispatcher latency + Telegram latency < inter-round window. The repository currently has no mechanism guaranteeing that inequality.

### 6. The target-start prediction window is estimated, not actually scheduled

The current predictor calculates `predictedStart = crashedAt + medianBettingWindow` and `remainingBeforeTarget = predictedStart - Date.now()`. The betting window is learned from BG(N) − ED(N−1) using a rolling median.

That is a useful estimate, but it is not a real target-start scheduler. It assumes future N+1 start ≈ current ED + historical median gap, rather than observing an actual future-target start/deadline. More importantly, because crashedAt is effectively decode-time for native ED, the prediction's timing anchor itself moves with the ingestion delay.

### 7. This explains the "pulling takes 3-4 seconds" symptom

The REST poll path is explicitly only a recovery path: healthy WS → poll dormant; degraded WS → aggressive recovery; dead WS → poll becomes live recovery, using 500-1000 ms recovery intervals.

But a poll tick performs: REST fetch → insert rounds → update live state → validate newly inserted rounds → maybePredictNewest() before the newest prediction attempt. The repository itself previously documented the critical poll path as capable of 2-5 seconds before prediction even starts — incompatible with a 3-5 second crash-to-crash window. If the native WS misses ED and poll recovery takes over, the prediction can already be late before prediction code starts.

### 8. There is a second poll-specific bottleneck

`maybePredictNewest()` does several DB checks before calling `attemptNPlusOnePrediction()`: isEdgeFresh(), pending_predictions lookup, live_round_state lookup, crash_rounds lookup, WS health checks, pending lookup. The recovery path is not "detect newest crash → immediately predict" but "fetch → ingest → validate → multiple DB eligibility checks → predict". Appropriate for recovery, not for a latency-critical live prediction path.

### 9. The dispatcher is NOT the primary root cause anymore

The repository now has a genuine prediction lane: prediction rows claimed separately (type = prediction, batch = 1, priority = 100), normal lane excludes prediction rows, prediction lane runs first, prediction wakes are lane-specific. The old architecture (prediction waits behind result batch) is no longer the dominant problem.

### 10. The 800 ms validation delay was never a sufficient guarantee

The validator inserts the WIN/LOSS row with next_attempt_at = authoritativeNow + 800ms. But prediction dispatch itself can take longer than 800 ms. The dependency gate is much stronger: the result query refuses to claim a validation row while a correlated N+1 prediction is pending/inflight within the last 30 seconds. The 800 ms delay is not what is causing the current co-delivery; it is now merely a minimum delay. The actual ordering guarantee is the dependency gate.

### 11. Why prediction + WIN/LOSS still appear together

The current ordering is: prediction queued → claimed → Telegram accepts → DELIVERED → result dependency disappears → WIN/LOSS claimable. That is working as designed.

If the user sees NEW PREDICTION / WIN/LOSS almost together, that does not prove the dispatcher sent them concurrently. It can mean the N+1 prediction was already late, Telegram finally accepted it, the result immediately became eligible, and Telegram accepted the result. The result is a secondary symptom of the late prediction.

### 12. Telegram "accepted" is not identical to user-visible receipt

`telegram_accepted_at` is based on the HTTP result from Telegram. `sendTelegramMessagePrimaryFirst()` waits for the primary Telegram destination and detaches secondary destinations. The system can reliably measure Telegram HTTP accepted, but it cannot currently measure Telegram client rendered notification to user: `telegram_accepted_at ≠ user_saw_at`. This matters if the two messages look simultaneous on the phone.

### 13. There is a real race around Telegram acceptance

The dispatcher correctly has a finalization guard: for predictions, after Telegram accepts the request, the DB transaction checks again that the target has not started/crashed, marking `late_acceptance` rather than valid delivery. But that does not undo a Telegram message already accepted by Telegram. The true guarantee is currently "late signals are detected/rejected/classified", not "late signals are physically impossible". The latter requires sufficient upstream scheduling margin.

### 14. Critical-pool contention is reduced, not eliminated

Dual-pool architecture: critical (prediction persistence, prediction dispatch) max 3; general (dashboard, analytics, feedback, background). The transaction helper pins the transaction to the pool associated with the supplied SQL wrapper (fixing the earlier severe error where supposedly critical transactions silently used the general pool). But contention inside a pool of only three connections remains a latency variable that must be measured, not assumed.

### 15. The event-driven path has no proactive computation stage — the decisive architectural defect

Current: ED(N) → learn N → predict N+1 → persist → send.

Required:

```
WHILE N IS RUNNING
N starts
  ↓
precompute N+1 context
  ↓
keep model/feature state hot
  ↓
continuously maintain candidate prediction
  ↓
ED(N)
  ↓
apply N final delta
  ↓
finalize N+1 prediction
  ↓
persist
  ↓
send immediately
```

The final ED(N) operation should be a small state update, not the beginning of the whole prediction pipeline.

### 16. Exact current ordering

| Stage | Current implementation |
|---|---|
| N actual crash | BC.Game |
| N ED received | native WS |
| noteRoundEnded(N) | synchronous |
| Incremental state update | synchronous |
| History append | synchronous |
| N+1 target claim | in-memory |
| Betting window calculation | in-memory |
| ACIE observe N | synchronous |
| ACIE evaluate N+1 | synchronous |
| Model conversion/validation | synchronous |
| Prediction ready | only now |
| DB transaction | critical pool |
| pending_predictions | committed |
| prediction outbox | committed |
| prediction wake | immediate |
| prediction claim | prediction lane |
| pre-send temporal gate | DB + in-memory |
| Telegram request | primary destination |
| Telegram acceptance | terminal prediction event |
| WIN/LOSS enqueue | validation path |
| validation wake | normal lane |
| result dependency check | prediction must be terminal |
| WIN/LOSS claim | only after prediction terminal |
| Telegram result request | normal lane |

The ordering is therefore mostly correct after the prediction exists. The missing stage is pre-crash N+1 preparation.

### 17. Root-cause ranking

- **P0 — Fundamental scheduling flaw.** N+1 prediction begins at ED(N), not before it. If the usable inter-round window is ~3-5 seconds, the system must spend the entire prediction/persistence/delivery budget inside that interval, with no deterministic guarantee that ED → prediction → DB → outbox → Telegram fits inside it.
- **P0 — Source/ingestion latency is hidden.** ed uses Date.now() at decode time as endTime, collapsing actual crash → network delay → decode into "crash timestamp = decode timestamp". This prevents accurate measurement of the very latency suspected to be responsible.
- **P0 — Poll recovery is structurally too slow for live prediction.** REST retrieval, insertion, lifecycle updates, validation and eligibility checks before prediction; if native WS misses an ED, the system can lose most/all of the inter-round window before N+1 computation begins.
- **P1 — Prediction inference remains synchronous.** The 80 ms "timeout" is only telemetry; it does not constrain execution or move inference off the Node event loop.
- **P1 — generated_at is semantically wrong.** The persisted timestamp is established before prediction computation finishes, making timing investigations falsely conclude the prediction was generated earlier than it actually was.
- **P1 — Target-start estimate is statistical, not authoritative.** crashEnd + median betting window is a fallback, not a primary SLA anchor.
- **P1 — Final Telegram acceptance race cannot guarantee physical user receipt.** The gate can classify a late acceptance, but cannot retract an already-accepted message.
- **P2 — Critical-pool contention remains possible.** Prediction persistence and dispatch share the critical pool; needs production telemetry.
- **P2 — Poll path performs unnecessary work before recovery prediction.** Acceptable for recovery, undesirable if recovery must rescue a live missed ED within a 3-5 second window.

### 18. What is actually working (do not revert)

Native WS primary source; ED canonical trigger; BG no longer creates predictions; PR no longer kills valid prediction rows; ED duplicate dedupe; in-memory round registry updated synchronously; dedicated prediction outbox lane; single-item prediction lane; lane-aware wake; detached normal/result work; critical-pool persistence; atomic prediction+outbox commit; prediction temporal authorization; final temporal authorization; result dependency gate; result cannot normally claim while the correlated prediction is pending/inflight; primary-first Telegram delivery; explicit late-acceptance classification; stale outbox recovery detached from the hot path.

### 19. What should NOT be done

Do not keep adding +200ms/+500ms/+800ms/+2s delays to the WIN/LOSS path — that treats the symptom. Do not simply set VALIDATION_DISPATCH_DELAY_MS = 3000 or prediction priority = infinite. The dispatcher cannot create time that was already lost before the outbox row existed.

### 20. Production-grade architecture recommendation — Phase 1, fix observability first

Every prediction must carry one immutable timeline: source_event_received_at, source_event_protocol_time, source_event_decoded_at, prediction_started_at, acie_observe_started_at, acie_observe_completed_at, model_started_at, model_completed_at, prediction_generated_at, persist_started_at, persist_committed_at, outbox_enqueued_at, wake_at, claim_started_at, send_authorized_at, telegram_send_started_at, telegram_accepted_at, target_started_at, target_crashed_at, result_enqueued_at, result_claimed_at, result_telegram_accepted_at. Do not derive these later.

### 21. Correct timestamp semantics

`generatedAt = new Date().toISOString()` at the beginning of onGameEndPredict() should become: prediction_started_at = now at entry; model completes; prediction_generated_at = now; persist both. That gives prediction_compute_ms = prediction_generated_at − prediction_started_at rather than an inferred value.

### 22. Measure actual source latency

The native protocol synthesizes ed.endTime = Date.now(). This should become protocol_event_at / worker_received_at / worker_decoded_at where possible. At minimum received_at, decode_at, handler_at must be separate. Then source_transport_latency = worker_decode_at − protocol_event_at if an authoritative event timestamp is available; if not, explicitly call it decode_latency_unknown rather than pretending Date.now() is the crash timestamp.

### 23. Build the missing precompute layer — the real P0 fix

While crash N is running: BG(N) → precompute N+1. Maintain a PrecomputedPredictionContext { sourceRound: N, featureSnapshot, regime, modelState, calibrationState, ACIE state version, prediction candidate, computedAt }. When ED(N) arrives: append actual N → O(1) incremental state update → apply delta → final prediction → outbox. The goal: ED → final prediction → durable handoff with the model work already done.

*Document truncated at this point in the source material.*
