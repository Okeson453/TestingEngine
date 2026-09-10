# Canonical production prediction path (P0 authority fix)

```
Native WebSocket / ED(N)
      ↓
Crash-End Authority (dedupe + claimTarget)
      ↓
Shared ACIE.observeRound(Crash N)   ← learning BEFORE next decision
      ↓
ACIE state / observationCount advances
      ↓
Shared ACIE.evaluateNext() → N+1 decision
      ↓
Decision validation + selectivity gates
      ↓
pending_predictions  +  notification_outbox
      ↓
Notification Worker → Telegram
```

## Authoritative instance

- Single process-wide engine: `getSharedACIEEngine()` (`src/lib/prediction/acie/shared-engine.ts`)
- Boot restores online state into that same instance
- `EntryDecisionService`, feedback, and live predictor all reference it
- Do not construct secondary `new ACIEEngine()` for live decisions

## Provenance (required on every emitted signal)

Stored in `feature_summary` (and logged as `PREDICTION_GENERATION`):

- `acie_instance_id`, `acie_observation_count`, `acie_state_version`
- `source_game_id`, `target_game_id`
- `feature_hash` (SHA-256 of canonical input fingerprint)
- `prediction_mode`: `NORMAL_ACIE` | `FALLBACK_BASELINE` | `ADVANCED_ACIE` | …
- `strategy_action`, `probability`, `confidence`

## Ordering invariant

```
Crash N received
  → deduplicate
  → ACIE.observeRound(N)
  → state version increments
  → ACIE evaluates N+1
  → persist prediction
```

Never: predict N+1 with PredictionEngine, then learn Crash N later.

## Fallback visibility

If ACIE is cold or unavailable, the path may fall back to `PredictionEngine`.
That path MUST set `prediction_mode=FALLBACK_BASELINE` and must not silently
look like NORMAL_ACIE.

## Related modules

- Live emission: `src/lib/prediction/live/predictor.ts` (`onGameEndPredict`)
- Shared engine: `src/lib/prediction/acie/shared-engine.ts`
- Provenance: `src/lib/prediction/acie/provenance.ts`
- Feedback: `src/lib/prediction/live/feedback.ts` (uses shared engine)
- Decision service: `src/lib/prediction/entry-decision-service.ts` (consumes shared engine)
