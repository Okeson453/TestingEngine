# Canonical production prediction path (P0/P1 authority fix)

```
Native WebSocket / ED(N)
      ↓
Crash-End Authority (dedupe + claimTarget)
      ↓
Shared ACIE.observeRound(Crash N)   ← learning BEFORE next decision
      ↓
recordAcieObservation(N, observationCount)
      ↓
ACIE state / observationCount advances
      ↓
Shared ACIE.evaluateNext() → N+1 decision + feature_hash
      ↓
assertFreshAcieState(N)  ← hard reject if stale
      ↓
pending_predictions (+ provenance columns)  +  notification_outbox
      ↓
Notification Worker → Telegram
```

## Authoritative instance

- Single process-wide engine: `getSharedACIEEngine()` (`src/lib/prediction/acie/shared-engine.ts`)
- Boot restores online state into that same instance
- `EntryDecisionService`, feedback, validator, and live predictor all reference it
- Do not construct secondary `new ACIEEngine()` for live decisions

## Provenance (required on every emitted signal)

Columns on `pending_predictions` (migration 0034) + `feature_summary` + logs:

- `acie_instance_id`, `acie_observation_count`, `acie_state_version`
- `source_game_id` / `source_round_id`, `target_game_id`
- `feature_hash` (SHA-256 of canonical input fingerprint)
- `prediction_mode`: `NORMAL_ACIE` | `FALLBACK_BASELINE` | `ADVANCED_ACIE` | `STALE_REJECTED`
- `execution_path`, `strategy_action`

## Ordering invariant

```
Crash N received
  → deduplicate
  → ACIE.observeRound(N)
  → state version increments
  → ACIE evaluates N+1
  → assertFreshAcieState(N)
  → persist prediction
```

Never: predict N+1 with PredictionEngine, then learn Crash N later.

## Stale-state enforcement

`assertFreshAcieState(sourceGameId)` must pass before persist. If ACIE did not
observe that source in this process, emission is **rejected** (`STALE_REJECTED`).

## Fallback visibility

If ACIE is cold or unavailable, the path may fall back to `PredictionEngine`.
That path MUST set `prediction_mode=FALLBACK_BASELINE` and must not silently
look like NORMAL_ACIE.

## Hierarchy

```
ACIE (authoritative decision)
  ├── Features / PSI ensemble
  ├── Strategy / opportunity
  └── Risk / selectivity gates
        ↓
   FINAL DECISION → pending_predictions
```

`PredictionEngine` is infrastructure/fallback only — not a parallel authority.

## Tests

- `src/lib/prediction/acie/independence.test.ts`
  - lower-multiplier sequence advances observation + feature_hash
  - stale reject without observation
  - duplicate ED idempotency
  - provenance shape

## Related modules

- Live emission: `src/lib/prediction/live/predictor.ts` (`onGameEndPredict`)
- Shared engine: `src/lib/prediction/acie/shared-engine.ts`
- Provenance: `src/lib/prediction/acie/provenance.ts`
- Stale guard: `src/lib/prediction/acie/stale-guard.ts`
- Feedback / validator: shared engine only
