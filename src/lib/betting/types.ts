/**
 * Canonical betting/entry risk contracts.
 *
 * RiskEvaluationInput is the ONE contract between the decision layer
 * (EntryDecisionService) and the risk gate (RiskEngine.evaluate). The
 * prediction-signal and limit fields are consumed by the decision layer
 * itself (live gating, ACIE risk state, pipeline bankroll) and stay on the
 * same object so there is no parallel legacy/current API.
 *
 * RiskEngine.evaluate is async by contract — callers MUST await it.
 */

/** Prediction signal snapshot handed to the risk gate alongside the stake. */
export interface RiskPredictionSignal {
  predictionId: string;
  probability: number;
  confidence: number;
  target: number;
  dataQuality: number;
  expiresAt: string;
}

export interface RiskEvaluationInput {
  /** Raw signal payload for the risk engine (kept loose: engine is a deferred stub). */
  signal: unknown;
  stake: number;
  bankroll: number;
  /** Entry mode — 'live' entries are gated on prediction readiness. */
  mode?: 'paper' | 'live';
  /** Current bankroll balance (may differ from stake basis). */
  currentBalance?: number;
  /** Consecutive errored/losing entries. */
  consecutiveErrors?: number;
  /** Entries already confirmed today. */
  dailyEntriesConfirmed?: number;
  /** Configured daily entry cap. */
  maxDailyEntries?: number;
  /** Minimum acceptable prediction probability/confidence. */
  minPredictionProbability?: number;
  minPredictionConfidence?: number;
  /** Canonical signal snapshot; undefined when no acceptable signal exists. */
  predictionSignal?: RiskPredictionSignal;
}

export interface RiskEvaluationResult {
  approved: boolean;
  reason: string;
  /** Machine-readable rejection code (e.g. 'PREDICTION_NOT_READY'). */
  rejectionReason?: string | null;
  /** Which gate failed first, when rejected. */
  firstFailure?: string | null;
  maxStake?: number;
}
