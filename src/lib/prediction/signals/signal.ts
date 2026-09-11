import type { FeaturePath, PredictionDecision, PredictionOutput, PredictionSignal } from '../types.ts';

/**
 * Context the caller (PredictionEngine) must supply so the signal is
 * COMPLETE at construction. Nothing may be added to the signal after
 * toSignal() returns \u2014 the object is frozen here.
 */
export interface SignalConstructionContext {
  /** Feature-generation path that produced the model's input vector. */
  featurePath: FeaturePath;
  /** The N+1 round this prediction targets (NOT the already-completed source round). */
  targetRoundId: string;
  /** Explicit decision state. If not provided, defaults to 'ENTRY' for backward compatibility.
   * When decision is 'NO_BET' or 'SKIP', this signal must NOT be delivered.
   */
  decision?: PredictionDecision;
}

/**
 * Convert a validated PredictionOutput into the canonical, immutable
 * PredictionSignal.
 *
 * Invariant: toSignal() receives a validated prediction and returns a
 * fully valid, schema-compliant, immutable PredictionSignal; nothing
 * downstream is permitted to augment or mutate it. All required fields \u2014
 * including featurePath and targetRoundId \u2014 are constructed HERE, and
 * Object.freeze() runs only after the complete object exists.
 */
export function toSignal(
  output: PredictionOutput,
  context: SignalConstructionContext,
): PredictionSignal {
  return Object.freeze({
    predictionId: output.predictionId,
    timestamp: output.timestamp,
    modelVersion: `${output.model.name}@${output.model.version}`,
    featureVersion: output.model.featureVersion,
    featurePath: context.featurePath,
    targetRoundId: context.targetRoundId,
    target: output.target,
    score: output.score,
    probability: output.probability,
    confidence: output.confidence,
    regimeId: output.regime?.id ?? null,
    dataQuality: output.dataQuality,
    reasoning: Object.freeze([...output.reasoning]),
    expiresAt: output.expiresAt,
    featureSummary: Object.freeze({ ...output.featureSummary }),
    decision: context.decision ?? 'ENTRY',
  });
}

export function isSignalExpired(signal: PredictionSignal, now = new Date()): boolean {
  return new Date(signal.expiresAt).getTime() <= now.getTime();
}
export function isSignalFresh(signal: PredictionSignal, maxAgeMs = 30_000, now = new Date()): boolean {
  if (isSignalExpired(signal, now)) return false;
  return now.getTime() - new Date(signal.timestamp).getTime() <= maxAgeMs;
}

/**
 * Check if a signal represents an actionable prediction (not NO_BET/SKIP).
 * This is the canonical gate: only signals with decision === 'ENTRY' or 'REDUCED_ENTRY'
 * should be delivered as predictions.
 */
export function isActionableSignal(signal: PredictionSignal): boolean {
  return signal.decision === 'ENTRY' || signal.decision === 'REDUCED_ENTRY';
}

/**
 * Check if a signal represents a NO_BET decision.
 * NO_BET and SKIP both mean "do not bet this round".
 */
export function isNoBetSignal(signal: PredictionSignal): boolean {
  return signal.decision === 'NO_BET' || signal.decision === 'SKIP';
}
