import type { FeaturePath, PredictionOutput, PredictionSignal } from '../types.ts';

/**
 * Context the caller (PredictionEngine) must supply so the signal is
 * COMPLETE at construction. Nothing may be added to the signal after
 * toSignal() returns — the object is frozen here.
 */
export interface SignalConstructionContext {
  /** Feature-generation path that produced the model's input vector. */
  featurePath: FeaturePath;
  /** The N+1 round this prediction targets (NOT the already-completed source round). */
  targetRoundId: string;
}

/**
 * Convert a validated PredictionOutput into the canonical, immutable
 * PredictionSignal.
 *
 * Invariant: toSignal() receives a validated prediction and returns a
 * fully valid, schema-compliant, immutable PredictionSignal; nothing
 * downstream is permitted to augment or mutate it. All required fields —
 * including featurePath and targetRoundId — are constructed HERE, and
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
  });
}

export function isSignalExpired(signal: PredictionSignal, now = new Date()): boolean {
  return new Date(signal.expiresAt).getTime() <= now.getTime();
}
export function isSignalFresh(signal: PredictionSignal, maxAgeMs = 30_000, now = new Date()): boolean {
  if (isSignalExpired(signal, now)) return false;
  return now.getTime() - new Date(signal.timestamp).getTime() <= maxAgeMs;
}
