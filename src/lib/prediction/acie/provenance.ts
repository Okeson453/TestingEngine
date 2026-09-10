/**
 * ACIE prediction provenance + input fingerprinting.
 *
 * Every emitted signal must carry enough metadata to answer:
 *   - Which crash had ACIE observed?
 *   - What state version / observation count produced it?
 *   - What feature fingerprint did it see?
 *   - Which execution path (NORMAL_ACIE / FALLBACK_BASELINE / ...)?
 */

import { createHash } from 'node:crypto';
import type { ACIEEvaluationResult } from './types.ts';
import type { OnlineAdaptiveState } from './online-state.ts';
import { getSharedACIEInstanceId } from './shared-engine.ts';

export type PredictionExecutionMode =
  | 'NORMAL_ACIE'
  | 'SAFE_BASELINE'
  | 'ADVANCED_ACIE'
  | 'FALLBACK_BASELINE'
  | 'STALE_REJECTED';

export interface AcieProvenance {
  acie_instance_id: string;
  acie_observation_count: number;
  acie_state_version: number;
  source_game_id: string;
  target_game_id: string;
  feature_hash: string;
  prediction_mode: PredictionExecutionMode;
  execution_path: string;
  strategy_action: string | null;
  model_name: string;
  model_version: string;
  probability: number;
  confidence: number;
  generated_at: string;
}

/** Deterministic SHA-256 of a canonical feature payload. */
export function computeFeatureHash(payload: unknown): string {
  const canonical = stableStringify(payload);
  return createHash('sha256').update(canonical).digest('hex').slice(0, 32);
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(',')}]`;
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(',')}}`;
}

export function buildAcieFeatureFingerprint(args: {
  crashPointsTail: number[];
  observationCount: number;
  regime: string;
  ewmaHitRate: number;
  psiProbability: number;
}): string {
  return computeFeatureHash({
    crashPointsTail: args.crashPointsTail.slice(-32),
    observationCount: args.observationCount,
    regime: args.regime,
    ewmaHitRate: Number(args.ewmaHitRate.toFixed(6)),
    psiProbability: Number(args.psiProbability.toFixed(6)),
  });
}

export function buildProvenance(args: {
  sourceGameId: string;
  targetGameId: string;
  online: Readonly<OnlineAdaptiveState>;
  evaluation: ACIEEvaluationResult;
  mode: PredictionExecutionMode;
  executionPath: string;
  probability: number;
  confidence: number;
  featureHash: string;
  modelName?: string;
  modelVersion?: string;
}): AcieProvenance {
  return {
    acie_instance_id: getSharedACIEInstanceId(),
    acie_observation_count: args.online.observationCount ?? 0,
    // Prefer observationCount as monotonic state version; online has no separate version field.
    acie_state_version: args.online.observationCount ?? 0,
    source_game_id: args.sourceGameId,
    target_game_id: args.targetGameId,
    feature_hash: args.featureHash,
    prediction_mode: args.mode,
    execution_path: args.executionPath,
    strategy_action: args.evaluation.strategy?.action ?? null,
    model_name: args.modelName ?? 'acie-psi',
    model_version: args.modelVersion ?? 'acie-v3',
    probability: args.probability,
    confidence: args.confidence,
    generated_at: new Date().toISOString(),
  };
}
