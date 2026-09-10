/**
 * Prediction replay helper (P2) — recompute probability from stored feature summary
 * using the current baseline model for audit / "why was this predicted".
 */

import { BaselineStatisticalModel } from '../models/baseline-model.ts';
import type { FeatureVector, Regime, ThresholdTarget } from '../types.ts';
import { CURRENT_FEATURE_VERSION } from '../features/feature-meta.ts';

export interface ReplayInput {
  featureSummary: Record<string, number>;
  target?: ThresholdTarget;
  regimeName?: string | null;
  sampleSize?: number;
  dataQualityScore?: number;
}

export interface ReplayResult {
  probability: number;
  confidence: number;
  reasoning: string[];
  featureSummary: Record<string, number>;
  modelVersion: string;
}

export function replayBaselinePrediction(input: ReplayInput): ReplayResult {
  const model = new BaselineStatisticalModel();
  const values = { ...input.featureSummary };
  if (values.hit_rate_50 == null && values.hit_1_30_50 != null) {
    values.hit_rate_50 = values.hit_1_30_50;
  }
  if (values.hit_rate_100 == null && values.hit_1_30_100 != null) {
    values.hit_rate_100 = values.hit_1_30_100;
  }
  const sampleSize = input.sampleSize ?? values.sample_size ?? 50;
  values.sample_size = sampleSize;

  const features: FeatureVector = {
    roundId: 'replay',
    timestamp: new Date().toISOString(),
    featureVersion: CURRENT_FEATURE_VERSION,
    values,
    meta: {
      sampleSize,
      dataQualityScore: input.dataQualityScore ?? 0.8,
      missingFeatureCount: 0,
    },
  };

  let regime: Regime | null = null;
  if (input.regimeName) {
    regime = {
      id: input.regimeName,
      name: input.regimeName,
      dimensions: {
        lowMultiplierConcentration: 0,
        highMultiplierConcentration: 0,
        volatility: 0,
        streakState: 'neutral',
        thresholdFrequency: {},
        anomalyState: false,
      },
      confidence: 0.5,
      explanation: ['replay'],
      detectedAt: new Date().toISOString(),
    };
  }

  const out = model.predict(features, input.target ?? 1.3, regime);
  return {
    probability: out.probability,
    confidence: out.confidence,
    reasoning: out.reasoning,
    featureSummary: out.featureSummary,
    modelVersion: out.model.version,
  };
}
