/**
 * Baseline statistical / heuristic model with online adaptive multipliers.
 * Uses empirical rates from features + outcome-driven multiplier updates.
 */

import { CURRENT_FEATURE_VERSION } from '../features/feature-meta.ts';
import type { FeatureVector, ThresholdTarget, ModelIdentity, PredictionOutput, Regime, Dataset } from '../types.ts';
import { randomUUID } from 'crypto';

/**
 * Model capability contract.
 * Baseline models may no-op fit(); trainable models implement fit().
 */
export interface PredictiveModel {
  readonly identity: ModelIdentity;
  /** Optional training step. Baseline statistical models leave this as no-op. */
  fit?(trainingData: Dataset): void | Promise<void>;
  predict(
    features: FeatureVector,
    target: ThresholdTarget,
    regime: Regime | null
  ): PredictionOutput;
  /** Optional online outcome feedback for adaptive models. */
  observeOutcome?(
    predicted: number,
    actual: 0 | 1,
    crashPoint?: number,
    target?: ThresholdTarget,
  ): void;
}

interface OutcomeSample {
  predicted: number;
  actual: 0 | 1;
  gapActive: boolean;
  streakActive: boolean;
  anomalyActive: boolean;
}

/**
 * Deterministic statistical baseline with online adaptive multipliers.
 */
export class BaselineStatisticalModel implements PredictiveModel {
  readonly identity: ModelIdentity = {
    name: 'baseline-statistical',
    version: '1.1.0',
    featureVersion: CURRENT_FEATURE_VERSION,
    targetVersion: 'tv-1.0.0',
  };

  private gapMultiplier = 1.15;
  private streakMultiplier = 1.1;
  private anomalyMultiplier = 0.7;
  private shortWeight = 0.7;
  private longWeight = 0.3;
  private outcomes: OutcomeSample[] = [];
  private readonly maxOutcomes = 200;
  private lastGapActive = false;
  private lastStreakActive = false;
  private lastAnomalyActive = false;

  /**
   * Optional batch fit: seed empirical priors from training rows if available.
   */
  fit(trainingData: Dataset): void {
    try {
      const rows = (trainingData as { rows?: Array<{ crashPoint?: number; y?: number }> })?.rows
        ?? (trainingData as { samples?: Array<{ crashPoint?: number }> })?.samples
        ?? [];
      if (!Array.isArray(rows) || rows.length < 20) return;
      const cps = rows
        .map((r) => Number((r as { crashPoint?: number }).crashPoint ?? (r as { y?: number }).y))
        .filter((x) => Number.isFinite(x) && x > 0);
      if (cps.length < 20) return;
      const hit = cps.filter((c) => c >= 1.3).length / cps.length;
      // Mild prior nudge toward observed hit rate via short/long blend weights
      if (hit < 0.4) {
        this.shortWeight = 0.6;
        this.longWeight = 0.4;
      } else if (hit > 0.7) {
        this.shortWeight = 0.8;
        this.longWeight = 0.2;
      }
    } catch {
      /* soft */
    }
  }

  /**
   * Online learning: record outcome and periodically adapt multipliers.
   */
  observeOutcome(
    predicted: number,
    actual: 0 | 1,
    _crashPoint?: number,
    _target?: ThresholdTarget,
  ): void {
    if (!Number.isFinite(predicted)) return;
    this.outcomes.push({
      predicted: Math.min(0.999, Math.max(0.001, predicted)),
      actual,
      gapActive: this.lastGapActive,
      streakActive: this.lastStreakActive,
      anomalyActive: this.lastAnomalyActive,
    });
    if (this.outcomes.length > this.maxOutcomes) {
      this.outcomes.shift();
    }
    if (this.outcomes.length >= 10 && this.outcomes.length % 10 === 0) {
      this.recomputeMultipliers();
    }
  }

  private recomputeMultipliers(): void {
    const adapt = (
      filter: (o: OutcomeSample) => boolean,
      current: number,
      lo: number,
      hi: number,
    ): number => {
      const subset = this.outcomes.filter(filter);
      if (subset.length < 5) return current;
      // Mean residual: predicted - actual; positive => overconfident
      const residual =
        subset.reduce((s, o) => s + (o.predicted - o.actual), 0) / subset.length;
      // If overconfident on this condition, reduce multiplier toward 1 (or lower for anomaly)
      const step = Math.max(-0.08, Math.min(0.08, -residual * 0.5));
      return Math.max(lo, Math.min(hi, current + step));
    };
    this.gapMultiplier = adapt((o) => o.gapActive, this.gapMultiplier, 0.5, 2.0);
    this.streakMultiplier = adapt((o) => o.streakActive, this.streakMultiplier, 0.5, 2.0);
    this.anomalyMultiplier = adapt((o) => o.anomalyActive, this.anomalyMultiplier, 0.3, 1.0);
  }

  predict(
    features: FeatureVector,
    target: ThresholdTarget,
    regime: Regime | null
  ): PredictionOutput {
    const v = features.values;
    const targetKey =
      target === 1.3 ? '1_30' : target === 2.0 ? '2_00' : target === 5.0 ? '5_00' : '10_00';

    let baseProb = v[`hit_${targetKey}_50`] ?? 0.3;
    const longProb = v[`hit_${targetKey}_100`] ?? baseProb;
    baseProb = this.shortWeight * baseProb + this.longWeight * longProb;

    // P1.6: Consume More Features in Baseline Model
    // Volatility adjustment from roll_std_50
    const rollStd = v.roll_std_50 ?? 0;
    if (rollStd > 5) {
      baseProb *= 0.95; // high vol → slightly more conservative
    } else if (rollStd > 0 && rollStd < 1.5) {
      baseProb = Math.min(0.95, baseProb * 1.03);
    }

    // Time-of-day mild adjustment
    const hour = v.hour_utc;
    if (typeof hour === 'number' && Number.isFinite(hour)) {
      // Night hours (0-6 UTC) often different dynamics
      if (hour >= 0 && hour < 6) baseProb *= 0.97;
    }

    // Pacing adjustment
    const since = v[`since_${targetKey}`] ?? 0;
    const expectedGap = 1 / Math.max(baseProb, 0.05);
    this.lastGapActive = since > expectedGap * 1.5;
    if (this.lastGapActive) {
      baseProb = Math.min(0.95, baseProb * this.gapMultiplier);
    }

    const consecKey = target <= 1.3 ? 'consec_below_1_30' : 'consec_below_2_00';
    const consec = v[consecKey] ?? 0;
    this.lastStreakActive = consec >= 8;
    if (this.lastStreakActive) {
      baseProb = Math.min(0.95, baseProb * this.streakMultiplier);
    }

    // P1.7: Use Regime Dimensions in Baseline Model
    const dims = regime?.dimensions as Record<string, unknown> | undefined;
    this.lastAnomalyActive = Boolean(dims?.anomalyState);
    if (this.lastAnomalyActive) {
      baseProb *= this.anomalyMultiplier;
    }
    const lowConc = Number(dims?.lowMultiplierConcentration ?? dims?.lowConc ?? NaN);
    if (Number.isFinite(lowConc) && lowConc > 0.7) {
      baseProb *= 0.92; // deep-low concentration → slightly lower
    }
    const vol = Number(dims?.volatility ?? dims?.vol ?? NaN);
    if (Number.isFinite(vol) && vol > 15) {
      baseProb *= 0.93;
    }

    // Additional regime dimension features
    const streakState = dims?.streakState as string | undefined;
    if (streakState === 'low') {
      baseProb *= 1.1;
    }
    const thresholdFreq = dims?.thresholdFrequency as Record<string, number> | undefined;
    if (thresholdFreq && thresholdFreq['1.30'] < 0.5) {
      baseProb *= 0.9;
    }

    const probability = Math.max(0, Math.min(1, baseProb));
    const sampleFactor = Math.min(1, (v.sample_size ?? 0) / 50);
    const quality = features.meta.dataQualityScore;
    const confidence = Math.max(
      0,
      Math.min(1, 0.4 * sampleFactor + 0.4 * quality + 0.2 * (regime?.confidence ?? 0.5))
    );

    const reasoning: string[] = [
      `Baseline statistical heuristic (adaptive multipliers)`,
      `Base hit-rate (50): ${((v[`hit_${targetKey}_50`] ?? 0) * 100).toFixed(1)}%`,
      `Rounds since last ≥${target}x: ${since}`,
      `Sample size: ${v.sample_size ?? 0}`,
      `gapMult=${this.gapMultiplier.toFixed(3)} streakMult=${this.streakMultiplier.toFixed(3)} anomalyMult=${this.anomalyMultiplier.toFixed(3)}`,
    ];
    if (regime) reasoning.push(`Regime: ${regime.name}`);

    const now = new Date();
    return {
      predictionId: randomUUID(),
      model: this.identity,
      target,
      score: probability,
      probability,
      confidence,
      regime,
      dataQuality: quality,
      featureSummary: {
        hit_rate_50: v[`hit_${targetKey}_50`] ?? 0,
        since,
        sample_size: v.sample_size ?? 0,
        roll_mean_50: v.roll_mean_50 ?? 0,
        roll_std_50: v.roll_std_50 ?? 0,
        gap_multiplier: this.gapMultiplier,
        streak_multiplier: this.streakMultiplier,
      },
      reasoning,
      timestamp: now.toISOString(),
      expiresAt: new Date(now.getTime() + 60_000).toISOString(),
    };
  }

  /** Test/ops introspection */
  getAdaptiveState(): {
    gapMultiplier: number;
    streakMultiplier: number;
    anomalyMultiplier: number;
    outcomeCount: number;
  } {
    return {
      gapMultiplier: this.gapMultiplier,
      streakMultiplier: this.streakMultiplier,
      anomalyMultiplier: this.anomalyMultiplier,
      outcomeCount: this.outcomes.length,
    };
  }
}

/** Process-wide singleton so online learning persists across predictions. */
export const globalBaselineModel = new BaselineStatisticalModel();
