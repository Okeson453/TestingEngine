/**
 * Gap-conditional candidate model (FINAL_REPORT-2, item #3).
 *
 * Direct port of the report's two logistic models for P(next crash ≥ 1.30×):
 *
 *   Model A (gap regime INACTIVE — current regime since Sept 10):
 *     logit(P) = 1.145 − 0.023·log_m_lag1        (pseudo R² 0.0001, honest)
 *
 *   Model B (gap regime ACTIVE — Sept 8-9 behavior):
 *     logit(P) = 1.203 + 1.491·gap_z − 0.309·log_lag1_z
 *     walk-forward AUC 0.69-0.84 while the signal was present; ~0.5 after.
 *
 * The switch is controlled EXCLUSIVELY by the gap-regime test (item #4,
 * rolling ~800-round Spearman(gap, y), p<0.01). Default is INACTIVE — the
 * report is explicit that the gap signal is dead in the current regime
 * (OOS AUC 0.51 on the last 2,000 rounds) and this model must never fire
 * Model B on hope.
 *
 * NOTE (registry scope): model-registry is the offline/FALLBACK_BASELINE
 * registry — live ED/BG scoring runs ACIE PSI models. This is a candidate:
 * it becomes live only through the promotion gates (model-gate /
 * walk-forward-protocol), which is exactly where item #7's policy question
 * (blanket 50k gate vs tailored gate) applies.
 */

import { randomUUID } from 'crypto';
import type { PredictiveModel } from './baseline-model.ts';
import type { FeatureVector, ThresholdTarget, ModelIdentity, PredictionOutput, Regime, Dataset } from '../types.ts';
import { CURRENT_FEATURE_VERSION } from '../features/feature-meta.ts';

/** Report §5 Model A coefficients (log_m_lag1-only logistic). */
export const MODEL_A = { intercept: 1.145, logLag1: -0.023 } as const;
/** Report §5 Model B coefficients (gap_z + log_lag1_z logistic). */
export const MODEL_B = { intercept: 1.203, gapZ: 1.491, logLag1Z: -0.309 } as const;

export interface GapZParams {
  gapMeanS: number;
  gapStdS: number;
  logLag1Mean: number;
  logLag1Std: number;
}

/** Neutral standardization until fit() derives real ones from a Dataset.
 * With neutral z-params Model B is only meaningful after a fit — one more
 * reason the default regime state is INACTIVE. */
export const NEUTRAL_Z: GapZParams = {
  gapMeanS: 0,
  gapStdS: 1,
  logLag1Mean: 0,
  logLag1Std: 1,
};

function sigmoid(x: number): number {
  return 1 / (1 + Math.exp(-x));
}

function clamp01(p: number): number {
  return Math.min(0.99, Math.max(0.01, p));
}

export class GapConditionalModel implements PredictiveModel {
  readonly identity: ModelIdentity = {
    name: 'gap-conditional',
    version: '1.0.0',
    featureVersion: CURRENT_FEATURE_VERSION,
    targetVersion: 'tv-1.0.0',
  };

  private gapSignalActive = false;
  private z: GapZParams = { ...NEUTRAL_Z };

  /** Item #4 integration point: the rolling-Spearman regime test flips this. */
  setGapSignalActive(active: boolean): void {
    this.gapSignalActive = active;
  }

  isGapSignalActive(): boolean {
    return this.gapSignalActive;
  }

  setStandardization(z: GapZParams): void {
    this.z = {
      gapMeanS: z.gapMeanS,
      gapStdS: z.gapStdS > 0 ? z.gapStdS : 1,
      logLag1Mean: z.logLag1Mean,
      logLag1Std: z.logLag1Std > 0 ? z.logLag1Std : 1,
    };
  }

  getStandardization(): Readonly<GapZParams> {
    return this.z;
  }

  /** Derive z-params from training data (rows scored in the fv-2.1+ space). */
  fit(trainingData: Dataset): void {
    const gaps: number[] = [];
    const logLags: number[] = [];
    for (const row of trainingData.rows) {
      const g = row.features.values['gap_s'];
      const l = row.features.values['log_lag_1'];
      if (typeof g === 'number' && Number.isFinite(g) && g > 0) gaps.push(g);
      if (typeof l === 'number' && Number.isFinite(l) && l > 0) logLags.push(l);
    }
    if (gaps.length >= 100 && logLags.length >= 100) {
      this.setStandardization({
        gapMeanS: mean(gaps),
        gapStdS: std(gaps),
        logLag1Mean: mean(logLags),
        logLag1Std: std(logLags),
      });
    }
  }

  predict(
    features: FeatureVector,
    target: ThresholdTarget,
    regime: Regime | null,
  ): PredictionOutput {
    const v = features.values;
    const logLag1 = v['log_lag_1'] ?? 0;
    const gapS = v['gap_s'] ?? 0;
    const gapHasData = (v['gap_s'] ?? 0) > 0;

    let probability: number;
    const reasoning: string[] = [];
    if (this.gapSignalActive && gapHasData) {
      const gapZ = (gapS - this.z.gapMeanS) / this.z.gapStdS;
      const logLag1Z = (logLag1 - this.z.logLag1Mean) / this.z.logLag1Std;
      probability = clamp01(
        sigmoid(MODEL_B.intercept + MODEL_B.gapZ * gapZ + MODEL_B.logLag1Z * logLag1Z),
      );
      reasoning.push(
        `MODEL_B (gap regime ACTIVE): gap=${gapS.toFixed(1)}s z=${gapZ.toFixed(2)} log_lag1=${logLag1.toFixed(3)}`,
      );
    } else {
      // Model A. If the regime flag says active but gap data is missing
      // (beganAt never flowed), fall back to Model A rather than trusting
      // a zero-imputed gap — a 0s gap would read as an extreme short gap.
      probability = clamp01(sigmoid(MODEL_A.intercept + MODEL_A.logLag1 * logLag1));
      reasoning.push(
        this.gapSignalActive
          ? 'MODEL_A fallback: gap regime active but gap_s missing (no beganAt data)'
          : 'MODEL_A: gap regime inactive (per FINAL_REPORT-2, dead since Sept 10)',
      );
    }

    // Target guard: coefficients are fit for the 1.30 threshold only.
    if (target !== 1.3) {
      probability = clamp01(1 / Number(target));
      reasoning.push(`coefficient set is 1.30-specific — base-rate fallback for ${target}`);
    }

    const now = new Date();
    return {
      predictionId: randomUUID(),
      model: this.identity,
      target,
      score: probability,
      probability,
      // Candidate-grade confidence: never claim ensemble-level certainty.
      confidence: 0.3,
      regime,
      dataQuality: features.meta.dataQualityScore,
      featureSummary: {
        gap_s: gapS,
        log_lag_1: logLag1,
        gap_regime_active: this.gapSignalActive ? 1 : 0,
        sample_size: features.meta.sampleSize,
      },
      reasoning,
      timestamp: now.toISOString(),
      expiresAt: new Date(now.getTime() + 60_000).toISOString(),
    };
  }
}

function mean(xs: number[]): number {
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}

function std(xs: number[]): number {
  const m = mean(xs);
  const v = xs.reduce((a, b) => a + (b - m) ** 2, 0) / Math.max(1, xs.length - 1);
  return Math.sqrt(v);
}
