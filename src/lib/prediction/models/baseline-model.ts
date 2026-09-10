/**
 * Baseline statistical model — honest empirical rates + constrained adjustments.
 *
 * P0 calibration fix (diagnosis 2026-09):
 * - Uses genuine 50/100/200 window hit rates (not SHORT_CAP=30 aliases).
 * - Gap/streak multipliers do NOT boost probability until proven OOS
 *   (held at 1.0; still recorded for diagnostics).
 * - Confidence is calibration-aware and cannot claim ~100% when ECE is poor.
 * - Adaptive state is serializable for restore across worker restarts.
 * - Soft shrink toward empirical 1.3× base rate when sample is thin or
 *   recent calibration is poor.
 */

import { CURRENT_FEATURE_VERSION } from '../features/feature-meta.ts';
import type { FeatureVector, ThresholdTarget, ModelIdentity, PredictionOutput, Regime, Dataset } from '../types.ts';
import { randomUUID } from 'crypto';

/** Approximate long-run P(crash ≥ 1.3) under fair crash process ≈ 1/1.3. */
export const EMPIRICAL_BASE_1_30 = 1 / 1.3;

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

/** Serializable adaptive state for worker restart continuity. */
export interface BaselineAdaptiveState {
  version: 1;
  gapMultiplier: number;
  streakMultiplier: number;
  anomalyMultiplier: number;
  shortWeight: number;
  midWeight: number;
  longWeight: number;
  outcomes: OutcomeSample[];
  /** Rolling absolute calibration error |p - y| mean over recent outcomes. */
  rollingAbsError: number;
  /** When true, gap/streak multipliers are allowed to move probability (default false). */
  allowHeuristicBoosts: boolean;
  updatedAt: string;
}

/**
 * Deterministic statistical baseline with constrained online adaptation.
 */
export class BaselineStatisticalModel implements PredictiveModel {
  readonly identity: ModelIdentity = {
    name: 'baseline-statistical',
    version: '1.2.0',
    featureVersion: CURRENT_FEATURE_VERSION,
    targetVersion: 'tv-1.0.0',
  };

  /** Diagnostic only — do not apply as probability boosts unless allowHeuristicBoosts. */
  private gapMultiplier = 1.0;
  private streakMultiplier = 1.0;
  private anomalyMultiplier = 1.0;
  /** Blend: 50-window / 100-window / 200-window (was 0.7 short / 0.3 long on mislabeled short-30). */
  private shortWeight = 0.3;
  private midWeight = 0.4;
  private longWeight = 0.3;
  private outcomes: OutcomeSample[] = [];
  private readonly maxOutcomes = 200;
  private lastGapActive = false;
  private lastStreakActive = false;
  private lastAnomalyActive = false;
  private rollingAbsError = 0.25; // start moderately uncertain
  /**
   * Env ALLOW_HEURISTIC_BOOSTS=1 re-enables gap/streak probability multipliers.
   * Default OFF per diagnosis: gambler's-fallacy risk until OOS proven.
   */
  private allowHeuristicBoosts =
    process.env.ALLOW_HEURISTIC_BOOSTS === '1' ||
    process.env.ALLOW_HEURISTIC_BOOSTS === 'true';

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
      // No aggressive short-weight inflation from training hit rate.
    } catch {
      /* soft */
    }
  }

  observeOutcome(
    predicted: number,
    actual: 0 | 1,
    _crashPoint?: number,
    _target?: ThresholdTarget,
  ): void {
    if (!Number.isFinite(predicted)) return;
    const p = Math.min(0.999, Math.max(0.001, predicted));
    this.outcomes.push({
      predicted: p,
      actual,
      gapActive: this.lastGapActive,
      streakActive: this.lastStreakActive,
      anomalyActive: this.lastAnomalyActive,
    });
    if (this.outcomes.length > this.maxOutcomes) {
      this.outcomes.shift();
    }
    // Rolling abs calibration error (EMA)
    const absErr = Math.abs(p - actual);
    this.rollingAbsError = 0.95 * this.rollingAbsError + 0.05 * absErr;

    // Only adapt anomaly multiplier mildly; gap/streak stay at 1.0 unless allowed.
    if (this.outcomes.length >= 20 && this.outcomes.length % 20 === 0) {
      this.recomputeMultipliers();
    }
  }

  private recomputeMultipliers(): void {
    if (!this.allowHeuristicBoosts) {
      // Keep diagnostic multipliers near 1; only anomaly may damp slightly.
      this.gapMultiplier = 1.0;
      this.streakMultiplier = 1.0;
    }
    const subset = this.outcomes.filter((o) => o.anomalyActive);
    if (subset.length >= 8) {
      const residual =
        subset.reduce((s, o) => s + (o.predicted - o.actual), 0) / subset.length;
      // If overconfident under anomaly, damp slightly toward conservative
      const step = Math.max(-0.05, Math.min(0.05, -residual * 0.3));
      this.anomalyMultiplier = Math.max(0.7, Math.min(1.0, this.anomalyMultiplier + step));
    }
    if (this.allowHeuristicBoosts) {
      const adapt = (
        filter: (o: OutcomeSample) => boolean,
        current: number,
        lo: number,
        hi: number,
      ): number => {
        const sub = this.outcomes.filter(filter);
        if (sub.length < 10) return current;
        const residual =
          sub.reduce((s, o) => s + (o.predicted - o.actual), 0) / sub.length;
        const step = Math.max(-0.05, Math.min(0.05, -residual * 0.4));
        return Math.max(lo, Math.min(hi, current + step));
      };
      this.gapMultiplier = adapt((o) => o.gapActive, this.gapMultiplier, 0.85, 1.15);
      this.streakMultiplier = adapt((o) => o.streakActive, this.streakMultiplier, 0.85, 1.15);
    }
  }

  predict(
    features: FeatureVector,
    target: ThresholdTarget,
    regime: Regime | null
  ): PredictionOutput {
    const v = features.values;
    const targetKey =
      target === 1.3 ? '1_30' : target === 2.0 ? '2_00' : target === 5.0 ? '5_00' : '10_00';

    // SAFE_BASELINE mode via process global (set by safe-baseline-controller)
    const g = globalThis as {
      __safeBaselineMode__?: boolean;
      __safeBaselineProb__?: number;
      __safeBaselineSnap__?: { modelBrier: number; ece: number; n: number };
    };
    if (g.__safeBaselineMode__ === true) {
      const probability =
        typeof g.__safeBaselineProb__ === 'number'
          ? g.__safeBaselineProb__
          : target === 1.3
            ? EMPIRICAL_BASE_1_30
            : Math.min(0.95, Math.max(0.05, 1 / Number(target)));
      const snap = g.__safeBaselineSnap__ ?? { modelBrier: 0, ece: 0, n: 0 };
      const now = new Date();
      const quality = features.meta.dataQualityScore;
      return {
        predictionId: randomUUID(),
        model: { ...this.identity, version: '1.2.0-safe' },
        target,
        score: probability,
        probability,
        confidence: Math.min(0.55, 0.35 + 0.2 * Math.min(1, (v.sample_size ?? 0) / 100)),
        regime,
        dataQuality: quality,
        featureSummary: {
          safe_baseline: 1,
          model_brier: snap.modelBrier,
          ece: snap.ece,
          safe_n: snap.n,
          sample_size: v.sample_size ?? 0,
        },
        reasoning: [
          `SAFE_BASELINE active — empirical rate only (${(probability * 100).toFixed(1)}%)`,
          `Trigger metrics: brier=${snap.modelBrier.toFixed(3)} ece=${snap.ece.toFixed(3)} n=${snap.n}`,
        ],
        timestamp: now.toISOString(),
        expiresAt: new Date(now.getTime() + 60_000).toISOString(),
      };
    }

    // Prefer exact window rates (hit_rate_*) then fv-1 keys (now true windows).
    const rate50 =
      v.hit_rate_50 ??
      v[`hit_${targetKey}_50`] ??
      (target === 1.3 ? (v.short_hit_13 ?? EMPIRICAL_BASE_1_30) : EMPIRICAL_BASE_1_30);
    const rate100 =
      v.hit_rate_100 ??
      v[`hit_${targetKey}_100`] ??
      rate50;
    const rate200 =
      v.hit_rate_200 ??
      rate100;

    let baseProb =
      this.shortWeight * rate50 +
      this.midWeight * rate100 +
      this.longWeight * rate200;

    // Soft shrink toward empirical base when sample is thin
    const sampleSize = v.sample_size ?? v.n ?? features.meta.sampleSize ?? 0;
    const shrink = Math.min(1, sampleSize / 100);
    if (target === 1.3) {
      baseProb = shrink * baseProb + (1 - shrink) * EMPIRICAL_BASE_1_30;
    }

    // Calibration shrink: if recent |p-y| is high, pull toward empirical base
    const calQuality = Math.max(0, Math.min(1, 1 - this.rollingAbsError / 0.35));
    if (target === 1.3 && calQuality < 0.7) {
      const pull = 0.4 * (1 - calQuality);
      baseProb = (1 - pull) * baseProb + pull * EMPIRICAL_BASE_1_30;
    }

    // Volatility: mild conservative only (no aggressive boost)
    const rollStd = v.roll_std_50 ?? 0;
    if (rollStd > 8) {
      baseProb *= 0.97;
    }

    // Gap / streak: diagnostic flags only unless allowHeuristicBoosts
    const since = v[`since_${targetKey}`] ?? v.since_1_30 ?? 0;
    const consecBelow = v.consec_below_1_30 ?? 0;
    this.lastGapActive = since >= 3;
    this.lastStreakActive = consecBelow >= 3;
    this.lastAnomalyActive = Boolean(regime?.dimensions?.anomalyState);

    if (this.allowHeuristicBoosts) {
      if (this.lastGapActive) baseProb *= this.gapMultiplier;
      if (this.lastStreakActive) baseProb *= this.streakMultiplier;
      if (this.lastAnomalyActive) baseProb *= this.anomalyMultiplier;
    } else if (this.lastAnomalyActive) {
      // Mild anomaly damp only
      baseProb *= Math.min(1, this.anomalyMultiplier);
    }

    // Regime name adjustments: very mild, no large boosts
    const dims = regime?.dimensions;
    if (dims?.anomalyState) {
      baseProb *= 0.98;
    }

    // Cap confidence-era overstatement: never claim > ~88% without strong sample+cal
    const hardCap =
      sampleSize >= 100 && calQuality >= 0.75 ? 0.88 : sampleSize >= 50 ? 0.82 : 0.78;
    let probability = Math.max(0.05, Math.min(hardCap, baseProb));

    // Apply fitted calibrator when boot has published a warm calibrator fn
    const cal = (globalThis as {
      __calibrateProbability__?: (p: number, regimeKey: string, sampleSize: number) => number;
    }).__calibrateProbability__;
    if (typeof cal === 'function') {
      try {
        const regimeKey = regime?.id ?? regime?.name ?? 'global';
        probability = cal(probability, regimeKey, sampleSize);
        probability = Math.max(0.05, Math.min(hardCap, probability));
      } catch { /* soft */ }
    }

    // Calibration-aware confidence (NOT ≈ probability)
    const sampleFactor = Math.min(1, sampleSize / 100);
    const quality = features.meta.dataQualityScore;
    const regimeStab = regime?.confidence ?? 0.5;
    let confidence =
      0.35 * sampleFactor +
      0.25 * quality +
      0.25 * calQuality +
      0.15 * regimeStab;
    // If calibration is poor, hard-cap confidence
    if (calQuality < 0.5) {
      confidence = Math.min(confidence, 0.5);
    }
    if (this.rollingAbsError > 0.28) {
      confidence = Math.min(confidence, 0.45);
    }
    confidence = Math.max(0.15, Math.min(0.85, confidence));

    const reasoning: string[] = [
      `Baseline statistical v1.2 (honest windows, heuristic boosts ${this.allowHeuristicBoosts ? 'ON' : 'OFF'})`,
      `hit50=${(rate50 * 100).toFixed(1)}% hit100=${(rate100 * 100).toFixed(1)}% hit200=${(rate200 * 100).toFixed(1)}%`,
      `blend→${(baseProb * 100).toFixed(1)}% capped→${(probability * 100).toFixed(1)}%`,
      `calAbsErr=${this.rollingAbsError.toFixed(3)} calQ=${calQuality.toFixed(2)} conf=${(confidence * 100).toFixed(0)}%`,
      `Rounds since ≥${target}x: ${since}; sample=${sampleSize}`,
    ];
    if (regime) reasoning.push(`Regime: ${regime.name} (id=${regime.id})`);

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
        hit_rate_50: rate50,
        hit_rate_100: rate100,
        hit_rate_200: rate200,
        since: Number(since),
        sample_size: sampleSize,
        roll_mean_50: v.roll_mean_50 ?? 0,
        roll_std_50: v.roll_std_50 ?? 0,
        rolling_abs_error: this.rollingAbsError,
        calibration_quality: calQuality,
        gap_multiplier: this.gapMultiplier,
        streak_multiplier: this.streakMultiplier,
        heuristic_boosts: this.allowHeuristicBoosts ? 1 : 0,
      },
      reasoning,
      timestamp: now.toISOString(),
      expiresAt: new Date(now.getTime() + 60_000).toISOString(),
    };
  }

  getAdaptiveState(): {
    gapMultiplier: number;
    streakMultiplier: number;
    anomalyMultiplier: number;
    outcomeCount: number;
    rollingAbsError: number;
    allowHeuristicBoosts: boolean;
  } {
    return {
      gapMultiplier: this.gapMultiplier,
      streakMultiplier: this.streakMultiplier,
      anomalyMultiplier: this.anomalyMultiplier,
      outcomeCount: this.outcomes.length,
      rollingAbsError: this.rollingAbsError,
      allowHeuristicBoosts: this.allowHeuristicBoosts,
    };
  }

  /** Serialize for worker_state persistence. */
  exportState(): BaselineAdaptiveState {
    return {
      version: 1,
      gapMultiplier: this.gapMultiplier,
      streakMultiplier: this.streakMultiplier,
      anomalyMultiplier: this.anomalyMultiplier,
      shortWeight: this.shortWeight,
      midWeight: this.midWeight,
      longWeight: this.longWeight,
      outcomes: this.outcomes.slice(-this.maxOutcomes),
      rollingAbsError: this.rollingAbsError,
      allowHeuristicBoosts: this.allowHeuristicBoosts,
      updatedAt: new Date().toISOString(),
    };
  }

  /** Restore after worker restart. */
  importState(state: BaselineAdaptiveState | null | undefined): void {
    if (!state || state.version !== 1) return;
    if (Number.isFinite(state.gapMultiplier)) this.gapMultiplier = state.gapMultiplier;
    if (Number.isFinite(state.streakMultiplier)) this.streakMultiplier = state.streakMultiplier;
    if (Number.isFinite(state.anomalyMultiplier)) this.anomalyMultiplier = state.anomalyMultiplier;
    if (Number.isFinite(state.shortWeight)) this.shortWeight = state.shortWeight;
    if (Number.isFinite(state.midWeight)) this.midWeight = state.midWeight;
    if (Number.isFinite(state.longWeight)) this.longWeight = state.longWeight;
    if (Number.isFinite(state.rollingAbsError)) this.rollingAbsError = state.rollingAbsError;
    if (Array.isArray(state.outcomes)) {
      this.outcomes = state.outcomes.slice(-this.maxOutcomes);
    }
    // Env still wins for heuristic boosts at runtime
    this.allowHeuristicBoosts =
      process.env.ALLOW_HEURISTIC_BOOSTS === '1' ||
      process.env.ALLOW_HEURISTIC_BOOSTS === 'true' ||
      Boolean(state.allowHeuristicBoosts && process.env.ALLOW_HEURISTIC_BOOSTS !== '0');
  }
}

/** Process-wide singleton so online learning persists across predictions. */
export const globalBaselineModel = new BaselineStatisticalModel();
