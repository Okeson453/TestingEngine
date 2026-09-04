/**
 * Strategy Layer — converts ACIE decision state into ENTRY / SKIP / REDUCED_ENTRY.
 *
 * High-frequency (HF) adaptive mode targets up to ~500 quality entries/day
 * without hard-blocking on mild evidence degradation.
 */

import { fractionalKellyStake } from '../stake/kelly-sizer.ts';
import {
  StrategyDecision,
  StrategyDecisionContext,
  StrategyPolicy,
  StrategyPolicyMode,
} from './types.ts';

/** Classic conservative defaults (lower entry rate). */
export const DEFAULT_STRATEGY_POLICY: StrategyPolicy = {
  mode: 'adaptive',
  supportedThreshold: 0.62,
  weakThreshold: 0.66,
  fallbackThreshold: 0.6,
  maxCalibrationError: 0.14,
  highUncertainty: 0.22,
  consecutiveLossReduceAt: 4,
  reducedStakeFactor: 0.5,
  defaultStake: 700,
};

/**
 * Tuned for ≥500 selective entries/day at 1.30× cash-out.
 * Slightly lower bars, stronger mean-reversion bias after low streaks,
 * softer consecutive-loss handling so the engine keeps evaluating.
 */
export const HIGH_FREQUENCY_STRATEGY_POLICY: StrategyPolicy = {
  mode: 'adaptive',
  supportedThreshold: 0.58,
  weakThreshold: 0.62,
  fallbackThreshold: 0.57,
  maxCalibrationError: 0.16,
  highUncertainty: 0.25,
  consecutiveLossReduceAt: 5,
  reducedStakeFactor: 0.6,
  defaultStake: 700,
};

export class StrategyLayer {
  private readonly policy: StrategyPolicy;

  constructor(policy: StrategyPolicy = HIGH_FREQUENCY_STRATEGY_POLICY) {
    this.policy = policy;
  }

  evaluate(ctx: StrategyDecisionContext): StrategyDecision {
    const { probability, evidence, calibrationError, uncertainty, riskState, regime } = ctx;
    const p = this.policy;

    // Extreme calibration failure — skip only in strict mode
    if (calibrationError > p.maxCalibrationError && evidence === 'DEGRADED') {
      if (p.mode === 'strict') {
        return this.skip(
          `PSI degraded and poorly calibrated (error ${(calibrationError * 100).toFixed(1)}%).`
        );
      }
    }

    if (p.mode === 'strict') {
      if (evidence === 'DEGRADED' || evidence === 'INSUFFICIENT') {
        return this.skip(`Strict policy: evidence=${evidence}. No entry.`);
      }
    }

    let effectiveProb = probability;
    let threshold = p.supportedThreshold;
    let usingFallback = false;

    if (evidence === 'SUPPORTED') {
      threshold = p.supportedThreshold;
    } else if (evidence === 'WEAK') {
      threshold = p.weakThreshold;
    } else if (p.mode === 'frequency_fallback') {
      effectiveProb = ctx.baselineProbability;
      threshold = p.fallbackThreshold;
      usingFallback = true;
    } else if (p.mode === 'adaptive') {
      effectiveProb = probability;
      threshold = Math.max(p.weakThreshold - 0.02, p.fallbackThreshold);
    } else {
      return this.skip(`Evidence ${evidence} under strict/unsupported policy.`);
    }

    // Regime-adaptive threshold adjustments (boost entry rate after low streaks)
    threshold = this.regimeAdjustedThreshold(threshold, regime, ctx);

    // Soft daily pacing: when approaching limit, raise bar slightly to keep best entries
    const used = riskState.dailyEntriesUsed ?? 0;
    const limit = riskState.dailyEntriesLimit ?? 500;
    if (limit > 0 && used / limit > 0.85) {
      threshold += 0.03;
    } else if (limit > 0 && used / limit < 0.25 && used < limit * 0.25) {
      // Early day: slightly lower bar to build volume with still-selective signals
      threshold -= 0.015;
    }

    if (uncertainty.total > p.highUncertainty && evidence !== 'SUPPORTED') {
      if (effectiveProb >= threshold) {
        return {
          action: 'REDUCED_ENTRY',
          stake: this.reducedStake(riskState),
          reason: `High uncertainty (${(uncertainty.total * 100).toFixed(1)}%). Reduced stake.`,
          confidence: Math.max(0, 1 - uncertainty.total),
          isOpportunity: true,
        };
      }
    }

    if (effectiveProb < threshold) {
      return this.skip(
        `Probability ${(effectiveProb * 100).toFixed(1)}% below threshold ${(threshold * 100).toFixed(0)}%` +
          (usingFallback ? ' (frequency fallback).' : '.')
      );
    }

    if (riskState.consecutiveLosses >= p.consecutiveLossReduceAt) {
      return {
        action: 'REDUCED_ENTRY',
        stake: this.reducedStake(riskState),
        reason: `${riskState.consecutiveLosses} consecutive losses — reduced stake.`,
        confidence: effectiveProb,
        isOpportunity: true,
      };
    }

    const sized = this.sizeStake(effectiveProb, evidence, riskState, p.defaultStake);
    return {
      action: 'ENTRY',
      stake: sized.stake,
      reason:
        `P=${(effectiveProb * 100).toFixed(1)}% ≥ ${(threshold * 100).toFixed(0)}%` +
        ` | evidence=${evidence}` +
        (usingFallback ? ' | frequency_fallback' : '') +
        ` | regime=${regime} policy ok.` +
        ` | ${sized.reason}`,
      confidence: effectiveProb,
      isOpportunity: true,
    };
  }

  /**
   * §6.4 Fractional-Kelly stake sizing. Falls back to defaultStake when
   * edge ≤ 0, evidence is weak, or Kelly is disabled via env.
   */
  private sizeStake(
    probability: number,
    evidence: StrategyDecisionContext['evidence'],
    riskState: StrategyDecisionContext['riskState'],
    defaultStake: number,
  ): { stake: number; reason: string } {
    if (process.env.ACIE_KELLY_ENABLED === '0') {
      return { stake: defaultStake, reason: 'kelly-disabled' };
    }
    const evidenceQuality =
      evidence === 'SUPPORTED' ? 1 : evidence === 'WEAK' ? 0.55 : 0.2;
    if (evidenceQuality < 0.3) {
      return { stake: defaultStake, reason: 'fixed-stake-weak-evidence' };
    }
    const bankroll = riskState.balance > 0 ? riskState.balance : defaultStake * 20;
    const result = fractionalKellyStake({
      calibratedProbability: probability,
      target: 1.3,
      bankroll,
      fraction: Number(process.env.ACIE_KELLY_FRACTION ?? 0.25),
      maxBankrollFraction: Number(process.env.ACIE_KELLY_MAX_FRAC ?? 0.05),
      sampleConfidence: Math.min(1, probability),
      calibrationConfidence: 0.8,
      evidenceQuality,
      modelAgreement: 0.8,
      drawdownPressure: Math.min(1, riskState.consecutiveLosses / 10),
    });
    if (result.stake <= 0) {
      return { stake: defaultStake, reason: result.reason };
    }
    // Blend with default so we never jump more than 2x default in one step.
    const stake = Math.max(
      1,
      Math.min(defaultStake * 2, Math.round(0.5 * result.stake + 0.5 * defaultStake)),
    );
    return { stake, reason: result.reason };
  }

  withPolicy(partial: Partial<StrategyPolicy> & { mode?: StrategyPolicyMode }): StrategyLayer {
    return new StrategyLayer({ ...this.policy, ...partial });
  }

  getPolicy(): StrategyPolicy {
    return this.policy;
  }

  private regimeAdjustedThreshold(
    base: number,
    regime: StrategyDecisionContext['regime'],
    ctx: StrategyDecisionContext
  ): number {
    let t = base;
    if (regime === 'deep-low' || regime === 'low-cluster') {
      t -= 0.025; // mean-reversion window — more entries
    } else if (regime === 'volatile') {
      t += 0.02; // require stronger edge
    } else if (regime === 'high-activity') {
      t -= 0.01;
    }
    // If ensemble CI lower bound is already above ~0.55, relax slightly
    const lo = ctx.confidenceInterval?.[0];
    if (typeof lo === 'number' && lo >= 0.55) {
      t -= 0.01;
    }
    return Math.max(0.52, Math.min(0.75, t));
  }

  private reducedStake(_risk: StrategyDecisionContext['riskState']): number {
    return Math.max(1, Math.round(this.policy.defaultStake * this.policy.reducedStakeFactor));
  }

  private skip(reason: string): StrategyDecision {
    return {
      action: 'SKIP',
      stake: 0,
      reason,
      confidence: 0,
      isOpportunity: false,
    };
  }
}
