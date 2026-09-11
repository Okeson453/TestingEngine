/**
 * Strategy Layer — converts ACIE decision state into ENTRY / SKIP / REDUCED_ENTRY.
 *
 * High-frequency (HF) adaptive mode targets up to ~500 quality entries/day
 * without hard-blocking on mild evidence degradation.
 *
 * IMPORTANT — consecutiveLosses semantics:
 *   ACIEEngine currently increments consecutiveLosses on every crash < 1.30×
 *   in the live stream (not on confirmed entry/bet losses). Hard SKIP after
 *   2 stream "losses" caused multi-hour signal blackouts. Hard cooldown is
 *   therefore disabled (skipAt=999) until entry-outcome tracking is wired.
 *   Stake reduction + mild threshold escalation remain.
 */

import { fractionalKellyStake } from '../stake/kelly-sizer.ts';
import type {
  StrategyDecision,
  StrategyDecisionContext,
  StrategyPolicy,
  StrategyPolicyMode,
} from './types.ts';

/**
 * Fair odds for 1.30× cash-out: 1/1.30 ≈ 0.7692.
 * Any ENTRY threshold BELOW fair is negative EV on average and drags
 * realized win rate toward (or below) the base rate.
 *
 * QUALITY mode (default): require probability ≥ fair + edge so emitted
 * signals are positive-EV candidates. Volume is secondary to hit rate.
 */
const FAIR_130 = 1 / 1.3;
const QUALITY_EDGE = Number(process.env.ACIE_QUALITY_EDGE ?? 0.025);
const STRONG_EDGE = Number(process.env.ACIE_STRONG_EDGE ?? 0.045);

/** Quality-first defaults — thresholds at/above fair + edge. */
export const DEFAULT_STRATEGY_POLICY: StrategyPolicy = {
  mode: 'adaptive',
  supportedThreshold: FAIR_130 + QUALITY_EDGE, // ~0.794
  weakThreshold: FAIR_130 + STRONG_EDGE, // ~0.814
  fallbackThreshold: FAIR_130 + QUALITY_EDGE + 0.01,
  maxCalibrationError: 0.12,
  highUncertainty: 0.18,
  consecutiveLossReduceAt: 3,
  reducedStakeFactor: 0.45,
  defaultStake: 700,
  consecutiveLossSkipAt: 999,
  consecutiveLossMaxSkip: 0,
  lossStreakThresholdEscalation: 0.015,
};

/**
 * High-frequency policy kept for explicit opt-in only.
 * Still floors at fair odds so volume cannot force negative-EV entries.
 */
export const HIGH_FREQUENCY_STRATEGY_POLICY: StrategyPolicy = {
  mode: 'adaptive',
  supportedThreshold: FAIR_130 + 0.01,
  weakThreshold: FAIR_130 + QUALITY_EDGE,
  fallbackThreshold: FAIR_130,
  maxCalibrationError: 0.14,
  highUncertainty: 0.22,
  consecutiveLossReduceAt: 4,
  reducedStakeFactor: 0.55,
  defaultStake: 700,
  consecutiveLossSkipAt: 999,
  consecutiveLossMaxSkip: 0,
  lossStreakThresholdEscalation: 0.012,
};

/** Resolve active policy: QUALITY (default) or HF when ACIE_STRATEGY_MODE=hf. */
function resolveDefaultPolicy(): StrategyPolicy {
  const mode = String(process.env.ACIE_STRATEGY_MODE ?? "quality").toLowerCase();
  return mode === "hf" || mode === "high_frequency"
    ? HIGH_FREQUENCY_STRATEGY_POLICY
    : DEFAULT_STRATEGY_POLICY;
}

export class StrategyLayer {
  private readonly policy: StrategyPolicy;

  constructor(policy: StrategyPolicy = resolveDefaultPolicy()) {
    this.policy = policy;
  }

  evaluate(ctx: StrategyDecisionContext): StrategyDecision {
    const { probability, evidence, calibrationError, uncertainty, riskState, regime } = ctx;
    const p = this.policy;
    const cl = riskState?.consecutiveLosses ?? 0;

    // Hard consecutive-loss SKIP is intentionally inert (skipAt=999) while
    // consecutiveLosses tracks stream outcomes rather than entry outcomes.
    // When true entry-loss tracking is available, lower skipAt again.
    if (cl >= p.consecutiveLossSkipAt && p.consecutiveLossMaxSkip > 0) {
      const skipRounds = cl - p.consecutiveLossSkipAt + 1;
      if (skipRounds <= p.consecutiveLossMaxSkip) {
        return this.skip(
          `${cl} consecutive losses — cooling off (skip ${skipRounds}/${p.consecutiveLossMaxSkip}).`
        );
      }
      const emergencyThreshold = p.supportedThreshold + 0.12;
      if (probability < emergencyThreshold) {
        return this.skip(
          `${cl} consecutive losses — max skip exhausted, prob ${(probability * 100).toFixed(1)}% too weak.`
        );
      }
    }

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

    // Regime-adaptive + mild loss-streak threshold escalation (only after reduceAt)
    threshold = this.regimeAdjustedThreshold(threshold, regime, ctx);

    // Soft daily pacing
    const used = riskState.dailyEntriesUsed ?? 0;
    const limit = riskState.dailyEntriesLimit ?? 500;
    if (limit > 0 && used / limit > 0.85) {
      threshold += 0.03;
    } else if (limit > 0 && used / limit < 0.25 && used < limit * 0.25) {
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

    // Stake reduction only (no hard blackout) when past reduceAt
    if (cl >= p.consecutiveLossReduceAt) {
      return {
        action: 'REDUCED_ENTRY',
        stake: this.reducedStake(riskState),
        reason: `${cl} consecutive sub-1.30 outcomes — reduced stake (no skip).`,
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
    // Quality: raise bar in hard regimes; never lower below fair via regime bonus.
    if (regime === 'deep-low' || regime === 'low-cluster') {
      t += 0.02; // streaks of sub-1.30 are hostile — demand more edge
    } else if (regime === 'volatile') {
      t += 0.03;
    } else if (regime === 'high-activity') {
      t += 0.005;
    }
    const lo = ctx.confidenceInterval?.[0];
    // Only ease threshold when lower CI is itself above fair.
    if (typeof lo === 'number' && lo >= FAIR_130) {
      t -= 0.01;
    }

    // Mild escalation only after reduceAt (not after every 2 sub-1.30 crashes)
    const cl = ctx.riskState?.consecutiveLosses ?? 0;
    if (cl >= this.policy.consecutiveLossReduceAt) {
      const extra =
        (cl - this.policy.consecutiveLossReduceAt + 1) *
        this.policy.lossStreakThresholdEscalation;
      t += Math.min(extra, 0.05);
    }

    // Floor at fair odds; ceiling allows selective high-confidence entries.
    // Prior clamp at 0.75 was BELOW fair (~0.769) and forced negative-EV ENTRY.
    return Math.max(FAIR_130, Math.min(0.92, t));
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
