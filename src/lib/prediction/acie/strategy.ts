/**
 * Strategy Layer — converts ACIE decision state into ENTRY / SKIP / REDUCED_ENTRY.
 *
 * High-frequency (HF) adaptive mode targets up to ~500 quality entries/day
 * without hard-blocking on mild evidence degradation.
 *
 * IMPORTANT — consecutiveLosses semantics:
 *   ACIEEngine increments consecutiveLosses on every crash < 1.30× in the
 *   live stream. Max allowed sub-1.30 streak before selective SKIP is **2**
 *   (no waiting until 3/4/5). After 2 consecutive lows, ENTRY is blocked
 *   until the streak breaks (a ≥1.30 outcome resets the counter).
 */

import { fractionalKellyStake } from '../stake/kelly-sizer.ts';
import type {
  StrategyDecision,
  StrategyDecisionContext,
  StrategyPolicy,
  StrategyPolicyMode,
} from './types.ts';

/**
 * Live qualification floor aligns with MIN_SIGNAL_PROBABILITY (default 0.65).
 * 1/1.30 ≈ 0.7692 remains the mathematical break-even for EV analysis only —
 * it is NOT the live ENTRY floor when the product gate is absolute 65%.
 *
 * Prod was labeling 65–76% as strategy_veto because thresholds sat at
 * fair+edge (~0.80). Qualified predictions must start at 65%+.
 * Raise ACIE_QUALITY_EDGE / ACIE_STRONG_EDGE to re-tighten selectivity.
 */
const FAIR_130 = 1 / 1.3;
const ABS_FLOOR = Number(process.env.MIN_SIGNAL_PROBABILITY ?? 0.65);
const QUALITY_EDGE = Number(process.env.ACIE_QUALITY_EDGE ?? 0);
const STRONG_EDGE = Number(process.env.ACIE_STRONG_EDGE ?? 0.02);

/** Default policy — absolute 65% floor (matches MIN_SIGNAL_PROBABILITY). */
export const DEFAULT_STRATEGY_POLICY: StrategyPolicy = {
  mode: 'adaptive',
  supportedThreshold: Math.max(ABS_FLOOR, ABS_FLOOR + QUALITY_EDGE),
  weakThreshold: Math.max(ABS_FLOOR, ABS_FLOOR + STRONG_EDGE),
  fallbackThreshold: ABS_FLOOR,
  maxCalibrationError: 0.12,
  highUncertainty: 0.18,
  // streak_below==2 in normal regime has WR≈80% — do not skip at 2
  consecutiveLossReduceAt: 3,
  reducedStakeFactor: 0.45,
  defaultStake: 700,
  /** Skip at 3 consecutive losses (was 2). */
  consecutiveLossSkipAt: 3,
  /** Keep skipping for the duration of the ongoing streak (capped). */
  consecutiveLossMaxSkip: 8,
  lossStreakThresholdEscalation: 0.02,
};

/**
 * High-frequency policy — same 65% floor, slightly looser secondary gates.
 */
export const HIGH_FREQUENCY_STRATEGY_POLICY: StrategyPolicy = {
  mode: 'adaptive',
  supportedThreshold: ABS_FLOOR,
  weakThreshold: Math.max(ABS_FLOOR, ABS_FLOOR + QUALITY_EDGE),
  fallbackThreshold: ABS_FLOOR,
  maxCalibrationError: 0.14,
  highUncertainty: 0.22,
  consecutiveLossReduceAt: 3,
  reducedStakeFactor: 0.55,
  defaultStake: 700,
  consecutiveLossSkipAt: 3,
  consecutiveLossMaxSkip: 6,
  lossStreakThresholdEscalation: 0.015,
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
  /** Pass-18 hotfix: this field was REFERENCED (this.selectiveOnly) but never
   *  declared — undefined made the HF-only threshold discount apply in quality
   *  mode too, silently lowering the bar the quality gates raised. Quality
   *  (selective) policy = anything that is not the explicit HF policy. */
  private readonly selectiveOnly: boolean;

  constructor(policy: StrategyPolicy = resolveDefaultPolicy()) {
    this.policy = policy;
    this.selectiveOnly = policy !== HIGH_FREQUENCY_STRATEGY_POLICY;
  }

  evaluate(ctx: StrategyDecisionContext): StrategyDecision {
    const { probability, evidence, calibrationError, uncertainty, riskState, regime } = ctx;
    const p = this.policy;
    const cl = riskState?.consecutiveLosses ?? 0;

    // Max losing streak gate (default skip-at 3).
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

    // Hostile regimes: empirical WR below fair with high variance
    if (regime === 'low-cluster' || regime === 'deep-low') {
      return this.skip(
        `Regime=${regime}: empirical WR below fair with high variance; require normal/high-activity.`,
      );
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

    // Daily volume: hard stop at limit (product max 1500/day), soft pacing before.
    const used = riskState.dailyEntriesUsed ?? 0;
    const limit = riskState.dailyEntriesLimit ?? 1500;
    if (limit > 0 && used >= limit) {
      return this.skip(
        `Daily signal volume limit reached: ${used}/${limit}.`,
      );
    }
    if (limit > 0 && used / limit > 0.85) {
      threshold += 0.03;
    } else if (
      !this.selectiveOnly &&
      limit > 0 &&
      used / limit < 0.25 &&
      used < limit * 0.25
    ) {
      // HF only: never lower the bar in selective quality mode.
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
      if (this.selectiveOnly) {
        return this.skip(
          `${cl} consecutive sub-1.30 outcomes — selective mode requires full ENTRY only.`,
        );
      }
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

    // Floor at absolute qualification gate (65%); ceiling allows selective
    // high-confidence entries. Break-even (FAIR_130) is analysis-only.
    return Math.max(ABS_FLOOR, Math.min(0.92, t));
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
