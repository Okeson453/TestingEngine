/**
 * Model degradation detection + SAFE_BASELINE fallback (P1).
 *
 * Tracks rolling Brier / ECE vs a constant empirical base-rate predictor.
 * When the active model underperforms the constant baseline for a sustained
 * window, switches mode to SAFE_BASELINE (emit empirical base rate only).
 *
 * Mode is process-local + serializable to worker_state for restart continuity.
 */

import {
  brierScore,
  expectedCalibrationError,
  emptyBins,
  updateBin,
  type ReliabilityBin,
} from '../calibration/calibration-metrics.ts';
import { EMPIRICAL_BASE_1_30 } from '../models/baseline-model.ts';
import { getLogger } from '../../observability/logger.ts';

const logger = getLogger('safe-baseline');

function publishMode(ctrl: SafeBaselineController): void {
  const g = globalThis as {
    __safeBaselineMode__?: boolean;
    __safeBaselineProb__?: number;
    __safeBaselineSnap__?: { modelBrier: number; ece: number; n: number };
  };
  const safe = ctrl.isSafe();
  g.__safeBaselineMode__ = safe;
  g.__safeBaselineProb__ = ctrl.safeProbability(1.3);
  const s = ctrl.snapshot();
  g.__safeBaselineSnap__ = { modelBrier: s.modelBrier, ece: s.ece, n: s.n };
}


export type ModelMode = 'MODEL_ACTIVE' | 'SAFE_BASELINE';

export interface SafeBaselineConfig {
  /** Min samples before degradation can trigger. */
  minSamples: number;
  /** Rolling window size for Brier comparison. */
  window: number;
  /**
   * Trigger SAFE if modelBrier > constantBrier + margin
   * for `breachCount` consecutive evaluations.
   */
  brierMargin: number;
  /** ECE threshold above which we treat calibration as failed. */
  eceThreshold: number;
  /** Consecutive evaluation breaches required to enter SAFE. */
  breachCountToSafe: number;
  /** Consecutive healthy evaluations required to leave SAFE. */
  healthyCountToActive: number;
  /** Evaluate every N observations. */
  evalEvery: number;
}

const DEFAULT_CONFIG: SafeBaselineConfig = {
  minSamples: 50,
  window: 100,
  brierMargin: 0.01,
  eceThreshold: 0.12,
  breachCountToSafe: 2,
  healthyCountToActive: 3,
  evalEvery: 25,
};

interface Sample {
  predicted: number;
  actual: 0 | 1;
}

export interface SafeBaselineSnapshot {
  mode: ModelMode;
  modelBrier: number;
  constantBrier: number;
  ece: number;
  n: number;
  breachStreak: number;
  healthyStreak: number;
  updatedAt: string;
}

export class SafeBaselineController {
  private mode: ModelMode = 'MODEL_ACTIVE';
  private samples: Sample[] = [];
  private bins: ReliabilityBin[] = emptyBins(10);
  private sinceEval = 0;
  private breachStreak = 0;
  private healthyStreak = 0;
  private lastModelBrier = 0;
  private lastConstantBrier = 0;
  private lastEce = 0;
  private readonly cfg: SafeBaselineConfig;

  constructor(cfg: Partial<SafeBaselineConfig> = {}) {
    this.cfg = { ...DEFAULT_CONFIG, ...cfg };
    if (process.env.FORCE_SAFE_BASELINE === '1') {
      this.mode = 'SAFE_BASELINE';
    }
    publishMode(this);
  }

  getMode(): ModelMode {
    if (process.env.FORCE_SAFE_BASELINE === '1') return 'SAFE_BASELINE';
    if (process.env.FORCE_MODEL_ACTIVE === '1') return 'MODEL_ACTIVE';
    return this.mode;
  }

  isSafe(): boolean {
    return this.getMode() === 'SAFE_BASELINE';
  }

  /** Safe probability for 1.3x target. */
  safeProbability(target = 1.3): number {
    if (target <= 1.3) return EMPIRICAL_BASE_1_30;
    // Approximate fair odds for other thresholds
    return Math.min(0.95, Math.max(0.05, 1 / target));
  }

  observe(predicted: number, actual: 0 | 1): SafeBaselineSnapshot {
    const p = Math.min(0.999, Math.max(0.001, predicted));
    this.samples.push({ predicted: p, actual });
    if (this.samples.length > this.cfg.window) {
      this.samples.shift();
    }
    updateBin(this.bins, p, actual);
    this.sinceEval += 1;

    if (
      this.samples.length >= this.cfg.minSamples &&
      this.sinceEval >= this.cfg.evalEvery
    ) {
      this.sinceEval = 0;
      this.evaluate();
    }
    publishMode(this);
    return this.snapshot();
  }

  private evaluate(): void {
    const n = this.samples.length;
    if (n < this.cfg.minSamples) return;

    let modelBrierSum = 0;
    let constBrierSum = 0;
    for (const s of this.samples) {
      modelBrierSum += brierScore(s.predicted, s.actual);
      constBrierSum += brierScore(EMPIRICAL_BASE_1_30, s.actual);
    }
    const modelBrier = modelBrierSum / n;
    const constantBrier = constBrierSum / n;
    const ece = expectedCalibrationError(this.bins);

    this.lastModelBrier = modelBrier;
    this.lastConstantBrier = constantBrier;
    this.lastEce = ece;

    const degraded =
      modelBrier > constantBrier + this.cfg.brierMargin || ece > this.cfg.eceThreshold;

    if (degraded) {
      this.breachStreak += 1;
      this.healthyStreak = 0;
    } else {
      this.healthyStreak += 1;
      this.breachStreak = 0;
    }

    const prev = this.mode;
    if (
      this.mode === 'MODEL_ACTIVE' &&
      this.breachStreak >= this.cfg.breachCountToSafe
    ) {
      this.mode = 'SAFE_BASELINE';
      logger.warn(
        {
          component: 'safe-baseline',
          modelBrier,
          constantBrier,
          ece,
          n,
          breachStreak: this.breachStreak,
        },
        'MODEL_ACTIVE → SAFE_BASELINE (degradation detected)',
      );
    } else if (
      this.mode === 'SAFE_BASELINE' &&
      this.healthyStreak >= this.cfg.healthyCountToActive &&
      process.env.FORCE_SAFE_BASELINE !== '1'
    ) {
      this.mode = 'MODEL_ACTIVE';
      logger.info(
        {
          component: 'safe-baseline',
          modelBrier,
          constantBrier,
          ece,
          n,
          healthyStreak: this.healthyStreak,
        },
        'SAFE_BASELINE → MODEL_ACTIVE (performance recovered)',
      );
    } else if (prev !== this.mode) {
      /* already logged */
    }
    publishMode(this);
  }

  snapshot(): SafeBaselineSnapshot {
    return {
      mode: this.getMode(),
      modelBrier: this.lastModelBrier,
      constantBrier: this.lastConstantBrier,
      ece: this.lastEce,
      n: this.samples.length,
      breachStreak: this.breachStreak,
      healthyStreak: this.healthyStreak,
      updatedAt: new Date().toISOString(),
    };
  }

  exportState(): SafeBaselineSnapshot & { samples: Sample[] } {
    return { ...this.snapshot(), samples: this.samples.slice() };
  }

  importState(state: Partial<SafeBaselineSnapshot & { samples?: Sample[] }> | null): void {
    if (!state) return;
    if (state.mode === 'SAFE_BASELINE' || state.mode === 'MODEL_ACTIVE') {
      this.mode = state.mode;
    }
    if (Array.isArray(state.samples)) {
      this.samples = state.samples.slice(-this.cfg.window);
      this.bins = emptyBins(10);
      for (const s of this.samples) updateBin(this.bins, s.predicted, s.actual);
    }
    if (Number.isFinite(state.breachStreak)) this.breachStreak = state.breachStreak as number;
    if (Number.isFinite(state.healthyStreak)) this.healthyStreak = state.healthyStreak as number;
    if (Number.isFinite(state.modelBrier)) this.lastModelBrier = state.modelBrier as number;
    if (Number.isFinite(state.constantBrier)) this.lastConstantBrier = state.constantBrier as number;
    if (Number.isFinite(state.ece)) this.lastEce = state.ece as number;
    publishMode(this);
  }
}

export const globalSafeBaseline = new SafeBaselineController();
