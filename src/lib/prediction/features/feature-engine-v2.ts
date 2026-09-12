/**
 * Feature Engine v2 — assembles incremental + feature families.
 * Critical path uses IncrementalStateEngine only (no full history scan).
 *
 * Each family is isolated so a throw surfaces featureStage=<name> for N+1 diagnosis.
 */

import { createHash } from 'crypto';
import type { FeatureVector, HistoricalRound } from '../types.ts';
import {
  IncrementalStateEngine,
  globalIncrementalState,
} from '../state/incremental-state-engine.ts';
import { FEATURE_VERSION_V2, CURRENT_FEATURE_VERSION } from './feature-meta.ts';
import { computeLagFeatures } from './lag-features.ts';
import { computeRunFeatures } from './run-features.ts';
import { computeMarkovFeatures } from './markov-features.ts';
import { computeSpectralFeatures } from './spectral-features.ts';
import { computeEntropyFeatures } from './entropy-features.ts';
import { computeTimeFeatures } from './time-features.ts';
import { computeCrossTargetFeatures } from './cross-target-features.ts';
import { computeGapFeatures } from './gap-features.ts';
import { computeFeatures as computeLegacy } from './calculators.ts';
import { getLogger } from '../../observability/logger.ts';

const logger = getLogger('FeatureEngineV2');

function familyError(featureStage: string, err: unknown): Error {
  const base = err instanceof Error ? err : new Error(String(err));
  const e = new Error(`[featureStage=${featureStage}] ${base.message}`);
  e.name = base.name || 'FeatureFamilyError';
  (e as Error & { featureStage?: string; cause?: unknown }).featureStage = featureStage;
  (e as Error & { featureStage?: string; cause?: unknown }).cause = base;
  return e;
}

function runFamily<T extends Record<string, number>>(
  featureStage: string,
  fn: () => T,
): T {
  try {
    return fn();
  } catch (err) {
    logger.error(
      {
        component: 'FeatureEngineV2',
        featureStage,
        errorName: err instanceof Error ? err.name : 'Error',
        errorMessage: err instanceof Error ? err.message : String(err),
        errorStack: err instanceof Error ? err.stack?.slice(0, 1200) : null,
      },
      `Feature family failed: ${featureStage}`,
    );
    throw familyError(featureStage, err);
  }
}

export class FeatureEngineV2 {
  readonly featureVersion = FEATURE_VERSION_V2;

  private readonly engine: IncrementalStateEngine;
  constructor(engine: IncrementalStateEngine = globalIncrementalState) {
    this.engine = engine;
  }

  /** O(1) snapshot from incremental state (critical path). */
  snapshotFromState(
    targetRoundId: string,
    timestamp: string = new Date().toISOString()
  ): FeatureVector {
    const values: Record<string, number> = {
      ...runFamily('base', () => this.baseFromEngine()),
      ...runFamily('lag', () => computeLagFeatures(this.engine)),
      ...runFamily('run', () => computeRunFeatures(this.engine)),
      ...runFamily('markov', () => computeMarkovFeatures(this.engine)),
      ...runFamily('spectral', () => computeSpectralFeatures(this.engine)),
      ...runFamily('entropy', () => computeEntropyFeatures(this.engine)),
      ...runFamily('time', () => computeTimeFeatures(new Date(timestamp))),
      ...runFamily('cross_target', () => computeCrossTargetFeatures(this.engine)),
      // FINAL_REPORT-2 #1: gap family — only Bonferroni-surviving feature.
      // Regime-dependent (dead since Sept 10); consumed by the gap-conditional
      // candidate model when the regime detector says the signal is active.
      ...runFamily('gap', () => computeGapFeatures(this.engine)),
    };
    for (const k of Object.keys(values)) {
      if (!Number.isFinite(values[k])) values[k] = 0;
    }
    const snap = this.engine.snapshot();
    return {
      roundId: targetRoundId,
      timestamp,
      featureVersion: this.featureVersion,
      values,
      meta: {
        sampleSize: snap.count,
        dataQualityScore: Math.min(1, snap.count / 100),
        missingFeatureCount: 0,
      },
    };
  }

  /** Offline / validation path: rebuild engine from prior rounds then snapshot. */
  buildVector(
    priorRounds: HistoricalRound[],
    targetRoundId: string,
    timestamp: string
  ): FeatureVector {
    const local = new IncrementalStateEngine();
    local.seed(priorRounds.map((r) => r.crashPoint));
    // FINAL_REPORT-2 #1: gap family needs round-start times. HistoricalRound
    // already carries startedAt (crash_rounds.began_at); feed it in round
    // order so the realized-gap chain rebuilds identically to live.
    for (const r of priorRounds) {
      if (r.startedAt) {
        const ms = new Date(r.startedAt).getTime();
        if (Number.isFinite(ms)) local.recordBeganAt(ms);
      }
    }
    const tmp = new FeatureEngineV2(local);
    return tmp.snapshotFromState(targetRoundId, timestamp);
  }

  /** Legacy full recompute for backtests that need identical fv-1 keys */
  buildLegacyVector(
    priorRounds: HistoricalRound[],
    targetRoundId: string,
    timestamp: string
  ): FeatureVector {
    const values = computeLegacy(priorRounds, timestamp);
    for (const k of Object.keys(values)) {
      if (!Number.isFinite(values[k])) values[k] = 0;
    }
    return {
      roundId: targetRoundId,
      timestamp,
      featureVersion: CURRENT_FEATURE_VERSION,
      values,
      meta: {
        sampleSize: priorRounds.length,
        dataQualityScore: values.quality_score ?? 0,
        missingFeatureCount: 0,
      },
    };
  }

  featureHash(values: Record<string, number>): string {
    const keys = Object.keys(values).sort();
    const payload = keys.map((k) => `${k}=${values[k]}`).join('|');
    return createHash('sha256').update(payload).digest('hex').slice(0, 16);
  }

  private baseFromEngine(): Record<string, number> {
    const e = this.engine;
    const s = e.snapshot();
    const variance = e.variance();
    // Empirical ≥1.3x rates from lag-ring windows (exact semantics).
    // SHORT_CAP=30 ring remains diagnostic only (short_hit_13) — never aliased as 50/100.
    const short13 = e.shortHitRate13();
    const ewma13 = s.ewmaHit13;
    const w = e.hitRateWindows13();
    const hit10 = e.hitRate(10.0);
    // Genuine independent windows for baseline blend (P0 calibration fix).
    const hit50 = w.w50;
    const hit100 = w.w100 > 0 ? w.w100 : hit50;
    return {
      n: s.count,
      sample_size: s.count,
      mean_cp: s.welford.mean,
      var_cp: variance,
      std_cp: Math.sqrt(variance),
      last_cp: s.lastCrash ?? 0,
      ewma_cp: s.ewma,
      ewma_hit_13: ewma13,
      short_mean: e.shortMean(),
      short_var: e.shortVariance(),
      short_hit_13: short13,
      // Exact window rates (do not mislabel short-30 as 50/100)
      hit_rate_20: w.w20,
      hit_rate_50: w.w50,
      hit_rate_100: w.w100,
      hit_rate_200: w.w200,
      quality_score: Math.min(1, s.count / 100),
      // fv-1 keys: now true 50/100-round rates (not SHORT_CAP aliases)
      hit_1_30_50: hit50,
      hit_1_30_100: hit100,
      hit_2_00_50: e.windowHitRate(50, 2.0),
      hit_2_00_100: e.windowHitRate(100, 2.0),
      hit_5_00_50: e.windowHitRate(50, 5.0),
      hit_5_00_100: e.windowHitRate(100, 5.0),
      hit_10_00_50: e.windowHitRate(50, 10.0),
      hit_10_00_100: e.windowHitRate(100, 10.0),
      // Rounds-since-last-hit — now tracked incrementally (was hard-coded 0).
      since_1_30: s.since.t13,
      since_2_00: s.since.t20,
      since_5_00: s.since.t50,
      since_10_00: s.since.t100,
      // Consecutive-below streaks — now mapped from runs (were missing entirely).
      consec_below_1_30: s.runs.below13,
      consec_below_2_00: s.runs.below20,
      consec_above_2_00: s.runs.above20,
      roll_std_50: Math.sqrt(e.shortVariance() || variance || 0),
    };
  }
}

export const globalFeatureEngineV2 = new FeatureEngineV2();
