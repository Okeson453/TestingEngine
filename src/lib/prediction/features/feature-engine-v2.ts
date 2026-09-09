/**
 * Feature Engine v2 — assembles incremental + feature families.
 * Critical path uses IncrementalStateEngine only (no full history scan).
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
import { computeFeatures as computeLegacy } from './calculators.ts';

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
      ...this.baseFromEngine(),
      ...computeLagFeatures(this.engine),
      ...computeRunFeatures(this.engine),
      ...computeMarkovFeatures(this.engine),
      ...computeSpectralFeatures(this.engine),
      ...computeEntropyFeatures(this.engine),
      ...computeTimeFeatures(new Date(timestamp)),
      ...computeCrossTargetFeatures(this.engine),
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
    // Empirical ≥1.3x rates from incremental state (true crash base-rate ≈ 1/1.3 ≈ 0.77).
    const short13 = e.shortHitRate13();
    const ewma13 = s.ewmaHit13;
    // Real hit rates for all targets, including 10x (no more approximations).
    const hit10 = e.hitRate(10.0);
    // BaselineModel still reads fv-1 keys (hit_1_30_50 etc.). Without these
    // aliases every live signal collapsed to the hard-coded default 0.30 /
    // ~0.596 confidence — identical on every row in Live Validation.
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
      quality_score: Math.min(1, s.count / 100),
      // fv-1 aliases for BaselineModel / registry default
      hit_1_30_50: short13,
      hit_1_30_100: ewma13 > 0 ? ewma13 : short13,
      hit_2_00_50: e.hitRate(2.0),
      hit_2_00_100: e.hitRate(2.0),
      hit_5_00_50: e.hitRate(5.0),
      hit_5_00_100: e.hitRate(5.0),
      hit_10_00_50: hit10,
      hit_10_00_100: hit10,
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
