/**
 * Gap feature family (FINAL_REPORT-2, items #1/#9).
 *
 * The ONLY feature that survived Bonferroni correction (81 features, α=0.01)
 * in the Sept 8-11 backtest: gap_s — seconds between consecutive round-start
 * timestamps — r=0.088, p=7.2e-12. The signal is REGIME-DEPENDENT and was
 * dead after Sept 10 (per-day Spearman: +0.557 Sept 9 → −0.001 Sept 11);
 * it is instrumented here so the regime detector (#4) can see it the moment
 * it reactivates, not after another 12k-round manual backtest.
 *
 * Mechanism per report §3.1: gap = round_duration(prev) + betting_window.
 * round_duration(prev) is observed directly at crash time, so the gap's
 * residual information is the betting-window component, conditional on m_lag1.
 *
 * Feature semantics: gap_s = beganAt(N) − beganAt(N−1), the most recent
 * REALIZED gap, tracked by IncrementalStateEngine.recordBeganAt(). 0 until
 * beganAt flows (missingValuePolicy zero) — the feature can read 0 both for
 * "no data" and a genuine sub-second gap; gap_count in the snapshot
 * disambiguates for the regime detector.
 */

import type { IncrementalStateEngine } from '../state/incremental-state-engine.ts';
import type { FeatureMeta } from './feature-meta.ts';

export const GAP_FEATURE_META: FeatureMeta[] = [
  {
    featureName: 'gap_s',
    featureVersion: '1',
    source: 'began-at-delta',
    updateCost: 'O(1)',
    dependencies: [],
    validityWindow: 2,
    missingValuePolicy: 'zero',
  },
  {
    featureName: 'log_lag_1',
    featureVersion: '1',
    source: 'lag-ring',
    updateCost: 'O(1)',
    dependencies: ['lag_1'],
    validityWindow: 64,
    missingValuePolicy: 'zero',
  },
];

/** Compute the gap family. Never throws on missing data — returns zeros
 * (family contract: a throw here would surface featureStage=gap and kill
 * the whole vector for a feature that is currently regime-dead anyway). */
export function computeGapFeatures(engine: IncrementalStateEngine): Record<string, number> {
  try {
    const snap = engine.snapshot();
    const lag1 = snap.lastCrash ?? 0;
    return {
      gap_s: snap.lastGapS,
      // Crash multipliers are >= 1 by CHECK constraint, so log >= 0.
      // Clamp guards restored/synthetic states that can hold 0.
      log_lag_1: lag1 >= 1 ? Math.log(lag1) : 0,
    };
  } catch {
    return { gap_s: 0, log_lag_1: 0 };
  }
}
