/**
 * P0 calibration / feature honesty regression tests (diagnosis 2026-09).
 * Source-level + pure unit checks that do not require a live DB.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { IncrementalStateEngine } from "../state/incremental-state-engine.ts";
import { BaselineStatisticalModel, EMPIRICAL_BASE_1_30 } from "../models/baseline-model.ts";
import { RegimeDetector } from "../regimes/regime-detector.ts";
import type { HistoricalRound, FeatureVector } from "../types.ts";

function makeRound(cp: number, i: number): HistoricalRound {
  return {
    id: `r${i}`,
    externalRoundId: String(i),
    sessionId: null,
    startedAt: null,
    crashedAt: new Date(Date.now() - (200 - i) * 1000).toISOString(),
    crashPoint: cp,
    observationSource: "test",
    dataQuality: "high",
    createdAt: new Date().toISOString(),
  };
}

describe("window hit rates are exact (not SHORT_CAP aliases)", () => {
  it("windowHitRate(50) counts last 50 lag-ring points only", () => {
    const eng = new IncrementalStateEngine();
    // 40 losses (<1.3) then 60 hits (>=1.3)
    for (let i = 0; i < 40; i++) eng.update(1.1);
    for (let i = 0; i < 60; i++) eng.update(1.5);
    // last 50 are all hits
    expect(eng.windowHitRate(50, 1.3)).toBeCloseTo(1.0, 5);
    // last 100 = 40 losses + 60 hits
    expect(eng.windowHitRate(100, 1.3)).toBeCloseTo(0.6, 5);
    const w = eng.hitRateWindows13();
    expect(w.w50).toBeCloseTo(1.0, 5);
    expect(w.w100).toBeCloseTo(0.6, 5);
  });

  it("shortHitRate13 remains SHORT_CAP=30 diagnostic only", () => {
    const eng = new IncrementalStateEngine();
    for (let i = 0; i < 100; i++) eng.update(i % 2 === 0 ? 1.5 : 1.1);
    // short ring is 30 points alternating → ~0.5
    expect(eng.shortHitRate13()).toBeGreaterThan(0.4);
    expect(eng.shortHitRate13()).toBeLessThan(0.6);
  });
});

describe("baseline does not invent 95% probability under poor calibration", () => {
  it("caps probability and confidence when calibration is poor", () => {
    const model = new BaselineStatisticalModel();
    // Poison calibration with overconfident wrong predictions
    for (let i = 0; i < 40; i++) {
      model.observeOutcome!(0.95, 0);
    }
    const features: FeatureVector = {
      roundId: "t1",
      timestamp: new Date().toISOString(),
      featureVersion: "fv-test" as FeatureVector["featureVersion"],
      values: {
        hit_rate_50: 0.95,
        hit_rate_100: 0.95,
        hit_rate_200: 0.95,
        hit_1_30_50: 0.95,
        hit_1_30_100: 0.95,
        sample_size: 100,
        roll_std_50: 1,
      },
      meta: { sampleSize: 100, dataQualityScore: 1, missingFeatureCount: 0 },
    };
    const out = model.predict(features, 1.3, null);
    expect(out.probability).toBeLessThanOrEqual(0.88);
    expect(out.confidence).toBeLessThanOrEqual(0.5);
    expect(out.probability).toBeGreaterThan(0.05);
  });

  it("without heuristic boosts, gap/streak do not inflate probability above window blend", () => {
    process.env.ALLOW_HEURISTIC_BOOSTS = "0";
    const model = new BaselineStatisticalModel();
    model.importState({
      version: 1,
      gapMultiplier: 1.5,
      streakMultiplier: 1.5,
      anomalyMultiplier: 1.0,
      shortWeight: 0.3,
      midWeight: 0.4,
      longWeight: 0.3,
      outcomes: [],
      rollingAbsError: 0.15,
      allowHeuristicBoosts: false,
      updatedAt: new Date().toISOString(),
    });
    const features: FeatureVector = {
      roundId: "t2",
      timestamp: new Date().toISOString(),
      featureVersion: "fv-test" as FeatureVector["featureVersion"],
      values: {
        hit_rate_50: 0.75,
        hit_rate_100: 0.75,
        hit_rate_200: 0.75,
        hit_1_30_50: 0.75,
        hit_1_30_100: 0.75,
        since_1_30: 10,
        consec_below_1_30: 8,
        sample_size: 120,
        roll_std_50: 1,
      },
      meta: { sampleSize: 120, dataQualityScore: 1, missingFeatureCount: 0 },
    };
    const out = model.predict(features, 1.3, null);
    // Should stay near 0.75 / empirical base, not 0.75*1.5*1.5
    expect(out.probability).toBeLessThan(0.85);
    expect(out.featureSummary.heuristic_boosts).toBe(0);
  });

  it("export/import state is stable across restart simulation", () => {
    const a = new BaselineStatisticalModel();
    for (let i = 0; i < 15; i++) a.observeOutcome!(0.7, i % 3 === 0 ? 0 : 1);
    const snap = a.exportState();
    const b = new BaselineStatisticalModel();
    b.importState(snap);
    expect(b.getAdaptiveState().outcomeCount).toBe(a.getAdaptiveState().outcomeCount);
    expect(b.getAdaptiveState().rollingAbsError).toBeCloseTo(
      a.getAdaptiveState().rollingAbsError,
      5,
    );
  });
});

describe("regime id is deterministic classification", () => {
  it("same window yields same regime.id (not random UUID each time)", () => {
    const det = new RegimeDetector();
    const rounds = Array.from({ length: 40 }, (_, i) => makeRound(1.2, i));
    const a = det.detect(rounds);
    const b = det.detect(rounds);
    expect(a.id).toBe(b.id);
    expect(a.name).toBe(b.name);
    expect(a.id).toBe(a.name);
    // instanceId may differ
    expect(a.instanceId).toBeTruthy();
    expect(b.instanceId).toBeTruthy();
  });

  it("neutral vs deep-low produce different deterministic ids", () => {
    const det = new RegimeDetector();
    const neutral = Array.from({ length: 50 }, (_, i) =>
      makeRound(i % 3 === 0 ? 2.0 : 1.4, i),
    );
    const deepLow = Array.from({ length: 50 }, (_, i) => makeRound(1.1, i));
    const n = det.detect(neutral);
    const d = det.detect(deepLow);
    expect(n.id).not.toBe(d.id);
    expect(["neutral", "low-concentration", "high-volatility", "high-activity"]).toContain(
      n.name,
    );
    expect(["deep-low", "low-concentration", "anomalous"]).toContain(d.name);
  });
});

describe("empirical base constant", () => {
  it("matches 1/1.3", () => {
    expect(EMPIRICAL_BASE_1_30).toBeCloseTo(0.76923, 4);
  });
});
