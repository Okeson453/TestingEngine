/**
 * Gap feature family + gap-conditional candidate model tests
 * (FINAL_REPORT-2 items #1, #2, #3).
 */
import { describe, it, expect } from "vitest";
import { IncrementalStateEngine } from "../state/incremental-state-engine.ts";
import { computeGapFeatures, GAP_FEATURE_META } from "./gap-features.ts";
import { FeatureEngineV2 } from "./feature-engine-v2.ts";
import { FEATURE_VERSION_V2 } from "./feature-meta.ts";
import {
  GapConditionalModel,
  MODEL_A,
  MODEL_B,
  NEUTRAL_Z,
} from "../models/gap-conditional-model.ts";
import { ModelRegistry } from "../models/model-registry.ts";
import type { HistoricalRound, FeatureVector, ThresholdTarget } from "../types.ts";

const T: ThresholdTarget = 1.3;

function vec(engine: IncrementalStateEngine, roundId = "t"): FeatureVector {
  return new FeatureEngineV2(engine).snapshotFromState(roundId, new Date().toISOString());
}

function fvFromValues(values: Record<string, number>): FeatureVector {
  return {
    roundId: "t",
    timestamp: new Date().toISOString(),
    featureVersion: FEATURE_VERSION_V2,
    values,
    meta: { sampleSize: 120, dataQualityScore: 1, missingFeatureCount: 0 },
  };
}

describe("IncrementalStateEngine gap tracking", () => {
  it("records the realized gap between consecutive beganAt observations", () => {
    const eng = new IncrementalStateEngine();
    eng.update(2.0);
    eng.recordBeganAt(1_000_000);
    eng.update(1.8);
    eng.recordBeganAt(1_030_000); // +30s
    const g = eng.getGapState();
    expect(g.lastGapS).toBe(30);
    expect(g.gapCount).toBe(1);
    expect(g.lastBeganAtMs).toBe(1_030_000);
  });

  it("ignores non-monotonic and implausible (>600s) gaps", () => {
    const eng = new IncrementalStateEngine();
    eng.recordBeganAt(1_000_000);
    eng.recordBeganAt(900_000); // backwards (clock jump) — keeps chain, no gap
    eng.recordBeganAt(1_700_000); // +700s — beyond sanity window
    const g = eng.getGapState();
    expect(g.gapCount).toBe(0);
    expect(g.lastGapS).toBe(0);
    expect(g.lastBeganAtMs).toBe(1_700_000);
  });

  it("gap survives reset() clearing", () => {
    const eng = new IncrementalStateEngine();
    eng.recordBeganAt(1_000_000);
    eng.recordBeganAt(1_010_000);
    eng.reset();
    expect(eng.getGapState()).toEqual({ lastGapS: 0, gapCount: 0, lastBeganAtMs: null });
  });
});

describe("gap feature family", () => {
  it("declares gap_s and log_lag_1 metadata", () => {
    expect(GAP_FEATURE_META.map((m) => m.featureName).sort()).toEqual(["gap_s", "log_lag_1"]);
  });

  it("computes gap_s and log_lag_1 from engine state", () => {
    const eng = new IncrementalStateEngine();
    eng.update(1.2);
    eng.recordBeganAt(2_000_000);
    eng.update(4.0); // lag_1 = 4.0 once observed
    eng.recordBeganAt(2_025_000);
    const f = computeGapFeatures(eng);
    expect(f.gap_s).toBe(25);
    expect(f.log_lag_1).toBeCloseTo(Math.log(4.0), 10);
  });

  it("degrades to zero without beganAt data (missingValuePolicy zero)", () => {
    const eng = new IncrementalStateEngine();
    for (let i = 0; i < 5; i++) eng.update(1.5);
    const f = computeGapFeatures(eng);
    expect(f.gap_s).toBe(0);
    expect(f.log_lag_1).toBeCloseTo(Math.log(1.5), 10);
  });

  it("snapshotFromState includes the gap family keys", () => {
    const eng = new IncrementalStateEngine();
    eng.update(2.0);
    eng.recordBeganAt(Date.now() - 20_000);
    eng.update(2.5);
    eng.recordBeganAt(Date.now());
    const fv = vec(eng);
    expect(fv.values).toHaveProperty("gap_s");
    expect(fv.values).toHaveProperty("log_lag_1");
  });

  it("buildVector rebuilds the gap chain from HistoricalRound.startedAt", () => {
    const rounds: HistoricalRound[] = [];
    for (let i = 0; i < 5; i++) {
      rounds.push({
        id: `r${i}`,
        externalRoundId: String(i),
        sessionId: null,
        startedAt: new Date(1_000_000 + i * 30_000).toISOString(),
        crashedAt: new Date(1_000_000 + i * 30_000 + 10_000).toISOString(),
        crashPoint: 1.5 + i,
        observationSource: "test",
        dataQuality: "high",
        createdAt: new Date().toISOString(),
      });
    }
    const fv = new FeatureEngineV2().buildVector(rounds, "target", new Date().toISOString());
    expect(fv.values.gap_s).toBe(30);
    expect(fv.values.log_lag_1).toBeCloseTo(Math.log(5.5), 10);
  });
});

describe("GapConditionalModel", () => {
  it("is registered in the ModelRegistry", () => {
    const reg = new ModelRegistry();
    expect(reg.get("gap-conditional")).toBeDefined();
  });

  it("defaults to Model A (regime INACTIVE) and reproduces the report's next-round math", () => {
    const m = new GapConditionalModel();
    expect(m.isGapSignalActive()).toBe(false);
    // Report §6: log_m_lag1 = 0.191 → P = 0.7578
    const out = m.predict(fvFromValues({ gap_s: 0, log_lag_1: 0.191 }), T, null);
    expect(out.probability).toBeCloseTo(
      1 / (1 + Math.exp(-(MODEL_A.intercept + MODEL_A.logLag1 * 0.191))),
      10,
    );
    expect(out.probability).toBeGreaterThan(0.74);
    expect(out.probability).toBeLessThan(0.78);
  });

  it("switches to Model B only when the regime flag is active AND gap data exists", () => {
    const m = new GapConditionalModel();
    m.setStandardization({ ...NEUTRAL_Z, gapMeanS: 30, gapStdS: 10 });
    m.setGapSignalActive(true);
    // Short gap (z = -2): report says short gap ⇒ lower P(≥1.30) conditionally.
    const shortGap = m.predict(fvFromValues({ gap_s: 10, log_lag_1: 0.2 }), T, null);
    const longGap = m.predict(fvFromValues({ gap_s: 50, log_lag_1: 0.2 }), T, null);
    const zb = 1 / (1 + Math.exp(-(MODEL_B.intercept + MODEL_B.gapZ * -2 + MODEL_B.logLag1Z * 0.2)));
    expect(shortGap.probability).toBeCloseTo(zb, 10);
    expect(longGap.probability).toBeGreaterThan(shortGap.probability);
    expect(shortGap.reasoning[0]).toContain("MODEL_B");
  });

  it("falls back to Model A when the regime is active but gap_s is missing", () => {
    const m = new GapConditionalModel();
    m.setGapSignalActive(true);
    const out = m.predict(fvFromValues({ gap_s: 0, log_lag_1: 0.191 }), T, null);
    expect(out.reasoning[0]).toContain("MODEL_A fallback");
    expect(out.probability).toBeCloseTo(
      1 / (1 + Math.exp(-(MODEL_A.intercept + MODEL_A.logLag1 * 0.191))),
      10,
    );
  });

  it("refuses to score non-1.30 targets with these coefficients", () => {
    const m = new GapConditionalModel();
    const out = m.predict(fvFromValues({ gap_s: 20, log_lag_1: 0.2 }), 2.0, null);
    expect(out.reasoning.some((r) => r.includes("1.30-specific"))).toBe(true);
  });

  it("fit() derives z-params from a dataset scored in the fv-2.1 space", () => {
    const m = new GapConditionalModel();
    const rows = Array.from({ length: 200 }, (_, i) => ({
      features: fvFromValues({ gap_s: 10 + (i % 40), log_lag_1: Math.log(1.5) }),
      label: {
        roundId: `r${i}`,
        targetVersion: "tv-1.0.0",
        thresholds: { "1.3": (i % 3 === 0 ? 1 : 0) as 0 | 1 },
        crashPoint: 2.0,
        timestamp: new Date().toISOString(),
      },
    }));
    m.fit({ meta: {} as never, rows } as never);
    const z = m.getStandardization();
    expect(z.gapMeanS).toBeGreaterThan(0);
    expect(z.gapStdS).toBeGreaterThan(0);
  });
});
