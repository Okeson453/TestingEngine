import { describe, it, expect, beforeEach } from "vitest";
import {
  SafeBaselineController,
  liveSafeModeOverride,
} from "./safe-baseline-controller.ts";
import { EMPIRICAL_BASE_1_30 } from "../models/baseline-model.ts";

describe("SafeBaselineController", () => {
  beforeEach(() => {
    delete process.env.FORCE_SAFE_BASELINE;
    delete process.env.FORCE_MODEL_ACTIVE;
    const g = globalThis as { __safeBaselineMode__?: boolean };
    g.__safeBaselineMode__ = false;
  });

  it("starts MODEL_ACTIVE and emits empirical safe probability", () => {
    const c = new SafeBaselineController({ minSamples: 10, window: 20, evalEvery: 5, breachCountToSafe: 1 });
    expect(c.getMode()).toBe("MODEL_ACTIVE");
    expect(c.safeProbability(1.3)).toBeCloseTo(EMPIRICAL_BASE_1_30, 5);
  });

  it("enters SAFE_BASELINE when model Brier is worse than constant", () => {
    const c = new SafeBaselineController({
      minSamples: 20,
      window: 40,
      evalEvery: 10,
      breachCountToSafe: 1,
      brierMargin: 0.001,
      eceThreshold: 0.5,
    });
    // Overconfident predictions that are always wrong → high Brier
    for (let i = 0; i < 40; i++) {
      c.observe(0.95, 0);
    }
    expect(c.getMode()).toBe("SAFE_BASELINE");
    expect(c.isSafe()).toBe(true);
    const g = globalThis as { __safeBaselineMode__?: boolean };
    expect(g.__safeBaselineMode__).toBe(true);
  });

  it("export/import preserves mode", () => {
    const c = new SafeBaselineController({ minSamples: 10, window: 20, evalEvery: 5, breachCountToSafe: 1 });
    for (let i = 0; i < 30; i++) c.observe(0.9, 0);
    const snap = c.exportState();
    const c2 = new SafeBaselineController();
    c2.importState(snap);
    expect(c2.getMode()).toBe(c.getMode());
  });
});

describe("liveSafeModeOverride (pass 17 — live-path gating)", () => {
  const G = globalThis as {
    __safeBaselineMode__?: boolean;
    __safeBaselineProb__?: number;
  };

  it("returns null when MODEL_ACTIVE (no override on the live path)", () => {
    G.__safeBaselineMode__ = false;
    G.__safeBaselineProb__ = 0.769;
    expect(liveSafeModeOverride(0.85, 0.9)).toBeNull();
  });

  it("returns null when the global is unset (fail-open to MODEL_ACTIVE)", () => {
    delete G.__safeBaselineMode__;
    expect(liveSafeModeOverride(0.85, 0.9)).toBeNull();
  });

  it("overrides probability to the safe base rate and caps confidence", () => {
    G.__safeBaselineMode__ = true;
    G.__safeBaselineProb__ = EMPIRICAL_BASE_1_30;
    const out = liveSafeModeOverride(0.87, 0.96);
    expect(out).not.toBeNull();
    expect(out!.probability).toBeCloseTo(EMPIRICAL_BASE_1_30, 10);
    expect(out!.confidence).toBe(0.55);
  });

  it("keeps a lower confidence below the cap", () => {
    G.__safeBaselineMode__ = true;
    G.__safeBaselineProb__ = EMPIRICAL_BASE_1_30;
    const out = liveSafeModeOverride(0.87, 0.3);
    expect(out!.confidence).toBe(0.3);
  });

  it("falls back to the empirical base rate when the global prob is missing", () => {
    G.__safeBaselineMode__ = true;
    delete G.__safeBaselineProb__;
    const out = liveSafeModeOverride(0.87, 0.96);
    expect(out!.probability).toBeCloseTo(EMPIRICAL_BASE_1_30, 10);
  });
});
