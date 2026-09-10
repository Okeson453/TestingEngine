import { describe, it, expect, beforeEach } from "vitest";
import { SafeBaselineController } from "./safe-baseline-controller.ts";
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
