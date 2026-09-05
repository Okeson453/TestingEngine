/**
 * Closed-loop feedback unit tests (audit §54–§55).
 */
import { describe, it, expect, beforeEach } from "vitest";
import {
  analyzeFailure,
  processResolvedPredictionFeedback,
  resetFeedbackIdempotencyForTests,
} from "./feedback.ts";
import { globalIncrementalState } from "../state/incremental-state-engine.ts";
import { globalBaselineModel } from "../models/baseline-model.ts";

describe("analyzeFailure", () => {
  it("classifies high-prob LOSS as OVERCONFIDENT", () => {
    const a = analyzeFailure(0.85, 0, "LOSS");
    expect(a.classification).toBe("OVERCONFIDENT");
    expect(a.residual).toBeCloseTo(0.85, 5);
  });

  it("classifies WIN as WIN", () => {
    const a = analyzeFailure(0.6, 1, "WIN");
    expect(a.classification).toBe("WIN");
  });

  it("classifies low-prob WIN as UNDERCONFIDENT", () => {
    const a = analyzeFailure(0.2, 1, "WIN");
    expect(a.classification).toBe("UNDERCONFIDENT");
  });
});

describe("processResolvedPredictionFeedback", () => {
  beforeEach(() => {
    resetFeedbackIdempotencyForTests();
    globalIncrementalState.reset();
  });

  it("updates incremental state and is idempotent", async () => {
    const before = globalIncrementalState.snapshot().count;
    const r1 = await processResolvedPredictionFeedback({
      predictionId: "pred-1",
      targetGameId: "100",
      predictedProbability: 0.8,
      targetMultiplier: 1.3,
      actualMultiplier: 1.05,
      result: "LOSS",
      resolvedAt: new Date().toISOString(),
    });
    expect(r1.learningComponents.incremental).toBe(true);
    expect(r1.analysis.classification).toBe("OVERCONFIDENT");
    expect(globalIncrementalState.snapshot().count).toBe(before + 1);

    const mid = globalIncrementalState.snapshot().count;
    await processResolvedPredictionFeedback({
      predictionId: "pred-1",
      targetGameId: "100",
      predictedProbability: 0.8,
      targetMultiplier: 1.3,
      actualMultiplier: 1.05,
      result: "LOSS",
      resolvedAt: new Date().toISOString(),
    });
    // Idempotent: count must not increase again
    expect(globalIncrementalState.snapshot().count).toBe(mid);
  });

  it("adapts baseline after repeated overconfident losses", async () => {
    const before = globalBaselineModel.getAdaptiveState();
    // Prime gap-active via predict
    const features = {
      values: {
        hit_1_30_50: 0.4,
        hit_1_30_100: 0.4,
        since_1_30: 50,
        consec_below_1_30: 10,
        sample_size: 50,
        roll_std_50: 2,
      },
      meta: { dataQualityScore: 0.8, featureVersion: "test" },
    };
    const regime = {
      name: "neutral",
      dimensions: { anomalyState: false },
      confidence: 0.5,
      explanation: "",
      detectedAt: new Date().toISOString(),
    };
    for (let i = 0; i < 20; i++) {
      globalBaselineModel.predict(features as never, 1.3, regime as never);
      await processResolvedPredictionFeedback({
        predictionId: `pred-adapt-${i}`,
        targetGameId: String(200 + i),
        predictedProbability: 0.85,
        targetMultiplier: 1.3,
        actualMultiplier: 1.05,
        result: "LOSS",
        resolvedAt: new Date().toISOString(),
      });
    }
    const after = globalBaselineModel.getAdaptiveState();
    expect(after.outcomeCount).toBeGreaterThanOrEqual(20);
    // Gap multiplier should decrease after overconfident losses
    expect(after.gapMultiplier).toBeLessThanOrEqual(before.gapMultiplier);
  });
});
