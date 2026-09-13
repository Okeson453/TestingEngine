import { describe, it, expect, beforeEach } from "vitest";
import {
  resetLossCooldownForTests,
  noteValidatedPredictionOutcome,
  shouldForceLossCooldownSkip,
  consumeLossCooldownSkip,
  noteRoundCompletedForCooldown,
  getLossCooldownState,
} from "./prediction-loss-cooldown.ts";
import { shouldSkipReason } from "./predictor.ts";

describe("prediction-loss-cooldown", () => {
  beforeEach(() => {
    resetLossCooldownForTests();
  });

  it("ACTIVE: no forced skip", () => {
    expect(shouldForceLossCooldownSkip("100").skip).toBe(false);
  });

  it("LOSS on 100 → skip 101 → resume 102", () => {
    noteValidatedPredictionOutcome({
      predictionId: "p1",
      targetGameId: "100",
      result: "LOSS",
    });
    expect(getLossCooldownState().skipRemaining).toBe(1);
    expect(getLossCooldownState().skipTargetGameId).toBe("101");

    const s101 = shouldForceLossCooldownSkip("101");
    expect(s101.skip).toBe(true);

    // Even high probability is blocked at selectivity layer
    const gate = shouldSkipReason({
      probability: 0.9,
      confidence: 0.95,
      targetGameId: "101",
      minProbability: 0.65,
      minEdge: 0,
    });
    expect(gate.skip).toBe(true);
    expect(gate.reason).toBe("loss_cooldown");

    consumeLossCooldownSkip("101");
    expect(getLossCooldownState().skipRemaining).toBe(0);
    expect(shouldForceLossCooldownSkip("102").skip).toBe(false);
  });

  it("65% still eligible when not in cooldown", () => {
    const gate = shouldSkipReason({
      probability: 0.65,
      confidence: 0.9,
      targetGameId: "200",
      minProbability: 0.65,
      minEdge: 0,
    });
    expect(gate.skip).toBe(false);
  });

  it("below 65% rejected independent of cooldown", () => {
    const gate = shouldSkipReason({
      probability: 0.649,
      confidence: 0.9,
      targetGameId: "200",
      minProbability: 0.65,
      minEdge: 0,
    });
    expect(gate.skip).toBe(true);
    expect(gate.reason).toBe("probability_below_min");
  });

  it("duplicate LOSS validation does not double-arm cooldown", () => {
    noteValidatedPredictionOutcome({
      predictionId: "p2",
      targetGameId: "50",
      result: "LOSS",
    });
    noteValidatedPredictionOutcome({
      predictionId: "p2",
      targetGameId: "50",
      result: "LOSS",
    });
    expect(getLossCooldownState().skipRemaining).toBe(1);
    expect(getLossCooldownState().consecutivePredictionLosses).toBe(1);
  });

  it("WIN clears consecutive losses without removing scheduled skip", () => {
    noteValidatedPredictionOutcome({
      predictionId: "p3",
      targetGameId: "10",
      result: "LOSS",
    });
    // A different prediction wins (e.g. earlier signal) — streak clears but
    // the mandatory skip for 11 remains until consumed.
    noteValidatedPredictionOutcome({
      predictionId: "p4",
      targetGameId: "9",
      result: "WIN",
    });
    expect(getLossCooldownState().consecutivePredictionLosses).toBe(0);
    expect(shouldForceLossCooldownSkip("11").skip).toBe(true);
  });

  it("round completion clears skip if no prediction attempt ran", () => {
    noteValidatedPredictionOutcome({
      predictionId: "p5",
      targetGameId: "70",
      result: "LOSS",
    });
    expect(shouldForceLossCooldownSkip("71").skip).toBe(true);
    noteRoundCompletedForCooldown("71");
    expect(shouldForceLossCooldownSkip("71").skip).toBe(false);
    expect(getLossCooldownState().skipRemaining).toBe(0);
  });

  it("consecutive LOSSes each schedule one skip", () => {
    noteValidatedPredictionOutcome({
      predictionId: "a",
      targetGameId: "100",
      result: "LOSS",
    });
    consumeLossCooldownSkip("101");
    noteValidatedPredictionOutcome({
      predictionId: "b",
      targetGameId: "102",
      result: "LOSS",
    });
    expect(getLossCooldownState().consecutivePredictionLosses).toBe(2);
    expect(shouldForceLossCooldownSkip("103").skip).toBe(true);
    consumeLossCooldownSkip("103");
    expect(shouldForceLossCooldownSkip("104").skip).toBe(false);
  });
});
