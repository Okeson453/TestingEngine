/**
 * Regression tests for the live "NO BET THIS ROUND" selectivity path.
 *
 * Root cause fixed: MIN_SIGNAL_EDGE defaulted to 0, so the gate in
 * onGameEndPredict never fired and every ACIE evaluation was delivered
 * (and graded WIN/LOSS) as if a bet was placed.
 */
import { describe, expect, it } from "vitest";
import {
  MIN_SIGNAL_EDGE,
  MIN_SIGNAL_PROBABILITY,
  MIN_SIGNAL_CONFIDENCE,
  shouldSkipSignal,
} from "./predictor";

describe("selectivity gate (shouldSkipSignal)", () => {
  it("defaults MIN_SIGNAL_EDGE to a positive edge so weak rounds can be skipped", () => {
    // Production must not ship with all gates at 0 (that is the old bug).
    expect(MIN_SIGNAL_EDGE).toBeGreaterThan(0);
    expect(MIN_SIGNAL_EDGE).toBeCloseTo(0.015, 5);
    expect(MIN_SIGNAL_PROBABILITY).toBe(0);
    expect(MIN_SIGNAL_CONFIDENCE).toBe(0);
  });

  it("skips when probability is below fair+edge for 1.3x (~76.9% + 1.5%)", () => {
    // fair(1.3) ≈ 0.76923; needP ≈ 0.78423
    expect(
      shouldSkipSignal({ probability: 0.70, confidence: 0.9, target: 1.3 }),
    ).toBe(true);
    expect(
      shouldSkipSignal({ probability: 0.78, confidence: 0.9, target: 1.3 }),
    ).toBe(true);
    // Clearly above edge
    expect(
      shouldSkipSignal({ probability: 0.85, confidence: 0.9, target: 1.3 }),
    ).toBe(false);
  });

  it("skips on strategy_action=SKIP even when probability is high", () => {
    expect(
      shouldSkipSignal({
        probability: 0.95,
        confidence: 0.95,
        target: 1.3,
        strategyAction: "SKIP",
      }),
    ).toBe(true);
  });

  it("skips on pipeline_action=SKIP even when probability is high", () => {
    expect(
      shouldSkipSignal({
        probability: 0.95,
        confidence: 0.95,
        target: 1.3,
        pipelineAction: "SKIP",
      }),
    ).toBe(true);
  });

  it("skips when reasoning contains action=SKIP (legacy ACIE marker)", () => {
    expect(
      shouldSkipSignal({
        probability: 0.95,
        confidence: 0.95,
        target: 1.3,
        reasoning: ["acie", "action=SKIP", "evidence=WEAK"],
      }),
    ).toBe(true);
  });

  it("does not skip ENTRY / REDUCED_ENTRY with strong probability", () => {
    expect(
      shouldSkipSignal({
        probability: 0.88,
        confidence: 0.8,
        target: 1.3,
        strategyAction: "ENTRY",
      }),
    ).toBe(false);
    expect(
      shouldSkipSignal({
        probability: 0.88,
        confidence: 0.8,
        target: 1.3,
        strategyAction: "REDUCED_ENTRY",
      }),
    ).toBe(false);
  });

  it("honors explicit minProbability / minConfidence overrides", () => {
    expect(
      shouldSkipSignal({
        probability: 0.60,
        confidence: 0.9,
        target: 1.3,
        minEdge: 0,
        minProbability: 0.55,
      }),
    ).toBe(false);
    expect(
      shouldSkipSignal({
        probability: 0.50,
        confidence: 0.9,
        target: 1.3,
        minEdge: 0,
        minProbability: 0.55,
      }),
    ).toBe(true);
    expect(
      shouldSkipSignal({
        probability: 0.90,
        confidence: 0.4,
        target: 1.3,
        minEdge: 0,
        minConfidence: 0.5,
      }),
    ).toBe(true);
  });

  it("with minEdge=0 and no strategy SKIP, emits every valid probability (opt-out)", () => {
    expect(
      shouldSkipSignal({
        probability: 0.51,
        confidence: 0.2,
        target: 1.3,
        minEdge: 0,
        minProbability: 0,
        minConfidence: 0,
      }),
    ).toBe(false);
  });
});
