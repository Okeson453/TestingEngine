/**
 * Selectivity gate: default absolute probability floor at 65%.
 * Re-enable fair+edge selectivity via MIN_SIGNAL_EDGE>0.
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  MIN_SIGNAL_EDGE,
  MIN_SIGNAL_PROBABILITY,
  MIN_SIGNAL_CONFIDENCE,
  ALLOW_REDUCED_ENTRY,
  shouldSkipSignal,
} from "./predictor.ts";
import { _resetAdaptiveEdgeForTests } from "./adaptive-edge.ts";

describe("selectivity gate (shouldSkipSignal)", () => {
  it("defaults to absolute 65% probability gate (edge=0)", () => {
    assert.equal(MIN_SIGNAL_EDGE, 0);
    assert.equal(MIN_SIGNAL_PROBABILITY, 0.65);
    assert.equal(MIN_SIGNAL_CONFIDENCE, 0);
    assert.equal(ALLOW_REDUCED_ENTRY, false);
  });

  it("skips below 65% and allows at/above 65% under default edge=0", () => {
    _resetAdaptiveEdgeForTests();
    assert.equal(
      shouldSkipSignal({ probability: 0.64, confidence: 0.9, target: 1.3, minEdge: 0 }),
      true,
    );
    assert.equal(
      shouldSkipSignal({ probability: 0.65, confidence: 0.9, target: 1.3, minEdge: 0 }),
      false,
    );
    assert.equal(
      shouldSkipSignal({ probability: 0.70, confidence: 0.9, target: 1.3, minEdge: 0 }),
      false,
    );
    assert.equal(
      shouldSkipSignal({ probability: 0.74, confidence: 0.9, target: 1.3, minEdge: 0 }),
      false,
    );
  });

  it("fair+edge selectivity still applies when minEdge>0", () => {
    // needP = max(0.65, 0.7692+0.03) = 0.7992
    assert.equal(
      shouldSkipSignal({ probability: 0.70, confidence: 0.9, target: 1.3, minEdge: 0.03 }),
      true,
    );
    assert.equal(
      shouldSkipSignal({ probability: 0.79, confidence: 0.9, target: 1.3, minEdge: 0.03 }),
      true,
    );
    assert.equal(
      shouldSkipSignal({ probability: 0.85, confidence: 0.9, target: 1.3, minEdge: 0.03 }),
      false,
    );
  });

  it("skips REDUCED_ENTRY by default", () => {
    assert.equal(
      shouldSkipSignal({
        probability: 0.88,
        confidence: 0.8,
        target: 1.3,
        minEdge: 0,
        strategyAction: "REDUCED_ENTRY",
      }),
      true,
    );
  });

  it("allows ENTRY at 65%+ with default gate", () => {
    assert.equal(
      shouldSkipSignal({
        probability: 0.68,
        confidence: 0.8,
        target: 1.3,
        minEdge: 0,
        strategyAction: "ENTRY",
      }),
      false,
    );
  });

  it("skips on strategy_action=SKIP even when probability is high", () => {
    assert.equal(
      shouldSkipSignal({
        probability: 0.95,
        confidence: 0.95,
        target: 1.3,
        minEdge: 0,
        strategyAction: "SKIP",
      }),
      true,
    );
  });
});
