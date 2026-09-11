/**
 * Selective delivery: only full ENTRY with fair+edge is emitted.
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

describe("selectivity gate (shouldSkipSignal)", () => {
  it("defaults MIN_SIGNAL_EDGE to a selective positive edge", () => {
    assert.ok(MIN_SIGNAL_EDGE > 0);
    assert.ok(Math.abs(MIN_SIGNAL_EDGE - 0.03) < 1e-9);
    assert.equal(MIN_SIGNAL_PROBABILITY, 0);
    assert.equal(MIN_SIGNAL_CONFIDENCE, 0);
    assert.equal(ALLOW_REDUCED_ENTRY, false);
  });

  it("skips when probability is below fair+edge for 1.3x", () => {
    assert.equal(
      shouldSkipSignal({ probability: 0.70, confidence: 0.9, target: 1.3 }),
      true,
    );
    assert.equal(
      shouldSkipSignal({ probability: 0.79, confidence: 0.9, target: 1.3 }),
      true,
    );
    assert.equal(
      shouldSkipSignal({ probability: 0.85, confidence: 0.9, target: 1.3 }),
      false,
    );
  });

  it("skips REDUCED_ENTRY by default", () => {
    assert.equal(
      shouldSkipSignal({
        probability: 0.88,
        confidence: 0.8,
        target: 1.3,
        strategyAction: "REDUCED_ENTRY",
      }),
      true,
    );
  });

  it("allows ENTRY with strong probability", () => {
    assert.equal(
      shouldSkipSignal({
        probability: 0.88,
        confidence: 0.8,
        target: 1.3,
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
        strategyAction: "SKIP",
      }),
      true,
    );
  });
});
