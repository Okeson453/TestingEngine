/**
 * Adaptive edge + fair-odds strategy quality gates.
 */
import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";
import {
  getAdaptiveMinEdge,
  recordSignalOutcome,
  getAdaptiveEdgeStats,
  _resetAdaptiveEdgeForTests,
} from "./adaptive-edge.ts";
import {
  DEFAULT_STRATEGY_POLICY,
  HIGH_FREQUENCY_STRATEGY_POLICY,
  StrategyLayer,
} from "../acie/strategy.ts";

const FAIR = 1 / 1.3;

beforeEach(() => {
  _resetAdaptiveEdgeForTests();
});

describe("adaptive edge", () => {
  it("starts near base edge", () => {
    const e = getAdaptiveMinEdge();
    assert.ok(e >= 0.01 && e <= 0.08, `edge out of range: ${e}`);
  });

  it("raises edge after a losing streak of emitted signals", () => {
    const before = getAdaptiveMinEdge();
    for (let i = 0; i < 20; i++) recordSignalOutcome(false);
    const after = getAdaptiveMinEdge();
    assert.ok(after >= before, `expected edge to rise, before=${before} after=${after}`);
    const stats = getAdaptiveEdgeStats();
    assert.equal(stats.n, 20);
    assert.ok(stats.hitRate !== null && stats.hitRate < 0.1);
  });

  it("eases edge after strong winning streak", () => {
    for (let i = 0; i < 20; i++) recordSignalOutcome(true);
    const e = getAdaptiveMinEdge();
    // Should not go above base when overperforming
    assert.ok(e <= Number(process.env.MIN_SIGNAL_EDGE ?? 0.03) + 0.001);
  });
});

describe("strategy fair-odds quality", () => {
  it("DEFAULT policy thresholds are at or above fair 1.30 odds", () => {
    assert.ok(
      DEFAULT_STRATEGY_POLICY.supportedThreshold >= FAIR,
      `supported ${DEFAULT_STRATEGY_POLICY.supportedThreshold} < fair ${FAIR}`,
    );
    assert.ok(
      DEFAULT_STRATEGY_POLICY.weakThreshold >= FAIR,
      `weak ${DEFAULT_STRATEGY_POLICY.weakThreshold} < fair ${FAIR}`,
    );
  });

  it("HF policy also floors at fair", () => {
    assert.ok(HIGH_FREQUENCY_STRATEGY_POLICY.supportedThreshold >= FAIR - 1e-9);
    assert.ok(HIGH_FREQUENCY_STRATEGY_POLICY.fallbackThreshold >= FAIR - 1e-9);
  });

  it("skips when probability is below fair even if legacy-low", () => {
    const layer = new StrategyLayer(DEFAULT_STRATEGY_POLICY);
    const d = layer.evaluate({
      target: 1.3,
      probability: 0.7, // below fair
      confidenceInterval: [0.6, 0.8],
      calibrationError: 0.05,
      evidence: "SUPPORTED",
      regime: "normal",
      regimeStability: 10,
      uncertainty: { model: 0.1, data: 0.1, total: 0.14 },
      riskState: {
        currentExposure: 0,
        consecutiveLosses: 0,
        dailyEntriesUsed: 0,
        dailyEntriesLimit: 500,
        balance: 10_000,
      },
      baselineProbability: FAIR,
    });
    assert.equal(d.action, "SKIP");
    assert.equal(d.isOpportunity, false);
  });

  it("allows ENTRY when probability clearly exceeds fair + edge", () => {
    const layer = new StrategyLayer(DEFAULT_STRATEGY_POLICY);
    const d = layer.evaluate({
      target: 1.3,
      probability: 0.88,
      confidenceInterval: [0.8, 0.95],
      calibrationError: 0.03,
      evidence: "SUPPORTED",
      regime: "normal",
      regimeStability: 20,
      uncertainty: { model: 0.08, data: 0.08, total: 0.11 },
      riskState: {
        currentExposure: 0,
        consecutiveLosses: 0,
        dailyEntriesUsed: 10,
        dailyEntriesLimit: 500,
        balance: 10_000,
      },
      baselineProbability: FAIR,
    });
    assert.ok(
      d.action === "ENTRY" || d.action === "REDUCED_ENTRY",
      `expected ENTRY, got ${d.action}: ${d.reason}`,
    );
    assert.equal(d.isOpportunity, true);
  });
});
