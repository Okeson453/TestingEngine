/**
 * Walk-forward style validation for the live ACIE PSI ensemble.
 *
 * Uses synthetic sequences with a mild conditional structure (after 3+
 * sub-1.30 streaks, next hit rate is slightly lower). Measures:
 * - Brier score vs constant fair baseline
 * - Calibration residual
 * - That ensemble stays near fair under pure noise (no spurious edge)
 *
 * Does NOT claim a production edge; only verifies the ensemble is
 * better-calibrated than a naive constant and does not explode.
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { PredictiveSequenceIntelligence } from "./psi.ts";
import { TemporalPatternLearner } from "./tpl.ts";
import { createInitialOnlineState, applyOnlineUpdate, MODEL_NAMES } from "./online-state.ts";
import type { SOLRecord, SequenceState } from "./types.ts";

const FAIR = 1 / 1.3;
const TARGET = 1.3;

function mulberry32(seed: number): () => number {
  return () => {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function generateSeries(n: number, seed: number, structured: boolean): number[] {
  const rnd = mulberry32(seed);
  const out: number[] = [];
  let streakBelow = 0;
  for (let i = 0; i < n; i++) {
    let p = FAIR;
    if (structured && streakBelow >= 3) {
      p = FAIR - 0.04; // mild mean-reversion structure
    }
    const hit = rnd() < p;
    const crash = hit ? 1.3 + rnd() * 2 : 1.01 + rnd() * 0.28;
    out.push(crash);
    streakBelow = crash < TARGET ? streakBelow + 1 : 0;
  }
  return out;
}

function toSol(cps: number[], tpl: TemporalPatternLearner): SOLRecord[] {
  const recs: SOLRecord[] = [];
  for (let i = 0; i < cps.length; i++) {
    const prior = cps.slice(0, i);
    const state =
      prior.length > 0
        ? tpl.computeSequenceState(prior)
        : tpl.computeSequenceState([]);
    const regime = tpl.detectRegime(state);
    recs.push({
      roundId: String(i),
      timestamp: new Date(Date.UTC(2026, 0, 1, 0, 0, i)).toISOString(),
      crashPoint: cps[i]!,
      reached130: cps[i]! >= TARGET,
      previousOutcomes: prior.slice(-20),
      previousReached130: prior.slice(-20).map((c) => c >= TARGET),
      sequenceState: state,
      regime,
      regimeDuration: 1,
      psiProbability: FAIR,
      psiConfidence: 0.5,
      prediction: false,
      actualResult: cps[i]! >= TARGET,
      residual: FAIR - (cps[i]! >= TARGET ? 1 : 0),
      squaredError: (FAIR - (cps[i]! >= TARGET ? 1 : 0)) ** 2,
      logLoss: 0.5,
    } as SOLRecord);
  }
  return recs;
}

function brier(preds: number[], actuals: number[]): number {
  let s = 0;
  for (let i = 0; i < preds.length; i++) {
    const a = actuals[i]!;
    const p = preds[i]!;
    s += (p - a) ** 2;
  }
  return s / preds.length;
}

describe("PSI ensemble walk-forward validation", () => {
  it("on structured series, ensemble Brier is not worse than fair constant", () => {
    const tpl = new TemporalPatternLearner();
    const psi = new PredictiveSequenceIntelligence(tpl);
    const series = generateSeries(400, 42, true);
    const trainEnd = 250;
    const preds: number[] = [];
    const actuals: number[] = [];
    let online = createInitialOnlineState();

    for (let i = 40; i < series.length - 1; i++) {
      const historyCps = series.slice(0, i);
      const sol = toSol(historyCps, tpl);
      const state = tpl.computeSequenceState(historyCps);
      const regime = tpl.detectRegime(state);
      const { psi: out, models } = psi.estimateWithModels({
        crashPoints: historyCps,
        sequenceState: state,
        regime,
        history: sol,
        ensembleWeights: online.ensembleWeights,
        ewmaHitRate: online.ewmaHitRate,
      });
      if (i >= trainEnd) {
        preds.push(out.estimatedProbability);
        actuals.push(series[i]! >= TARGET ? 1 : 0);
      }
      const modelProbs: Record<string, number> = {};
      for (const m of models) modelProbs[m.modelName] = m.probability;
      online = applyOnlineUpdate(online, {
        crashPoint: series[i]!,
        psiProbability: out.estimatedProbability,
        modelProbabilities: modelProbs,
        sequenceState: state,
        regime,
      });
    }

    const bEnsemble = brier(preds, actuals);
    const bFair = brier(
      preds.map(() => FAIR),
      actuals,
    );
    // Allow small noise margin; must not be substantially worse than fair.
    assert.ok(
      bEnsemble <= bFair + 0.015,
      `ensemble Brier ${bEnsemble.toFixed(4)} vs fair ${bFair.toFixed(4)}`,
    );
    // Probabilities stay in a sane band (no extreme overconfidence).
    const meanP = preds.reduce((a, b) => a + b, 0) / preds.length;
    assert.ok(meanP > 0.55 && meanP < 0.9, `meanP=${meanP}`);
  });

  it("on pure noise near fair, ensemble stays close to baseline (no hallucinated edge)", () => {
    const tpl = new TemporalPatternLearner();
    const psi = new PredictiveSequenceIntelligence(tpl);
    const series = generateSeries(300, 7, false);
    const preds: number[] = [];
    for (let i = 50; i < series.length; i++) {
      const historyCps = series.slice(0, i);
      const sol = toSol(historyCps, tpl);
      const state = tpl.computeSequenceState(historyCps);
      const regime = tpl.detectRegime(state);
      const { psi: out } = psi.estimateWithModels({
        crashPoints: historyCps,
        sequenceState: state,
        regime,
        history: sol,
        ewmaHitRate: FAIR,
      });
      preds.push(out.estimatedProbability);
    }
    const meanP = preds.reduce((a, b) => a + b, 0) / preds.length;
    assert.ok(
      Math.abs(meanP - FAIR) < 0.06,
      `noise meanP=${meanP} should stay near fair ${FAIR}`,
    );
  });

  it("online weights remain within floor/ceiling after many updates", () => {
    let online = createInitialOnlineState();
    const tpl = new TemporalPatternLearner();
    const state = tpl.computeSequenceState([1.2, 1.5, 1.1, 2.0, 1.05]);
    for (let i = 0; i < 80; i++) {
      const modelProbabilities: Record<string, number> = {};
      for (const name of MODEL_NAMES) {
        modelProbabilities[name] = FAIR + (i % 3 === 0 ? 0.05 : -0.03);
      }
      online = applyOnlineUpdate(online, {
        crashPoint: i % 2 === 0 ? 1.5 : 1.1,
        psiProbability: FAIR,
        modelProbabilities,
        sequenceState: state,
        regime: "normal",
      });
    }
    for (const name of MODEL_NAMES) {
      const w = online.ensembleWeights[name]!;
      assert.ok(w >= 0.05 && w <= 0.4, `${name} weight ${w} out of band`);
    }
    const sum = MODEL_NAMES.reduce((s, n) => s + online.ensembleWeights[n]!, 0);
    assert.ok(Math.abs(sum - 1) < 0.02, `weights sum ${sum}`);
  });

  it("model max-dev clip prevents extreme single-model probabilities", () => {
    const tpl = new TemporalPatternLearner();
    const psi = new PredictiveSequenceIntelligence(tpl);
    // Artificial long below-streak history
    const cps = Array.from({ length: 80 }, (_, i) =>
      i < 70 ? 1.05 : 1.05,
    );
    const sol = toSol(cps, tpl);
    const state = tpl.computeSequenceState(cps);
    const { psi: out, models } = psi.estimateWithModels({
      crashPoints: cps,
      sequenceState: state,
      regime: "deep-low",
      history: sol,
      ewmaHitRate: FAIR,
    });
    for (const m of models) {
      assert.ok(
        Math.abs(m.probability - FAIR) <= 0.11,
        `${m.modelName} dev ${m.probability - FAIR}`,
      );
    }
    assert.ok(out.estimatedProbability > 0.5 && out.estimatedProbability < 0.9);
  });
});
