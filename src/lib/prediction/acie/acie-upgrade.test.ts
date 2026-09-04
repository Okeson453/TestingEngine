/**
 * ACIE v4 upgrade acceptance tests (P0/P1) — pure unit tests, no DB.
 * Spec: ACIE_Combined_Upgrade_Recommendations.md
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { PlattCalibrator } from "../calibration/platt-calibrator.ts";
import { fractionalKellyStake } from "../stake/kelly-sizer.ts";
import { computeFeatures } from "../features/calculators.ts";
import type { HistoricalRound } from "../types.ts";

test("§5.2 Platt calibrator maps overconfident scores downward", () => {
  const cal = new PlattCalibrator();
  const pairs: Array<{ p: number; y: 0 | 1 }> = [];
  for (let i = 0; i < 100; i++) {
    pairs.push({ p: 0.9, y: i < 60 ? 1 : 0 });
  }
  cal.fit(pairs);
  assert.equal(cal.fitted, true);
  const calibrated = cal.calibrate(0.9);
  assert.ok(calibrated < 0.9, `expected calibrated < 0.9, got ${calibrated}`);
  assert.ok(calibrated > 0.4, `expected calibrated > 0.4, got ${calibrated}`);
});

test("§5.2 Platt falls back to identity when unfitted", () => {
  const cal = new PlattCalibrator();
  assert.equal(cal.calibrate(0.73), 0.73);
});

test("§6.4 fractional Kelly returns 0 stake when no edge", () => {
  const r = fractionalKellyStake({
    calibratedProbability: 0.4,
    target: 1.3,
    bankroll: 10_000,
    sampleConfidence: 1,
    calibrationConfidence: 1,
    evidenceQuality: 1,
    modelAgreement: 1,
    drawdownPressure: 0,
  });
  assert.equal(r.stake, 0);
  assert.ok(r.kellyFull <= 0);
});

test("§6.4 fractional Kelly positive edge sizes stake", () => {
  const r = fractionalKellyStake({
    calibratedProbability: 0.8,
    target: 1.3,
    bankroll: 10_000,
    sampleConfidence: 1,
    calibrationConfidence: 1,
    evidenceQuality: 1,
    modelAgreement: 1,
    drawdownPressure: 0,
    fraction: 0.25,
    maxBankrollFraction: 0.05,
  });
  assert.ok(r.stake > 0, `expected positive stake, got ${r.stake}`);
  assert.ok(r.stake <= 500, `stake should respect maxFrac, got ${r.stake}`);
});

test("§6.3 temporal features present and finite", () => {
  const now = Date.now();
  const rounds: HistoricalRound[] = [];
  for (let i = 0; i < 30; i++) {
    rounds.push({
      id: String(i),
      externalRoundId: String(i),
      sessionId: null,
      startedAt: new Date(now - (30 - i) * 4000).toISOString(),
      crashedAt: new Date(now - (30 - i) * 4000 + 3000).toISOString(),
      crashPoint: 1 + (i % 5) * 0.2,
      observationSource: "test",
      dataQuality: "high",
      createdAt: new Date(now - (30 - i) * 4000).toISOString(),
    });
  }
  const f = computeFeatures(rounds, new Date(now).toISOString());
  assert.ok(Number.isFinite(f.seconds_since_prev));
  assert.ok(Number.isFinite(f.rounds_per_hour_est));
  assert.ok(Number.isFinite(f.time_since_hit_1_30_sec));
  assert.ok(Number.isFinite(f.hour_utc));
  assert.ok(Number.isFinite(f.since_1_30));
});

test("§5.2 Platt does not produce NaN or out-of-range values", () => {
  const cal = new PlattCalibrator();
  const pairs: Array<{ p: number; y: 0 | 1 }> = [];
  for (let i = 0; i < 50; i++) {
    pairs.push({ p: 0.55 + (i % 10) * 0.03, y: i % 2 === 0 ? 1 : 0 });
  }
  cal.fit(pairs);
  for (const p of [0.01, 0.5, 0.99]) {
    const c = cal.calibrate(p);
    assert.ok(Number.isFinite(c) && c > 0 && c < 1, `bad calibrate(${p})=${c}`);
  }
});
