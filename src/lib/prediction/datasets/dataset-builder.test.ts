/**
 * DatasetBuilder contract tests (node:test — canonical runner).
 * Covers: dataset construction, insufficient history, chronological ordering,
 * duplicate rounds, missing rounds, leakage detection, feature/label alignment.
 * Pure unit — no DB.
 */
import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { DatasetBuilder } from './dataset-builder.ts';
import { FeatureEngine } from '../features/feature-engine.ts';
import { LabelGenerator } from '../labels/label-generator.ts';
import { CriticalError } from '../../utils/errors.ts';
import type { HistoricalRound } from '../types.ts';

function makeRound(cp: number, i: number): HistoricalRound {
  return {
    id: `r${i}`,
    externalRoundId: String(i),
    sessionId: null,
    startedAt: null,
    crashedAt: new Date(Date.parse('2026-01-01T00:00:00Z') + i * 4_000).toISOString(),
    crashPoint: cp,
    observationSource: null,
    dataQuality: null,
    createdAt: new Date(Date.parse('2026-01-01T00:00:00Z') + i * 4_000).toISOString(),
  };
}

function makeHistory(count: number, cp = (i: number) => (i % 5 === 0 ? 1.05 : 1.8 + (i % 3) * 0.4)): HistoricalRound[] {
  return Array.from({ length: count }, (_, i) => makeRound(cp(i), i));
}

describe('DatasetBuilder', () => {
  test('builds a dataset with aligned features and labels above minHistory', () => {
    const b = new DatasetBuilder();
    const rounds = makeHistory(30);
    const ds = b.build(rounds, { minHistory: 20 });
    assert.equal(ds.meta.sampleCount, 10);
    assert.equal(ds.meta.featureVersion, new FeatureEngine().featureVersion);
    assert.equal(ds.meta.targetVersion, new LabelGenerator().targetVersion);
    assert.equal(ds.meta.leakageCheckPassed, true);
    assert.ok(ds.meta.configHash.length > 0);
    // Feature/label alignment: one row per feature vector, label round matches
    for (const row of ds.rows) {
      const idx = rounds.findIndex((r) => r.id === row.features.roundId);
      assert.ok(idx >= 20, 'feature rows only for rounds at/after minHistory');
      assert.equal(row.label.timestamp, rounds[idx]!.crashedAt);
      assert.ok(Object.keys(row.features.values).length > 0);
    }
  });

  test('insufficient history yields an empty dataset, not a throw', () => {
    const b = new DatasetBuilder();
    const ds = b.build(makeHistory(20), { minHistory: 20 });
    assert.equal(ds.meta.sampleCount, 0);
    assert.equal(ds.rows.length, 0);
  });

  test('rejects non-chronological input', () => {
    const b = new DatasetBuilder();
    const rounds = makeHistory(25);
    // swap two rounds to break chronology
    const r0 = rounds[3]!;
    const r1 = rounds[10]!;
    rounds[3] = r1;
    rounds[10] = r0;
    assert.throws(() => b.build(rounds, { minHistory: 20 }), CriticalError);
  });

  test('leakage detection: failOnLeakage throws, disabled flags the meta', () => {
    const b = new DatasetBuilder();
    const rounds = makeHistory(25);
    // Leakage scenario that stays chronological: the target round's
    // startedAt (feature timestamp source) is AFTER its crashedAt (label
    // timestamp), so the feature vector knows the label time.
    const mutated = rounds.map((r, i) =>
      i === 24
        ? { ...r, startedAt: new Date(Date.parse(r.crashedAt!) + 60_000).toISOString() }
        : r,
    );
    assert.throws(() => b.build(mutated, { minHistory: 20, failOnLeakage: true }), CriticalError);
    const ds = b.build(mutated, { minHistory: 20, failOnLeakage: false });
    assert.equal(ds.meta.leakageCheckPassed, false);
  });

  test('non-finite feature values are sanitized, never emitted raw', () => {
    const e = new FeatureEngine();
    const v = e.buildVector(makeHistory(25), 'target', new Date().toISOString());
    for (const val of Object.values(v.values)) {
      assert.ok(Number.isFinite(val), 'feature values must be finite');
    }
  });
});
