/**
 * Tests for IncrementalStateEngine fixes:
 * - since_* counters track rounds-since-last-hit (were hard-coded 0)
 * - hits.t100 / hitRate(10.0) returns real measured rate (was hitRate(5.0)*0.4)
 * - runs.below20 / maxBelow20 tracked (were missing)
 * - LAG_CAP is 512 (was 64) and getRecentPoints respects it
 * - getLifecycleState() returns explicit state
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { IncrementalStateEngine, LAG_CAP } from './incremental-state-engine.ts';
import { FeatureEngineV2 } from '../features/feature-engine-v2.ts';

describe('IncrementalStateEngine — since_* counters', () => {
  let engine: IncrementalStateEngine;

  beforeEach(() => {
    engine = new IncrementalStateEngine();
  });

  it('all since counters start at 0', () => {
    const snap = engine.snapshot();
    expect(snap.since.t13).toBe(0);
    expect(snap.since.t20).toBe(0);
    expect(snap.since.t50).toBe(0);
    expect(snap.since.t100).toBe(0);
  });

  it('increments since counters every round and resets on hit', () => {
    // Round 1: crash at 1.0 (below all thresholds)
    engine.update(1.0);
    let snap = engine.snapshot();
    expect(snap.since.t13).toBe(1);
    expect(snap.since.t20).toBe(1);
    expect(snap.since.t50).toBe(1);
    expect(snap.since.t100).toBe(1);

    // Round 2: crash at 1.5 (above 1.3 only)
    engine.update(1.5);
    snap = engine.snapshot();
    expect(snap.since.t13).toBe(0); // reset
    expect(snap.since.t20).toBe(2); // still counting
    expect(snap.since.t50).toBe(2);
    expect(snap.since.t100).toBe(2);

    // Round 3: crash at 3.0 (above 1.3 and 2.0)
    engine.update(3.0);
    snap = engine.snapshot();
    expect(snap.since.t13).toBe(0);
    expect(snap.since.t20).toBe(0); // reset
    expect(snap.since.t50).toBe(3);
    expect(snap.since.t100).toBe(3);

    // Round 4: crash at 12.0 (above all)
    engine.update(12.0);
    snap = engine.snapshot();
    expect(snap.since.t13).toBe(0);
    expect(snap.since.t20).toBe(0);
    expect(snap.since.t50).toBe(0);
    expect(snap.since.t100).toBe(0); // reset
  });

  it('roundsSince() accessor matches snapshot values', () => {
    engine.update(1.0);
    engine.update(1.0);
    engine.update(1.0);
    expect(engine.roundsSince(1.3)).toBe(3);
    expect(engine.roundsSince(2.0)).toBe(3);
    expect(engine.roundsSince(5.0)).toBe(3);
    expect(engine.roundsSince(10.0)).toBe(3);
  });

  it('since counters survive many rounds without hitting target', () => {
    for (let i = 0; i < 50; i++) {
      engine.update(1.0);
    }
    const snap = engine.snapshot();
    expect(snap.since.t13).toBe(50);
    expect(snap.since.t100).toBe(50);
  });

  it('reset() clears since counters', () => {
    engine.update(1.0);
    engine.update(1.0);
    engine.reset();
    const snap = engine.snapshot();
    expect(snap.since.t13).toBe(0);
    expect(snap.since.t20).toBe(0);
  });
});

describe('IncrementalStateEngine — hits.t100 and hitRate(10.0)', () => {
  let engine: IncrementalStateEngine;

  beforeEach(() => {
    engine = new IncrementalStateEngine();
  });

  it('tracks hits at 10.0 threshold', () => {
    engine.update(1.0);
    engine.update(10.5);
    engine.update(1.0);
    engine.update(15.0);
    const snap = engine.snapshot();
    expect(snap.hits.t100).toBe(2);
  });

  it('hitRate(10.0) returns real measured rate', () => {
    for (let i = 0; i < 10; i++) {
      engine.update(i < 2 ? 12.0 : 1.0);
    }
    // 2 out of 10 rounds hit 10x
    expect(engine.hitRate(10.0)).toBeCloseTo(0.2, 5);
  });

  it('hitRate(10.0) is not approximated from 5x rate', () => {
    for (let i = 0; i < 10; i++) {
      // All rounds hit 5x but none hit 10x
      engine.update(5.0);
    }
    expect(engine.hitRate(5.0)).toBeCloseTo(1.0, 5);
    expect(engine.hitRate(10.0)).toBeCloseTo(0.0, 5);
  });
});

describe('IncrementalStateEngine — runs.below20 and maxBelow20', () => {
  let engine: IncrementalStateEngine;

  beforeEach(() => {
    engine = new IncrementalStateEngine();
  });

  it('tracks consecutive below-2.0 runs', () => {
    engine.update(1.0);
    engine.update(1.5);
    engine.update(1.0);
    let snap = engine.snapshot();
    expect(snap.runs.below20).toBe(3);

    // Hit 2.0 — resets below20
    engine.update(2.5);
    snap = engine.snapshot();
    expect(snap.runs.below20).toBe(0);
  });

  it('tracks maxBelow20 across the lifetime', () => {
    for (let i = 0; i < 5; i++) engine.update(1.0);
    engine.update(2.5); // reset
    for (let i = 0; i < 3; i++) engine.update(1.0);
    const snap = engine.snapshot();
    expect(snap.runs.maxBelow20).toBe(5);
  });
});

describe('IncrementalStateEngine — LAG_CAP and getRecentPoints', () => {
  it('LAG_CAP is 512', () => {
    expect(LAG_CAP).toBe(512);
  });

  it('getRecentPoints returns up to LAG_CAP points', () => {
    const engine = new IncrementalStateEngine();
    const total = 600;
    for (let i = 0; i < total; i++) {
      engine.update(1.0 + i * 0.01);
    }
    const points = engine.getRecentPoints();
    expect(points.length).toBe(LAG_CAP);
  });

  it('getRecentPoints returns fewer when history is short', () => {
    const engine = new IncrementalStateEngine();
    for (let i = 0; i < 10; i++) {
      engine.update(1.0 + i * 0.1);
    }
    const points = engine.getRecentPoints();
    expect(points.length).toBe(10);
  });

  it('getRecentPoints respects max parameter', () => {
    const engine = new IncrementalStateEngine();
    for (let i = 0; i < 100; i++) {
      engine.update(1.0 + i * 0.1);
    }
    const points = engine.getRecentPoints(50);
    expect(points.length).toBe(50);
  });

  it('getRecentPoints returns oldest-to-newest order', () => {
    const engine = new IncrementalStateEngine();
    for (let i = 0; i < 5; i++) {
      engine.update(10 + i);
    }
    const points = engine.getRecentPoints();
    expect(points[0]).toBe(10);
    expect(points[4]).toBe(14);
  });
});

describe('IncrementalStateEngine — getLifecycleState', () => {
  it('returns COLD when no observations', () => {
    const engine = new IncrementalStateEngine();
    expect(engine.getLifecycleState()).toBe('COLD');
  });

  it('returns WARMING with 1-19 observations', () => {
    const engine = new IncrementalStateEngine();
    for (let i = 0; i < 10; i++) engine.update(1.0);
    expect(engine.getLifecycleState()).toBe('WARMING');
  });

  it('returns WARM with 20-99 observations', () => {
    const engine = new IncrementalStateEngine();
    for (let i = 0; i < 50; i++) engine.update(1.0);
    expect(engine.getLifecycleState()).toBe('WARM');
  });

  it('returns PRODUCTION with >=100 observations', () => {
    const engine = new IncrementalStateEngine();
    for (let i = 0; i < 100; i++) engine.update(1.0);
    expect(engine.getLifecycleState()).toBe('PRODUCTION');
  });
});

describe('FeatureEngineV2 — real since_* and consec_below_* features', () => {
  let engine: IncrementalStateEngine;

  beforeEach(() => {
    engine = new IncrementalStateEngine();
  });

  it('since_* features reflect real rounds-since-last-hit', () => {
    // Seed: 5 rounds below 1.3, then 1 round at 1.5, then 3 below
    const cps = [1.0, 1.0, 1.0, 1.0, 1.0, 1.5, 1.0, 1.0, 1.0];
    engine.seed(cps);

    
    const v2 = new FeatureEngineV2(engine);
    const fv = v2.snapshotFromState('test', new Date().toISOString());

    // Last 3 rounds were below 1.3 (since the hit at round 6)
    expect(fv.values.since_1_30).toBe(3);
    // All 9 rounds were below 2.0
    expect(fv.values.since_2_00).toBe(9);
    // Not zero anymore
    expect(fv.values.since_1_30).not.toBe(0);
  });

  it('consec_below_1_30 reflects real consecutive below-1.3 streak', () => {
    // 5 rounds below 1.3
    engine.seed([1.0, 1.0, 1.0, 1.0, 1.0]);

    
    const v2 = new FeatureEngineV2(engine);
    const fv = v2.snapshotFromState('test', new Date().toISOString());

    expect(fv.values.consec_below_1_30).toBe(5);
    expect(fv.values.consec_below_1_30).not.toBe(0);
  });

  it('consec_below_2_00 reflects real consecutive below-2.0 streak', () => {
    // 8 rounds all below 2.0
    engine.seed([1.0, 1.5, 1.0, 1.5, 1.0, 1.5, 1.0, 1.5]);

    
    const v2 = new FeatureEngineV2(engine);
    const fv = v2.snapshotFromState('test', new Date().toISOString());

    expect(fv.values.consec_below_2_00).toBe(8);
  });

  it('hit_10_00_* features are real measured rates, not approximations', () => {
    // 10 rounds: 1 at 12.0, 9 at 1.0
    engine.seed([1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 12.0]);

    
    const v2 = new FeatureEngineV2(engine);
    const fv = v2.snapshotFromState('test', new Date().toISOString());

    // 1/10 = 0.1, not hitRate(5.0) * 0.4
    expect(fv.values.hit_10_00_50).toBeCloseTo(0.1, 5);
    expect(fv.values.hit_10_00_100).toBeCloseTo(0.1, 5);
  });
});
