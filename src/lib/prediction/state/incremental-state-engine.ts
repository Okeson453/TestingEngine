/**
 * Phase 1 — Incremental State Engine
 * O(1) sufficient statistics for the prediction critical path.
 * No full-history scans; no DB I/O.
 */

export const PREDICTION_TARGETS = [1.3, 2.0, 5.0] as const;
export type PredictionTarget = (typeof PREDICTION_TARGETS)[number];

export interface HitCounters {
  t13: number;
  t20: number;
  t50: number;
  t100: number;
}

export interface RunState {
  below13: number;
  above13: number;
  below15: number;
  below20: number;
  above20: number;
  maxBelow13: number;
  maxAbove13: number;
  maxBelow20: number;
}

/**
 * Rounds-since-last-hit counters for each target threshold.
 * Incremented every round; reset to 0 when the target is hit.
 */
export interface SinceCounters {
  t13: number;
  t20: number;
  t50: number;
  t100: number;
}

/**
 * Explicit engine lifecycle state — replaces implicit sample-count
 * inference scattered across the pipeline.
 *
 * COLD       — no observations yet (count === 0)
 * WARMING    — partial state, V1 fallback still in use (1..19)
 * WARM       — V2 incremental path active, building confidence (20..99)
 * PRODUCTION — fully warmed, all features real (≥100)
 * DEGRADED   — stale state (no update in >5 min while count > 0)
 */
export type EngineLifecycleState =
  | 'COLD'
  | 'WARMING'
  | 'WARM'
  | 'PRODUCTION'
  | 'DEGRADED';

const DEGRADED_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes

export interface MarkovState {
  /** 2x2 on (prev>=1.3, curr>=1.3): [fromBelow][toAbove] counts */
  trans: [[number, number], [number, number]];
  lastAbove13: boolean | null;
}

export interface WelfordState {
  n: number;
  mean: number;
  m2: number;
}

export interface IncrementalEngineSnapshot {
  count: number;
  lastCrash: number | null;
  welford: WelfordState;
  ewma: number;
  ewmaHit13: number;
  hits: HitCounters;
  runs: RunState;
  since: SinceCounters;
  markov: MarkovState;
  lagRing: number[];
  lagRingSize: number;
  shortSum: number;
  shortSumSq: number;
  shortHits13: number;
  shortCount: number;
  updatedAt: number;
  featureVersion: string;
}

export const LAG_CAP = 512;
const SHORT_CAP = 30;
const EWMA_ALPHA = 0.05;

function emptyWelford(): WelfordState {
  return { n: 0, mean: 0, m2: 0 };
}

function emptyRuns(): RunState {
  return {
    below13: 0,
    above13: 0,
    below15: 0,
    below20: 0,
    above20: 0,
    maxBelow13: 0,
    maxAbove13: 0,
    maxBelow20: 0,
  };
}

function emptySince(): SinceCounters {
  return { t13: 0, t20: 0, t50: 0, t100: 0 };
}

function emptyMarkov(): MarkovState {
  return {
    trans: [
      [0, 0],
      [0, 0],
    ],
    lastAbove13: null,
  };
}

export class IncrementalStateEngine {
  readonly featureVersion = 'inc-state-v1';
  private count = 0;
  private lastCrash: number | null = null;
  private welford: WelfordState = emptyWelford();
  private ewma = 1.3;
  private ewmaHit13 = 0.65;
  private hits: HitCounters = { t13: 0, t20: 0, t50: 0, t100: 0 };
  private runs: RunState = emptyRuns();
  private since: SinceCounters = emptySince();
  private markov: MarkovState = emptyMarkov();
  private lagRing: number[] = new Array(LAG_CAP).fill(0);
  private lagLen = 0;
  private lagPos = 0;
  /** Ring for last SHORT_CAP points (for short-window stats) */
  private shortRing: number[] = new Array(SHORT_CAP).fill(0);
  private shortLen = 0;
  private shortPos = 0;
  private shortSum = 0;
  private shortSumSq = 0;
  private shortHits13 = 0;
  private updatedAt = 0;

  seed(crashPoints: number[]): void {
    this.reset();
    for (const cp of crashPoints) {
      this.update(cp);
    }
  }

  reset(): void {
    this.count = 0;
    this.lastCrash = null;
    this.welford = emptyWelford();
    this.ewma = 1.3;
    this.ewmaHit13 = 0.65;
    this.hits = { t13: 0, t20: 0, t50: 0, t100: 0 };
    this.runs = emptyRuns();
    this.since = emptySince();
    this.markov = emptyMarkov();
    this.lagRing.fill(0);
    this.lagLen = 0;
    this.lagPos = 0;
    this.shortRing.fill(0);
    this.shortLen = 0;
    this.shortPos = 0;
    this.shortSum = 0;
    this.shortSumSq = 0;
    this.shortHits13 = 0;
    this.updatedAt = 0;
  }

  /** O(1) update on every crash */
  update(crashPoint: number): IncrementalEngineSnapshot {
    if (!Number.isFinite(crashPoint) || crashPoint <= 0) {
      return this.snapshot();
    }

    const above13 = crashPoint >= 1.3;
    const above20 = crashPoint >= 2.0;
    const above50 = crashPoint >= 5.0;
    const above100 = crashPoint >= 10.0;

    // Welford
    const w = this.welford;
    w.n += 1;
    const delta = crashPoint - w.mean;
    w.mean += delta / w.n;
    const delta2 = crashPoint - w.mean;
    w.m2 += delta * delta2;

    // EWMA
    this.ewma = EWMA_ALPHA * crashPoint + (1 - EWMA_ALPHA) * this.ewma;
    this.ewmaHit13 = EWMA_ALPHA * (above13 ? 1 : 0) + (1 - EWMA_ALPHA) * this.ewmaHit13;

    // Hits
    if (above13) this.hits.t13 += 1;
    if (above20) this.hits.t20 += 1;
    if (above50) this.hits.t50 += 1;
    if (above100) this.hits.t100 += 1;

    // Rounds-since-last-hit: increment all, then reset the ones that hit
    this.since.t13 += 1;
    this.since.t20 += 1;
    this.since.t50 += 1;
    this.since.t100 += 1;
    if (above13) this.since.t13 = 0;
    if (above20) this.since.t20 = 0;
    if (above50) this.since.t50 = 0;
    if (above100) this.since.t100 = 0;

    // Runs
    const r = this.runs;
    if (above13) {
      r.above13 += 1;
      r.below13 = 0;
      if (r.above13 > r.maxAbove13) r.maxAbove13 = r.above13;
    } else {
      r.below13 += 1;
      r.above13 = 0;
      if (r.below13 > r.maxBelow13) r.maxBelow13 = r.below13;
    }
    if (crashPoint < 1.5) r.below15 += 1;
    else r.below15 = 0;
    if (above20) {
      r.above20 += 1;
      r.below20 = 0;
    } else {
      r.below20 += 1;
      r.above20 = 0;
      if (r.below20 > r.maxBelow20) r.maxBelow20 = r.below20;
    }

    // Markov 2-state
    if (this.markov.lastAbove13 !== null) {
      const from = this.markov.lastAbove13 ? 1 : 0;
      const to = above13 ? 1 : 0;
      this.markov.trans[from][to] += 1;
    }
    this.markov.lastAbove13 = above13;

    // Lag ring
    this.lagRing[this.lagPos] = crashPoint;
    this.lagPos = (this.lagPos + 1) % LAG_CAP;
    if (this.lagLen < LAG_CAP) this.lagLen += 1;

    // Short window ring (maintain sum / sumSq / hits)
    if (this.shortLen === SHORT_CAP) {
      const evicted = this.shortRing[this.shortPos];
      this.shortSum -= evicted;
      this.shortSumSq -= evicted * evicted;
      if (evicted >= 1.3) this.shortHits13 -= 1;
    } else {
      this.shortLen += 1;
    }
    this.shortRing[this.shortPos] = crashPoint;
    this.shortPos = (this.shortPos + 1) % SHORT_CAP;
    this.shortSum += crashPoint;
    this.shortSumSq += crashPoint * crashPoint;
    if (above13) this.shortHits13 += 1;

    this.count += 1;
    this.lastCrash = crashPoint;
    this.updatedAt = Date.now();
    return this.snapshot();
  }

  snapshot(): IncrementalEngineSnapshot {
    return {
      count: this.count,
      lastCrash: this.lastCrash,
      welford: { ...this.welford },
      ewma: this.ewma,
      ewmaHit13: this.ewmaHit13,
      hits: { ...this.hits },
      runs: { ...this.runs },
      since: { ...this.since },
      markov: {
        trans: [
          [this.markov.trans[0][0], this.markov.trans[0][1]],
          [this.markov.trans[1][0], this.markov.trans[1][1]],
        ],
        lastAbove13: this.markov.lastAbove13,
      },
      lagRing: this.getLagArray(),
      lagRingSize: this.lagLen,
      shortSum: this.shortSum,
      shortSumSq: this.shortSumSq,
      shortHits13: this.shortHits13,
      shortCount: this.shortLen,
      updatedAt: this.updatedAt,
      featureVersion: this.featureVersion,
    };
  }

  getLagArray(): number[] {
    if (this.lagLen === 0) return [];
    const out: number[] = new Array(this.lagLen);
    const start = this.lagLen < LAG_CAP ? 0 : this.lagPos;
    for (let i = 0; i < this.lagLen; i++) {
      out[i] = this.lagRing[(start + i) % LAG_CAP];
    }
    return out;
  }

  variance(): number {
    const w = this.welford;
    return w.n > 1 ? w.m2 / (w.n - 1) : 0;
  }

  std(): number {
    return Math.sqrt(this.variance());
  }

  hitRate(target: number): number {
    if (this.count === 0) return 0;
    if (target <= 1.3) return this.hits.t13 / this.count;
    if (target <= 2.0) return this.hits.t20 / this.count;
    if (target <= 5.0) return this.hits.t50 / this.count;
    return this.hits.t100 / this.count;
  }

  /** Rounds since the last crash point ≥ target. Returns count if never hit. */
  roundsSince(target: number): number {
    if (target <= 1.3) return this.since.t13;
    if (target <= 2.0) return this.since.t20;
    if (target <= 5.0) return this.since.t50;
    return this.since.t100;
  }

  /**
   * Empirical hit rate over the last `window` rounds from the lag ring.
   * Semantics are exact: window=50 means the most recent min(50, lagLen) points.
   * Does NOT use the SHORT_CAP=30 ring (that remains short_hit_13 only).
   */
  windowHitRate(window: number, target = 1.3): number {
    const n = Math.min(Math.max(0, Math.floor(window)), this.lagLen);
    if (n <= 0) return 0;
    let hits = 0;
    // Newest is at lagPos-1; walk backward n steps
    for (let i = 1; i <= n; i++) {
      const idx = (this.lagPos - i + this.lagRing.length) % this.lagRing.length;
      if (this.lagRing[idx] >= target) hits += 1;
    }
    return hits / n;
  }

  /** Convenience: exact 20/50/100/200 windows for ≥1.3x */
  hitRateWindows13(): { w20: number; w50: number; w100: number; w200: number } {
    return {
      w20: this.windowHitRate(20, 1.3),
      w50: this.windowHitRate(50, 1.3),
      w100: this.windowHitRate(100, 1.3),
      w200: this.windowHitRate(200, 1.3),
    };
  }

  shortHitRate13(): number {
    return this.shortLen > 0 ? this.shortHits13 / this.shortLen : 0;
  }

  shortMean(): number {
    return this.shortLen > 0 ? this.shortSum / this.shortLen : 0;
  }

  shortVariance(): number {
    if (this.shortLen < 2) return 0;
    const mean = this.shortSum / this.shortLen;
    return Math.max(0, this.shortSumSq / this.shortLen - mean * mean);
  }

  markovPNextAbove13(): number {
    if (this.markov.lastAbove13 === null) return this.ewmaHit13;
    const from = this.markov.lastAbove13 ? 1 : 0;
    const row = this.markov.trans[from];
    const total = row[0] + row[1];
    if (total < 10) return this.ewmaHit13;
    return row[1] / total;
  }

  isWarm(minCount = 50): boolean {
    return this.count >= minCount;
  }

  /**
   * Explicit lifecycle state based on observation count and staleness.
   * Replaces ad-hoc `Math.min(1, snap.count / 100)` inference scattered
   * across the pipeline with a single authoritative state.
   */
  getLifecycleState(): EngineLifecycleState {
    if (this.count === 0) return 'COLD';
    const stale = Date.now() - this.updatedAt > DEGRADED_TIMEOUT_MS;
    if (stale) return 'DEGRADED';
    if (this.count < 20) return 'WARMING';
    if (this.count < 100) return 'WARM';
    return 'PRODUCTION';
  }
  /** Recent crash points for snapshot persistence (oldest→newest, capped at LAG_CAP). */
  getRecentPoints(max = LAG_CAP): number[] {
    const n = Math.min(max, this.lagLen);
    if (n <= 0) return [];
    const out: number[] = new Array(n);
    const start = (this.lagPos - this.lagLen + this.lagRing.length) % this.lagRing.length;
    for (let i = 0; i < n; i++) {
      out[i] = this.lagRing[(start + i) % this.lagRing.length];
    }
    return out;
  }

}

export const globalIncrementalState = new IncrementalStateEngine();
