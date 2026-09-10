/**
 * Lightweight feature contribution tracker (P1).
 * Records which feature keys were present and their values at prediction time,
 * correlated with outcomes for simple average-value-when-win vs loss analysis.
 */

export interface ContributionSample {
  features: Record<string, number>;
  predicted: number;
  actual: 0 | 1;
}

export interface FeatureContributionSummary {
  key: string;
  n: number;
  meanWhenWin: number;
  meanWhenLoss: number;
  delta: number;
}

const MAX = 500;

class FeatureContributionTracker {
  private samples: ContributionSample[] = [];

  observe(features: Record<string, number>, predicted: number, actual: 0 | 1): void {
    const slim: Record<string, number> = {};
    for (const k of [
      'hit_rate_50',
      'hit_rate_100',
      'hit_rate_200',
      'hit_1_30_50',
      'hit_1_30_100',
      'since_1_30',
      'roll_std_50',
      'short_hit_13',
      'ewma_hit_13',
    ]) {
      if (typeof features[k] === 'number' && Number.isFinite(features[k])) {
        slim[k] = features[k];
      }
    }
    this.samples.push({ features: slim, predicted, actual });
    if (this.samples.length > MAX) this.samples.shift();
  }

  summary(): FeatureContributionSummary[] {
    const keys = new Set<string>();
    for (const s of this.samples) {
      for (const k of Object.keys(s.features)) keys.add(k);
    }
    const out: FeatureContributionSummary[] = [];
    for (const key of keys) {
      let winSum = 0;
      let winN = 0;
      let lossSum = 0;
      let lossN = 0;
      for (const s of this.samples) {
        const v = s.features[key];
        if (v === undefined) continue;
        if (s.actual === 1) {
          winSum += v;
          winN += 1;
        } else {
          lossSum += v;
          lossN += 1;
        }
      }
      if (winN + lossN < 10) continue;
      const meanWhenWin = winN ? winSum / winN : 0;
      const meanWhenLoss = lossN ? lossSum / lossN : 0;
      out.push({
        key,
        n: winN + lossN,
        meanWhenWin,
        meanWhenLoss,
        delta: meanWhenWin - meanWhenLoss,
      });
    }
    return out.sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));
  }

  exportState(): ContributionSample[] {
    return this.samples.slice();
  }

  importState(samples: ContributionSample[] | null): void {
    if (!Array.isArray(samples)) return;
    this.samples = samples.slice(-MAX);
  }
}

export const globalFeatureContribution = new FeatureContributionTracker();
