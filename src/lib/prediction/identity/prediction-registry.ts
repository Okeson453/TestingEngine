/**
 * Prediction identity + temporal validity + rolling performance (audit fixes
 * for Problems 1, 2, 5, 6, 8, 9 of the prediction-stack diagnosis).
 *
 * P0 — Data lineage: every prediction is registered as an immutable
 * PredictionRecord keyed by targetRoundId. Feedback resolution looks up the
 * EXACT record for the round that just ended instead of relying on
 * `lastEmittedProbability`, which can point at the wrong prediction when a
 * signal is replaced or re-emitted.
 *
 * P0 — Temporal validity: BG (round start) backfills targetStartedAt. A
 * prediction with createdAt >= targetStartedAt is TEMPORALLY_INVALID and is
 * excluded from calibration/learning/performance statistics.
 *
 * P1 — Rolling performance windows (25/50/100/250) with win rate, Brier,
 * log loss, ECE, average confidence, predicted vs actual hit rate, plus a
 * drift controller (NORMAL → MONITOR → DEGRADED → LEARNING_RESTRICTED →
 * FROZEN) so outcome observation can be separated from model updates when
 * performance deteriorates — not just on 4-loss streaks.
 *
 * Zero DB I/O. This is process-local state; pending_predictions /
 * prediction_validations remain the durable backstop.
 */

export type TemporalValidity =
  | "TEMPORALLY_VALID"
  | "TEMPORALLY_UNVERIFIED"
  | "TEMPORALLY_INVALID";

import type { FeaturePath } from "../types.ts";
// Canonical FeaturePath lives in ../types.ts — one signal schema, no
// caller-side duplicates of the contract.
export type { FeaturePath };

export type DriftState =
  | "NORMAL"
  | "MONITOR"
  | "DEGRADED"
  | "LEARNING_RESTRICTED"
  | "FROZEN";

export interface PredictionRecord {
  predictionId: string;
  sourceRoundId: string | null;
  targetRoundId: string;
  createdAt: string;
  targetStartedAt: string | null;
  targetEndedAt: string | null;
  /** P_raw — model output before calibration. */
  rawProbability: number;
  /** P_calibrated — after calibration shrinkage (pre-pipeline). */
  calibratedProbability: number | null;
  /** P_pipeline — pipeline output (pre-sheath/risk). */
  pipelineProbability: number | null;
  /** P_final — final emitted probability. */
  finalProbability: number;
  confidence: number;
  target: number;
  regime: string | null;
  modelVersion: string | null;
  featureVersion: string | null;
  featurePath: FeaturePath | null;
  temporalValidity: TemporalValidity;
  /** Problem 8: full provenance versions. */
  provenance: {
    stateVersion?: string | number | null;
    acieStateVersion?: string | number | null;
    calibrationVersion?: string | number | null;
    pipelineVersion?: string | number | null;
    regimeVersion?: string | number | null;
  };
  /** Problem 9: decision-stage outcomes (where did the prediction go wrong?). */
  stages: {
    acieSignal: boolean | null;
    calibrationApplied: boolean | null;
    pipelineApplied: boolean | null;
    opportunityScore?: number | null;
    riskApproved?: boolean | null;
    riskRejectionReason?: string | null;
    sheathBlocked?: boolean | null;
    finalSignal: boolean | null;
  };
  /** NO_BET tracking: explicit decision state from the canonical gate. */
  decision?: 'ENTRY' | 'REDUCED_ENTRY' | 'SKIP' | 'NO_BET';
  /** Whether this prediction was actionable (not NO_BET/SKIP). */
  actionable: boolean;
  resolved: boolean;
  resolvedAt?: string;
  actualMultiplier?: number;
  result?: "WIN" | "LOSS" | "NO_BET";
}

function temporalValidityOf(
  createdAt: string,
  targetStartedAt: string | null,
): TemporalValidity {
  if (targetStartedAt == null) return "TEMPORALLY_UNVERIFIED";
  const created = new Date(createdAt).getTime();
  const started = new Date(targetStartedAt).getTime();
  if (!Number.isFinite(created) || !Number.isFinite(started)) {
    return "TEMPORALLY_UNVERIFIED";
  }
  // Hard gate: prediction must exist strictly before the target round starts.
  return created < started ? "TEMPORALLY_VALID" : "TEMPORALLY_INVALID";
}

const MAX_RECORDS = 2_000;

class PredictionRegistry {
  private byTarget = new Map<string, PredictionRecord>();
  private byId = new Map<string, PredictionRecord>();

  register(rec: PredictionRecord): PredictionRecord {
    rec.temporalValidity = temporalValidityOf(rec.createdAt, rec.targetStartedAt);
    // Set actionable based on decision
    rec.actionable = rec.decision !== 'NO_BET' && rec.decision !== 'SKIP';
    this.byTarget.set(rec.targetRoundId, rec);
    this.byId.set(rec.predictionId, rec);
    this.prune();
    return rec;
  }

  /** BG / round-start authoritative backfill. Re-evaluates temporal validity. */
  noteTargetStarted(targetRoundId: string, startedAt: string): PredictionRecord | null {
    const rec = this.byTarget.get(targetRoundId);
    if (!rec) return null;
    rec.targetStartedAt = startedAt;
    rec.temporalValidity = temporalValidityOf(rec.createdAt, startedAt);
    return rec;
  }

  noteTargetEnded(targetRoundId: string, endedAt: string): void {
    const rec = this.byTarget.get(targetRoundId);
    if (rec) rec.targetEndedAt = endedAt;
  }

  getByTarget(targetRoundId: string): PredictionRecord | null {
    return this.byTarget.get(targetRoundId) ?? null;
  }

  getByPredictionId(predictionId: string): PredictionRecord | null {
    return this.byId.get(predictionId) ?? null;
  }

  /** Resolve the exact prediction for a finished round. */
  resolve(
    targetRoundId: string,
    actualMultiplier: number,
    target = 1.3,
  ): { record: PredictionRecord; result: "WIN" | "LOSS" | "NO_BET"; probability: number } | null {
    const rec = this.byTarget.get(targetRoundId);
    if (!rec || rec.resolved) return null;
    rec.resolved = true;
    rec.resolvedAt = new Date().toISOString();
    rec.actualMultiplier = actualMultiplier;
    // NO_BET predictions should not be graded as WIN/LOSS
    if (rec.decision === 'NO_BET' || rec.decision === 'SKIP') {
      rec.result = 'NO_BET';
    } else {
      rec.result = actualMultiplier >= target ? "WIN" : "LOSS";
    }
    return { record: rec, result: rec.result, probability: rec.finalProbability };
  }

  /** Records that were never resolved — prune candidates. */
  private prune(): void {
    if (this.byTarget.size <= MAX_RECORDS) return;
    const cutoff = Date.now() - 2 * 60 * 60_000; // 2h
    for (const [k, v] of this.byTarget) {
      if (v.resolved || (v.createdAt && new Date(v.createdAt).getTime() < cutoff)) {
        this.byId.delete(v.predictionId);
        this.byTarget.delete(k);
      }
    }
  }

  stats(): { registered: number; temporallyInvalid: number; unresolved: number } {
    let invalid = 0;
    let unresolved = 0;
    for (const r of this.byTarget.values()) {
      if (r.temporalValidity === "TEMPORALLY_INVALID") invalid += 1;
      if (!r.resolved) unresolved += 1;
    }
    return { registered: this.byTarget.size, temporallyInvalid: invalid, unresolved };
  }
}

export const globalPredictionRegistry = new PredictionRegistry();

// ─── Rolling performance + drift controller (Problems 2 & 5) ───────────────

export interface WindowStats {
  window: number;
  n: number;
  winRate: number | null;
  brier: number | null;
  logLoss: number | null;
  ece: number | null;
  avgConfidence: number | null;
  avgPredicted: number | null;
  actualHitRate: number | null;
}

interface Sample {
  probability: number;
  confidence: number | null;
  won: boolean;
  at: number;
}

function logLossFn(p: number, y: 0 | 1): number {
  const eps = 1e-7;
  const q = Math.min(1 - eps, Math.max(eps, p));
  return y === 1 ? -Math.log(q) : -Math.log(1 - q);
}

function eceOf(samples: Sample[]): number | null {
  if (samples.length === 0) return null;
  const bins = 10;
  const num = new Array<number>(bins).fill(0);
  const den = new Array<number>(bins).fill(0);
  for (const s of samples) {
    const b = Math.min(bins - 1, Math.max(0, Math.floor(s.probability * bins)));
    num[b] += s.won ? 1 : 0;
    den[b] += 1;
  }
  let e = 0;
  const total = samples.length;
  for (let i = 0; i < bins; i += 1) {
    if (den[i] === 0) continue;
    e += (den[i]! / total) * Math.abs(num[i]! / den[i]! - i / bins + 0.5 / bins);
  }
  return e;
}

export class RollingPerformance {
  private samples: Sample[] = [];
  private readonly maxSamples = 500;
  private drift: DriftState = "NORMAL";
  private lastTransitionAt = Date.now();

  constructor(
    private readonly windows: number[] = [25, 50, 100, 250],
    /** Baseline Brier for a random/no-edge predictor at ~65% hit rate. */
    private readonly baselineBrier = 0.2275,
  ) {}

  observe(probability: number, won: boolean, confidence: number | null = null): void {
    this.samples.push({ probability, confidence, won, at: Date.now() });
    if (this.samples.length > this.maxSamples) this.samples.shift();
    this.updateDrift();
  }

  stats(): WindowStats[] {
    return this.windows.map((w) => {
      const s = this.samples.slice(-w);
      if (s.length === 0) {
        return {
          window: w, n: 0, winRate: null, brier: null, logLoss: null,
          ece: null, avgConfidence: null, avgPredicted: null, actualHitRate: null,
        };
      }
      const n = s.length;
      const winRate = s.filter((x) => x.won).length / n;
      const brier = s.reduce((a, x) => a + (x.probability - (x.won ? 1 : 0)) ** 2, 0) / n;
      const ll = s.reduce((a, x) => a + logLossFn(x.probability, x.won ? 1 : 0), 0) / n;
      const avgConfidence = s.some((x) => x.confidence != null)
        ? s.reduce((a, x) => a + (x.confidence ?? 0), 0) / n
        : null;
      const avgPredicted = s.reduce((a, x) => a + x.probability, 0) / n;
      return {
        window: w, n, winRate, brier, logLoss: ll, ece: eceOf(s),
        avgConfidence, avgPredicted, actualHitRate: winRate,
      };
    });
  }

  /** The primary window used for drift decisions (100 rounds). */
  primaryStats(): WindowStats | null {
    return this.stats().find((s) => s.window === 100) ?? null;
  }

  getDriftState(): DriftState {
    return this.drift;
  }

  shouldAllowLearning(): boolean {
    return this.drift !== "LEARNING_RESTRICTED" && this.drift !== "FROZEN";
  }

  /**
   * Drift controller: statistically significant deterioration over the 100-
   * round window (not just streak length). Hysteresis: recovery requires the
   * window to look sane again and at least 5 minutes between transitions.
   */
  private updateDrift(): void {
    const s = this.samples.slice(-100);
    if (s.length < 25) return; // not enough evidence — stay NORMAL
    const winRate = s.filter((x) => x.won).length / s.length;
    const brier = s.reduce((a, x) => a + (x.probability - (x.won ? 1 : 0)) ** 2, 0) / s.length;
    // SE of win rate under null (p=0.65 fair-ish baseline at target 1.3)
    const p0 = 0.65;
    const se = Math.sqrt((p0 * (1 - p0)) / s.length);
    const z = (winRate - p0) / se;
    const brierGap = brier - this.baselineBrier;

    let next: DriftState = this.drift;
    if (z < -3 || brierGap > 0.08) next = "FROZEN";
    else if (z < -2 || brierGap > 0.05) next = "LEARNING_RESTRICTED";
    else if (z < -1.5 || brierGap > 0.03) next = "DEGRADED";
    else if (z < -1 || brierGap > 0.015) next = "MONITOR";
    else next = "NORMAL";

    // Deterioration escalates immediately; recovery is gradual (one step at a
    // time, min 5 minutes in state).
    const now = Date.now();
    const order: DriftState[] = ["NORMAL", "MONITOR", "DEGRADED", "LEARNING_RESTRICTED", "FROZEN"];
    const cur = order.indexOf(this.drift);
    const nxt = order.indexOf(next);
    if (nxt < cur) {
      if (now - this.lastTransitionAt < 5 * 60_000) return;
      next = order[cur - 1]!; // gradual release
    }
    if (next !== this.drift) {
      this.lastTransitionAt = now;
      this.drift = next;
    }
  }
}

export const globalRollingPerformance = new RollingPerformance();
