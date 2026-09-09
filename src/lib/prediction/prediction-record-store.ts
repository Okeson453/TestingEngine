/**
 * Durable-in-process prediction identity for feedback lineage.
 * Feedback resolves by targetRoundId — never by lastEmittedProbability alone.
 */

export type TemporalValidity = "TEMPORALLY_VALID" | "TEMPORALLY_INVALID" | "TEMPORALLY_UNVERIFIED";

export interface PredictionRecord {
  predictionId: string;
  sourceRoundId: string | null;
  targetRoundId: string;
  createdAt: string;
  createdAtMs: number;
  targetStartedAt: string | null;
  targetEndedAt: string | null;
  rawProbability: number;
  calibratedProbability: number;
  pipelineProbability: number;
  finalProbability: number;
  regime: string;
  modelVersion: string;
  featureVersion: string;
  acieStateVersion?: string | null;
  calibrationVersion?: string | null;
  pipelineVersion?: string | null;
  temporalValidity: TemporalValidity;
  resolved: boolean;
  actualHit?: 0 | 1;
  resolvedAt?: string;
}

const byTarget = new Map<string, PredictionRecord>();
const byId = new Map<string, PredictionRecord>();
const MAX = 500;

function prune(): void {
  if (byTarget.size <= MAX) return;
  const cutoff = Date.now() - 2 * 60 * 60_000;
  for (const [k, v] of byTarget) {
    if (v.resolved || v.createdAtMs < cutoff) {
      byTarget.delete(k);
      byId.delete(v.predictionId);
    }
  }
}

export function registerPrediction(rec: PredictionRecord): PredictionRecord {
  prune();
  // Prefer first claim for a target (ED-first); later overwrites only if unresolved
  const existing = byTarget.get(rec.targetRoundId);
  if (existing && !existing.resolved) {
    return existing;
  }
  byTarget.set(rec.targetRoundId, rec);
  byId.set(rec.predictionId, rec);
  return rec;
}

export function getPredictionForTarget(targetRoundId: string): PredictionRecord | null {
  return byTarget.get(targetRoundId) ?? null;
}

export function getPredictionById(predictionId: string): PredictionRecord | null {
  return byId.get(predictionId) ?? null;
}

export function bindTargetStarted(targetRoundId: string, startedAt: string): void {
  const rec = byTarget.get(targetRoundId);
  if (!rec || rec.targetStartedAt) return;
  rec.targetStartedAt = startedAt;
  const startMs = new Date(startedAt).getTime();
  if (Number.isFinite(startMs) && rec.createdAtMs >= startMs) {
    rec.temporalValidity = "TEMPORALLY_INVALID";
  } else if (Number.isFinite(startMs) && rec.createdAtMs < startMs) {
    rec.temporalValidity = "TEMPORALLY_VALID";
  }
}

export function resolvePrediction(
  targetRoundId: string,
  crashPoint: number,
  endedAt?: string,
): PredictionRecord | null {
  const rec = byTarget.get(targetRoundId);
  if (!rec || rec.resolved) return rec ?? null;
  const actual: 0 | 1 = crashPoint >= 1.3 ? 1 : 0;
  rec.actualHit = actual;
  rec.resolved = true;
  rec.resolvedAt = endedAt ?? new Date().toISOString();
  rec.targetEndedAt = rec.resolvedAt;
  return rec;
}

export function checkTemporalValidity(
  createdAtMs: number,
  targetStartedAt: string | null | undefined,
): TemporalValidity {
  if (targetStartedAt == null || targetStartedAt === "") return "TEMPORALLY_UNVERIFIED";
  const startMs = new Date(targetStartedAt).getTime();
  if (!Number.isFinite(startMs)) return "TEMPORALLY_UNVERIFIED";
  return createdAtMs < startMs ? "TEMPORALLY_VALID" : "TEMPORALLY_INVALID";
}

/** Test helper */
export function _resetPredictionRecordStore(): void {
  byTarget.clear();
  byId.clear();
}
