import { getSql } from "@/lib/db";
import { PredictionEngine } from "./prediction-engine.ts";
import type {
  PredictionSignal,
  HistoricalRound,
  ThresholdTarget,
} from "./types.ts";
import type { CrashRound } from "@/lib/crash/types";
import type { Sql } from "@/lib/db";

const DEFAULT_TARGET: ThresholdTarget = 1.3;
const MIN_HISTORY = 20;
const MAX_HISTORY = 100;

function mapCrashRoundToHistorical(r: CrashRound): HistoricalRound {
  return {
    id: r.gameId,
    externalRoundId: r.gameId,
    sessionId: null,
    startedAt: r.beganAt,
    crashedAt: r.crashedAt,
    crashPoint: r.multiplier,
    observationSource: "bc-game-api",
    dataQuality: "high",
    createdAt: r.crashedAt,
  };
}

async function loadRecentRoundsForPrediction(
  sql: Sql,
  limit = MAX_HISTORY,
): Promise<HistoricalRound[]> {
  const rows = await sql<{
    game_id: string;
    multiplier: string | number;
    began_at: string | Date | null;
    crashed_at: string | Date;
  }>`
    select game_id, multiplier, began_at, crashed_at
    from crash_rounds
    order by crashed_at desc, game_id desc
    limit ${limit}
  `;
  return rows.reverse().map((row) =>
    mapCrashRoundToHistorical({
      gameId: row.game_id,
      multiplier: Number(row.multiplier),
      hash: null,
      salt: null,
      beganAt:
        row.began_at instanceof Date
          ? row.began_at.toISOString()
          : row.began_at,
      crashedAt:
        row.crashed_at instanceof Date
          ? row.crashed_at.toISOString()
          : row.crashed_at,
    }),
  );
}

export async function generateAndQueuePrediction(): Promise<PredictionSignal | null> {
  const sql = await getSql();
  const rounds = await loadRecentRoundsForPrediction(sql);
  if (rounds.length < MIN_HISTORY) return null;

  const engine = new PredictionEngine();
  const timestamp = new Date().toISOString();
  const signal = engine.predict({
    priorRounds: rounds,
    targetRoundId: "next",
    timestamp,
    target: DEFAULT_TARGET,
  });

  await sql`
    insert into pending_predictions (
      prediction_id, target_multiplier, probability, confidence,
      regime_name, regime_confidence, reasoning, feature_summary,
      model_version, requested_at
    ) values (
      ${signal.predictionId}, ${DEFAULT_TARGET}, ${signal.probability},
      ${signal.confidence}, ${signal.regimeId}, ${signal.regimeId ? 0.5 : null},
      ${signal.reasoning}, ${JSON.stringify(signal.featureSummary)},
      ${signal.modelVersion}, ${timestamp}
    )
  `;
  return signal;
}

export async function validateAgainstNewRounds(
  insertedRounds: CrashRound[],
): Promise<number> {
  if (insertedRounds.length === 0) return 0;
  const sql = await getSql();

  const targetRound = insertedRounds[0];

  const pending = await sql<{
    prediction_id: string;
    target_multiplier: number;
    probability: number;
    confidence: number;
    regime_name: string | null;
    regime_confidence: number | null;
    reasoning: string[];
    feature_summary: unknown;
    model_version: string;
    requested_at: string;
  }>`
    select prediction_id, target_multiplier, probability, confidence,
           regime_name, regime_confidence, reasoning, feature_summary,
           model_version, requested_at
    from pending_predictions
    where matched = false
    order by requested_at asc
    limit 1
  `;

  if (pending.length === 0) return 0;
  const p = pending[0];
  const actualMultiplier = targetRound.multiplier;
  const result = actualMultiplier >= p.target_multiplier ? "WIN" : "LOSS";

  await sql`
    insert into prediction_validations (
      prediction_id, game_id, target_multiplier, predicted_probability,
      predicted_confidence, actual_multiplier, result, model_version,
      regime_name, regime_confidence, reasoning, feature_summary,
      requested_at, resolved_at
    ) values (
      ${p.prediction_id}, ${targetRound.gameId}, ${p.target_multiplier},
      ${p.probability}, ${p.confidence}, ${actualMultiplier}, ${result},
      ${p.model_version}, ${p.regime_name}, ${p.regime_confidence},
      ${p.reasoning}, ${JSON.stringify(p.feature_summary)},
      ${p.requested_at}, ${new Date().toISOString()}
    )
    on conflict (prediction_id) do nothing
  `;

  await sql`
    update pending_predictions
    set matched = true, matched_game_id = ${targetRound.gameId}, matched_at = ${new Date().toISOString()}
    where prediction_id = ${p.prediction_id}
  `;

  return 1;
}

export interface DailyTarget {
  dailyTarget: number;
  updatedAt: string;
}

export async function getDailyTarget(): Promise<DailyTarget> {
  const sql = await getSql();
  const rows = await sql<{ daily_target: number; updated_at: string }>`
    select daily_target, updated_at from validation_config limit 1
  `;
  const r = rows[0];
  return {
    dailyTarget: r?.daily_target ?? 100,
    updatedAt: r?.updated_at ?? new Date().toISOString(),
  };
}

export async function setDailyTarget(target: number): Promise<DailyTarget> {
  const n = Math.max(20, Math.min(500, target));
  const sql = await getSql();
  const rows = await sql<{ daily_target: number; updated_at: string }>`
    update validation_config
    set daily_target = ${n}, updated_at = now()
    returning daily_target, updated_at
  `;
  const r = rows[0];
  return {
    dailyTarget: r?.daily_target ?? n,
    updatedAt: r?.updated_at ?? new Date().toISOString(),
  };
}

export interface TodayStats {
  total: number;
  wins: number;
  losses: number;
  winRate: number;
  lossRate: number;
  remaining: number;
}

export async function getTodayStats(): Promise<TodayStats> {
  const sql = await getSql();
  const [statsRows, targetRows] = await Promise.all([
    sql<{ result: string; count: number }>`
      select result, count(*)::int as count
      from prediction_validations
      where resolved_at::date = current_date
      group by result
    `,
    sql<{ daily_target: number }>`
      select daily_target from validation_config limit 1
    `,
  ]);
  const wins = statsRows.find((r) => r.result === "WIN")?.count ?? 0;
  const losses = statsRows.find((r) => r.result === "LOSS")?.count ?? 0;
  const total = wins + losses;
  const dailyTarget = targetRows[0]?.daily_target ?? 100;
  return {
    total,
    wins,
    losses,
    winRate: total === 0 ? 0 : wins / total,
    lossRate: total === 0 ? 0 : losses / total,
    remaining: Math.max(0, dailyTarget - total),
  };
}

export interface LifetimeStats {
  total: number;
  wins: number;
  losses: number;
  winRate: number;
  lossRate: number;
}

export async function getLifetimeStats(): Promise<LifetimeStats> {
  const sql = await getSql();
  const rows = await sql<{ result: string; count: number }>`
    select result, count(*)::int as count
    from prediction_validations
    group by result
  `;
  const wins = rows.find((r) => r.result === "WIN")?.count ?? 0;
  const losses = rows.find((r) => r.result === "LOSS")?.count ?? 0;
  const total = wins + losses;
  return {
    total,
    wins,
    losses,
    winRate: total === 0 ? 0 : wins / total,
    lossRate: total === 0 ? 0 : losses / total,
  };
}

export interface StreakSnapshot {
  currentKind: "WIN" | "LOSS" | "none";
  currentCount: number;
  maxWin: number;
  maxLoss: number;
}

export async function getStreaks(): Promise<StreakSnapshot> {
  const sql = await getSql();
  const rows = await sql<{ result: string }>`
    select result
    from prediction_validations
    order by resolved_at desc, id desc
    limit 5000
  `;
  const results = rows.map((r) => r.result);
  if (results.length === 0) {
    return { currentKind: "none", currentCount: 0, maxWin: 0, maxLoss: 0 };
  }
  const first = results[0];
  let currentCount = 0;
  for (const r of results) {
    if (r === first) currentCount++;
    else break;
  }
  let maxWin = 0;
  let maxLoss = 0;
  let runWin = 0;
  let runLoss = 0;
  for (let i = results.length - 1; i >= 0; i--) {
    if (results[i] === "WIN") {
      runWin++;
      runLoss = 0;
      if (runWin > maxWin) maxWin = runWin;
    } else {
      runLoss++;
      runWin = 0;
      if (runLoss > maxLoss) maxLoss = runLoss;
    }
  }
  return {
    currentKind: first as "WIN" | "LOSS",
    currentCount,
    maxWin,
    maxLoss,
  };
}

export interface ValidationRecord {
  predictionId: string;
  gameId: string;
  targetMultiplier: number;
  predictedProbability: number;
  predictedConfidence: number;
  actualMultiplier: number;
  result: "WIN" | "LOSS";
  modelVersion: string;
  regimeName: string | null;
  requestedAt: string;
  resolvedAt: string;
}

export async function getRecentValidations(
  limit = 10,
): Promise<ValidationRecord[]> {
  const sql = await getSql();
  const rows = await sql<{
    prediction_id: string;
    game_id: string;
    target_multiplier: number;
    predicted_probability: number;
    predicted_confidence: number;
    actual_multiplier: number;
    result: string;
    model_version: string;
    regime_name: string | null;
    requested_at: string;
    resolved_at: string;
  }>`
    select prediction_id, game_id, target_multiplier, predicted_probability,
           predicted_confidence, actual_multiplier, result, model_version,
           regime_name, requested_at, resolved_at
    from prediction_validations
    order by resolved_at desc, id desc
    limit ${limit}
  `;
  return rows.map((r) => ({
    predictionId: r.prediction_id,
    gameId: r.game_id,
    targetMultiplier: Number(r.target_multiplier),
    predictedProbability: Number(r.predicted_probability),
    predictedConfidence: Number(r.predicted_confidence),
    actualMultiplier: Number(r.actual_multiplier),
    result: r.result as "WIN" | "LOSS",
    modelVersion: r.model_version,
    regimeName: r.regime_name,
    requestedAt: r.requested_at,
    resolvedAt: r.resolved_at,
  }));
}

export interface ValidationHistoryOpts {
  page?: number;
  pageSize?: number;
  result?: "WIN" | "LOSS" | null;
  fromDate?: string;
  toDate?: string;
}

export interface ValidationHistoryResult {
  records: ValidationRecord[];
  total: number;
  page: number;
  pageSize: number;
}

export async function getValidationHistory(
  opts: ValidationHistoryOpts = {},
): Promise<ValidationHistoryResult> {
  const page = Math.max(1, opts.page ?? 1);
  const pageSize = Math.max(1, Math.min(100, opts.pageSize ?? 20));
  const offset = (page - 1) * pageSize;
  const sql = await getSql();

  const conditions: string[] = [];
  const params: unknown[] = [];
  let paramIdx = 1;

  if (opts.result) {
    conditions.push(`result = $${paramIdx++}`);
    params.push(opts.result);
  }
  if (opts.fromDate) {
    conditions.push(`resolved_at::date >= $${paramIdx++}`);
    params.push(opts.fromDate);
  }
  if (opts.toDate) {
    conditions.push(`resolved_at::date <= $${paramIdx++}`);
    params.push(opts.toDate);
  }

  const where =
    conditions.length > 0 ? `where ${conditions.join(" and ")}` : "";
  const countQuery = `select count(*)::int as total from prediction_validations ${where}`;
  const dataQuery = `
    select prediction_id, game_id, target_multiplier, predicted_probability,
           predicted_confidence, actual_multiplier, result, model_version,
           regime_name, requested_at, resolved_at
    from prediction_validations ${where}
    order by resolved_at desc, id desc
    limit $${paramIdx++} offset $${paramIdx++}
  `;
  params.push(pageSize, offset);

  const [countRows, dataRows] = await Promise.all([
    sql.query<{ total: number }>(countQuery, params.slice(0, paramIdx - 3)),
    sql.query<{
      prediction_id: string;
      game_id: string;
      target_multiplier: number;
      predicted_probability: number;
      predicted_confidence: number;
      actual_multiplier: number;
      result: string;
      model_version: string;
      regime_name: string | null;
      requested_at: string;
      resolved_at: string;
    }>(dataQuery, params),
  ]);

  const total = countRows[0]?.total ?? 0;
  const records = dataRows.map((r) => ({
    predictionId: r.prediction_id,
    gameId: r.game_id,
    targetMultiplier: Number(r.target_multiplier),
    predictedProbability: Number(r.predicted_probability),
    predictedConfidence: Number(r.predicted_confidence),
    actualMultiplier: Number(r.actual_multiplier),
    result: r.result as "WIN" | "LOSS",
    modelVersion: r.model_version,
    regimeName: r.regime_name,
    requestedAt: r.requested_at,
    resolvedAt: r.resolved_at,
  }));

  return { records, total, page, pageSize };
}

export interface PendingStatus {
  hasPending: boolean;
  pendingCount: number;
  oldestPendingAt: string | null;
}

export async function getPendingStatus(): Promise<PendingStatus> {
  const sql = await getSql();
  const rows = await sql<{
    count: number;
    oldest: string | null;
  }>`
    select count(*)::int as count, min(requested_at) as oldest
    from pending_predictions
    where matched = false
  `;
  const r = rows[0];
  return {
    hasPending: (r?.count ?? 0) > 0,
    pendingCount: r?.count ?? 0,
    oldestPendingAt: r?.oldest ?? null,
  };
}
