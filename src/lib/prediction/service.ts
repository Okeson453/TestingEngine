import { getSql } from "@/lib/db";

// NOTE: validateAgainstNewRounds / generateAndQueuePrediction / pendingByGame
// and their supporting helpers were removed (second-opinion audit rec 3):
// runtime-unreachable dead code — the live path is the predictor/validator
// pipeline in @/lib/prediction/live/. Historical fix coverage lived in
// fix-verification.test.ts, removed alongside (see commit history 8f4a26d
// for the original fixes).

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

/** Max rows for a single export download (safety cap). */
export const HISTORY_EXPORT_MAX = 50_000;

/**
 * Fetch the full validation history (all pages) for CSV/JSON download.
 * Honours the same filters as getValidationHistory. Capped at HISTORY_EXPORT_MAX.
 */
export async function getAllValidationHistory(
  opts: Omit<ValidationHistoryOpts, "page" | "pageSize"> = {},
): Promise<{ records: ValidationRecord[]; total: number; truncated: boolean }> {
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
    limit $${paramIdx++}
  `;
  params.push(HISTORY_EXPORT_MAX);

  const [countRows, dataRows] = await Promise.all([
    sql.query<{ total: number }>(countQuery, params.slice(0, paramIdx - 2)),
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
    requestedAt:
      r.requested_at instanceof Date
        ? r.requested_at.toISOString()
        : String(r.requested_at),
    resolvedAt:
      r.resolved_at instanceof Date
        ? r.resolved_at.toISOString()
        : String(r.resolved_at),
  }));

  return {
    records,
    total,
    truncated: total > records.length,
  };
}

/** Serialize validation records to CSV (UTF-8 with header). */
export function validationRecordsToCsv(records: ValidationRecord[]): string {
  const header = [
    "prediction_id",
    "game_id",
    "result",
    "target_multiplier",
    "actual_multiplier",
    "predicted_probability",
    "predicted_confidence",
    "model_version",
    "regime_name",
    "requested_at",
    "resolved_at",
  ];
  const escape = (v: string | number | null | undefined): string => {
    if (v == null) return "";
    const s = String(v);
    if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const lines = [header.join(",")];
  for (const r of records) {
    lines.push(
      [
        escape(r.predictionId),
        escape(r.gameId),
        escape(r.result),
        escape(r.targetMultiplier),
        escape(r.actualMultiplier),
        escape(r.predictedProbability),
        escape(r.predictedConfidence),
        escape(r.modelVersion),
        escape(r.regimeName),
        escape(r.requestedAt),
        escape(r.resolvedAt),
      ].join(","),
    );
  }
  return lines.join("\n");
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