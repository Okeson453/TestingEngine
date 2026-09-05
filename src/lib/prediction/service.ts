import { randomUUID } from "node:crypto";
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

export interface LastRoundSnapshot {
  gameId: string;
  multiplier: number;
  crashedAt: string;
}

async function loadLastRoundSnapshot(sql: Sql): Promise<LastRoundSnapshot | null> {
  const rows = await sql<{
    game_id: string;
    multiplier: string | number;
    crashed_at: string | Date;
  }>`
    select game_id, multiplier, crashed_at
    from crash_rounds
    order by crashed_at desc, game_id desc
    limit 1
  `;
  if (rows.length === 0) return null;
  const r = rows[0]!;
  const crashedAt =
    r.crashed_at instanceof Date ? r.crashed_at.toISOString() : String(r.crashed_at);
  return {
    gameId: r.game_id,
    multiplier: Number(r.multiplier),
    crashedAt,
  };
}

// (Removed: loadRecentRoundsForPrediction — superseded by the strict
// `crashed_at < $beginTime` window in @/lib/prediction/live/predictor.ts.)

/**
 * Returned to callers (the worker) so they can build a richer notification
 * payload (the `lastRound` is the most recently-resolved round at the moment
 * of generation — the "Last round" line in the Telegram prediction message).
 */
export interface QueuedPrediction {
  signal: PredictionSignal;
  lastRound: LastRoundSnapshot | null;
}

/**
 * Generate and queue a prediction for the next round.
 *
 * Spec context: the operator-supplied diagnosis identified that the legacy
 * `targetRoundId: "next"` string literal is a label with no DB binding. This
 * rewrite computes the actual target explicitly:
 *
 *   target_game_id = MAX(crash_rounds.game_id) + 1
 *
 * …and rejects generation when the target has already crashed or when an
 * active prediction is already queued for it. The new
 * `pending_predictions_active_target_uidx` partial UNIQUE index is the
 * safety net for concurrent workers.
 */
export async function generateAndQueuePrediction(
  sql: Sql,
): Promise<QueuedPrediction | null> {
  // 1. Determine the next round id: MAX(game_id) + 1.
  const maxRows = await sql<{ max_id: string | null }>`
    SELECT MAX(game_id) as max_id FROM crash_rounds
  `;
  const maxId = maxRows[0]?.max_id ?? "0";
  const targetGameId = (BigInt(maxId) + 1n).toString();

  // 2. Temporal safety: the target must not already exist as a crashed
  //    round. If it does, BC.Game has already moved past it and we
  //    cannot predict it.
  const existingTarget = await sql<{ game_id: string }>`
    SELECT game_id FROM crash_rounds WHERE game_id = ${targetGameId} LIMIT 1
  `;
  if (existingTarget.length > 0) return null;

  // 3. Idempotency: at most one active prediction per target round. The
  //    partial UNIQUE index is the canonical gate; this SELECT is the
  //    fast-path that lets us skip the model call when one already exists.
  const existingPending = await sql<{ prediction_id: string }>`
    SELECT prediction_id FROM pending_predictions
    WHERE target_game_id = ${targetGameId} AND status = 'PENDING'
    LIMIT 1
  `;
  if (existingPending.length > 0) return null;

  // 4. Freshness guard: history excludes the target itself and any
  //    future round. `crashed_at <= now()` AND `game_id < target`
  //    guarantees the strict causal window — the model never sees
  //    the target round's own multiplier.
  const [rounds, lastRound] = await Promise.all([
    sql<{
      game_id: string;
      multiplier: string | number;
      began_at: string | Date | null;
      crashed_at: string | Date;
    }>`
      SELECT game_id, multiplier, began_at, crashed_at
      FROM crash_rounds
      WHERE crashed_at <= now() AND game_id < ${targetGameId}
      ORDER BY crashed_at DESC, game_id DESC LIMIT 100
    `,
    loadLastRoundSnapshot(sql),
  ]);

  if (rounds.length < MIN_HISTORY) return null;

  const engine = new PredictionEngine();
  const timestamp = new Date().toISOString();
  const signal = engine.predict({
    priorRounds: rounds.reverse().map((r) =>
      mapCrashRoundToHistorical({
        gameId: r.game_id,
        multiplier: Number(r.multiplier),
        hash: null,
        salt: null,
        beganAt:
          r.began_at instanceof Date ? r.began_at.toISOString() : r.began_at,
        crashedAt:
          r.crashed_at instanceof Date
            ? r.crashed_at.toISOString()
            : String(r.crashed_at),
      }),
    ),
    targetRoundId: targetGameId,
    timestamp,
    target: DEFAULT_TARGET,
  });

  await sql`
    INSERT INTO pending_predictions (
      prediction_id, target_multiplier, probability, confidence,
      regime_name, regime_confidence, reasoning, feature_summary,
      model_version, requested_at, target_game_id, source_round_id,
      target_round_started_at, correlation_id, generated_at, status
    ) VALUES (
      ${signal.predictionId}, ${DEFAULT_TARGET}, ${signal.probability},
      ${signal.confidence}, ${signal.regimeId}, ${signal.regimeId ? 0.5 : null},
      ${signal.reasoning}, ${JSON.stringify(signal.featureSummary)},
      ${signal.modelVersion}, ${timestamp}, ${targetGameId}, ${maxId},
      ${timestamp}, ${randomUUID()}, ${timestamp}, 'PENDING'
    )
    ON CONFLICT (target_game_id) WHERE status = 'PENDING' DO NOTHING
  `;

  return { signal, lastRound };
}

// (Removed: generatePredictionForTargetRound and predictionExistsForTarget.
// The event-driven path now lives in @/lib/prediction/live/predictor.ts
// (onGameStart) and @/lib/prediction/live/validator.ts (onGameEnd). The
// legacy `predictionTime >= targetStartTime` throw was a no-op in the
// real system — see TestingEngine_Deep_Diagnosis.md §0 row 3.)

/**
 * Outcome of one validation pass: how many predictions were resolved against
 * how many newly-inserted rounds in this cycle, plus the durable game_id
 * mapping the worker wrote (one row per (prediction_id, game_id) pair).
 *
 * `pairs` is rich enough for the worker to build a Telegram validation
 * notification without any additional DB query — `targetMultiplier`/`probability`
 * are present for normal matches; on the worker-crash recovery re-pass,
 * `targetMultiplier === 0` and the worker skips notification (the dashboard
 * already shows the historical WIN/LOSS, and re-notifying would be noise).
 */
export interface ValidationOutcome {
  resolved: number;
  insertedRounds: number;
  pairs: Array<{
    predictionId: string;
    gameId: string;
    result: "WIN" | "LOSS";
    targetMultiplier: number;
    actualMultiplier: number;
    probability: number;
    resolvedAt: string;
  }>;
}

/**
 * Pair newly-inserted rounds with the oldest unmatched pending predictions,
 * writing WIN/LOSS deterministically. Enforces the durable 1:1:1 invariant:
 *
 *   exactly 1 prediction  ↔  exactly 1 target game_id  ↔  exactly 1 validation
 *
 *   * Each newly-inserted round is validated at most once
 *     (UNIQUE(prediction_validations.game_id) — see 0007).
 *   * Each pending prediction is validated at most once
 *     (UNIQUE(prediction_validations.prediction_id) — already in 0005,
 *      plus `pending_predictions.target_game_id` is set as the durable anchor).
 *   * The pairing is oldest-pending ↔ oldest-new-round, so cycles that
 *     discover N rounds in one poll resolve N predictions (capped by the
 *     number of unmatched pendings).  Any leftover rounds simply wait for
 *     the next cycle / next prediction generation.
 *   * The whole pass is idempotent: a re-run after a worker crash leaves
 *     already-resolved rows untouched (ON CONFLICT DO NOTHING).
 */
export async function validateAgainstNewRounds(
  insertedRounds: CrashRound[],
): Promise<ValidationOutcome> {
  const outcome: ValidationOutcome = { resolved: 0, insertedRounds: 0, pairs: [] };
  if (insertedRounds.length === 0) return outcome;
  const sql = await getSql();

  const sortedNew = [...insertedRounds].sort((a, b) => {
    if (a.crashedAt === b.crashedAt) return a.gameId.localeCompare(b.gameId);
    return a.crashedAt < b.crashedAt ? -1 : 1;
  });
  outcome.insertedRounds = sortedNew.length;
  const gameIds = sortedNew.map((r) => r.gameId);

  // Batch preload — eliminates N+1 (3 queries per round → 2 queries total).
  const existingRows = await sql<{
    game_id: string;
    prediction_id: string;
    result: string;
    predicted_probability: string | number | null;
    resolved_at: string | Date;
  }>`
    select game_id, prediction_id, result, predicted_probability, resolved_at
    from prediction_validations
    where game_id = any(${gameIds}::text[])
  `;
  const existingByGame = new Map(existingRows.map((r) => [r.game_id, r]));

  const pendingRows = await sql<{
    prediction_id: string;
    target_game_id: string;
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
    select prediction_id, target_game_id, target_multiplier, probability, confidence,
           regime_name, regime_confidence, reasoning, feature_summary,
           model_version, requested_at
    from pending_predictions
    where target_game_id = any(${gameIds}::text[])
      and status = 'PENDING'
  `;
  const pendingByGame = new Map(pendingRows.map((r) => [r.target_game_id, r]));

  for (const targetRound of sortedNew) {
    const existing = existingByGame.get(targetRound.gameId);
    if (existing) {
      outcome.resolved += 1;
      outcome.pairs.push({
        predictionId: existing.prediction_id,
        gameId: targetRound.gameId,
        result: existing.result as "WIN" | "LOSS",
        targetMultiplier: 0,
        actualMultiplier: targetRound.multiplier,
        probability: Number(existing.predicted_probability ?? 0),
        resolvedAt:
          existing.resolved_at instanceof Date
            ? existing.resolved_at.toISOString()
            : String(existing.resolved_at),
      });
      continue;
    }

    const p = pendingByGame.get(targetRound.gameId);
    if (!p) continue;

    const actualMultiplier = targetRound.multiplier;
    const result = actualMultiplier >= p.target_multiplier ? "WIN" : "LOSS";
    const now = new Date().toISOString();

    await sql`
      update pending_predictions
         set status = 'MATCHED', matched = true,
             matched_game_id = ${targetRound.gameId},
             matched_at = ${now}
       where prediction_id = ${p.prediction_id}
         and status = 'PENDING'
    `;

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
        ${p.requested_at}, ${now}
      )
      on conflict on constraint prediction_validations_prediction_id_key do nothing
    `;

    // Online learning feedback (aligned with live validator path)
    try {
      const actual: 0 | 1 = result === "WIN" ? 1 : 0;
      const predicted = Number(p.probability);
      const { feedbackPredictionPipeline } = await import(
        "@/lib/prediction/prediction-pipeline"
      );
      feedbackPredictionPipeline(predicted, actual);
      const { globalModelPerformance } = await import(
        "@/lib/prediction/ensemble/model-performance"
      );
      globalModelPerformance.observe(String(p.model_version ?? "baseline"), predicted, actual);
      globalModelPerformance.observe("live", predicted, actual);
    } catch {
      /* soft */
    }

    outcome.resolved += 1;
    outcome.pairs.push({
      predictionId: p.prediction_id,
      gameId: targetRound.gameId,
      result,
      targetMultiplier: Number(p.target_multiplier),
      actualMultiplier,
      probability: Number(p.probability),
      resolvedAt: now,
    });
  }

  return outcome;
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