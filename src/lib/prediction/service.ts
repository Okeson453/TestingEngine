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

async function loadRecentRoundsForPrediction(
  sql: Sql,
  limit = MAX_HISTORY,
): Promise<HistoricalRound[]> {
  // Temporal-integrity guard: only include rounds whose outcome is fully known
  // at the moment we generate the prediction (crashed_at <= now). This is the
  // existing MAX_HISTORY cap (model input budget), plus an explicit cutoff
  // that defends against any future-dated rows from the upstream ingest path.
  const rows = await sql<{
    game_id: string;
    multiplier: string | number;
    began_at: string | Date | null;
    crashed_at: string | Date;
  }>`
    select game_id, multiplier, began_at, crashed_at
    from crash_rounds
    where crashed_at <= now()
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
 * Generate and queue a prediction for a specific target round.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.10
 *
 * Shim that delegates to the new event-driven live module
 * (`onGameStart`). The legacy "next" string literal path is REMOVED —
 * all production code paths must supply a concrete `targetGameId` and
 * `targetRoundStartedAt` so the strict temporal invariant
 * `prediction_generated_at < target_round_started_at` is provable from
 * persisted data.
 */
export async function generateAndQueuePrediction(
  targetGameId: string,
  targetRoundStartedAt: string,
): Promise<QueuedPrediction | null> {
  if (!targetGameId || !targetRoundStartedAt) {
    throw new Error(
      "generateAndQueuePrediction: targetGameId and targetRoundStartedAt are required " +
        "(the legacy 'next' string literal path is removed per spec §7.10).",
    );
  }
  const { onGameStart } = await import("./live/predictor");
  const sql = await getSql();
  const lastRound = await loadLastRoundSnapshot(sql);
  const result = await onGameStart({
    gameId: targetGameId,
    beginTime: targetRoundStartedAt,
    hash: null,
    salt: null,
    sourceRoundGameId: lastRound?.gameId ?? null,
    receivedAt: new Date().toISOString(),
  });
  if (result.kind !== "predicted") {
    return null;
  }
  // Construct a minimal QueuedPrediction shape for the existing callers.
  return {
    signal: {
      predictionId: result.predictionId,
      timestamp: result.predictionGeneratedAt,
      modelVersion: "v1",
      featureVersion: "v1",
      target: 1.3 as ThresholdTarget,
      probability: 0.5,
      confidence: 0.5,
      regimeId: null,
      score: 0,
      dataQuality: "high",
      expiresAt: null,
      reasoning: [],
      featureSummary: {},
    },
    lastRound,
  };
}

/**
 * Generate a prediction specifically for event-driven approach.
 * This is the new primary method for Socket.IO triggered predictions.
 * 
 * @param targetGameId - The BC.Game round ID from the bg event
 * @param targetRoundStartedAt - The beganAt timestamp from the bg event
 * @param rounds - Historical rounds for prediction input
 */
export async function generatePredictionForTargetRound(
  targetGameId: string,
  targetRoundStartedAt: string,
  rounds: HistoricalRound[],
): Promise<QueuedPrediction | null> {
  const sql = await getSql();
  const lastRound = await loadLastRoundSnapshot(sql);
  
  if (rounds.length < MIN_HISTORY) return null;

  const engine = new PredictionEngine();
  const timestamp = new Date().toISOString();
  
  // Verify temporal invariant: prediction must be generated before target round starts
  const predictionTime = new Date(timestamp).getTime();
  const targetStartTime = new Date(targetRoundStartedAt).getTime();
  
  if (predictionTime >= targetStartTime) {
    throw new Error(
      `Temporal invariant violation: prediction_generated_at (${predictionTime}) >= ` +
      `target_round_started_at (${targetStartTime}) for game ${targetGameId}`
    );
  }

  const signal = engine.predict({
    priorRounds: rounds,
    targetRoundId: targetGameId,  // Use actual game ID
    timestamp,
    target: DEFAULT_TARGET,
  });

  // Store with target anchoring for temporal invariant
  await sql`
    insert into pending_predictions (
      prediction_id, target_multiplier, probability, confidence,
      regime_name, regime_confidence, reasoning, feature_summary,
      model_version, requested_at, target_game_id, target_round_started_at
    ) values (
      ${signal.predictionId}, ${DEFAULT_TARGET}, ${signal.probability},
      ${signal.confidence}, ${signal.regimeId}, ${signal.regimeId ? 0.5 : null},
      ${signal.reasoning}, ${JSON.stringify(signal.featureSummary)},
      ${signal.modelVersion}, ${timestamp}, ${targetGameId}, ${targetRoundStartedAt}
    )
    on conflict (prediction_id) do nothing
  `;

  return { signal, lastRound };
}

/**
 * Check if a prediction already exists for a specific target game.
 * Used to prevent duplicate predictions in the event-driven approach.
 */
export async function predictionExistsForTarget(
  sql: Sql,
  targetGameId: string,
): Promise<boolean> {
  const rows = await sql<{ count: number }>`
    select count(*)::int as count
    from pending_predictions
    where target_game_id = ${targetGameId}
  `;
  return (rows[0]?.count ?? 0) > 0;
}

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

  // Newest first → sort so we can walk oldest-newest deterministically.
  const sortedNew = [...insertedRounds].sort((a, b) => {
    if (a.crashedAt === b.crashedAt) return a.gameId.localeCompare(b.gameId);
    return a.crashedAt < b.crashedAt ? -1 : 1;
  });

  for (const targetRound of sortedNew) {
    outcome.insertedRounds += 1;

    // If this round was already validated (worker crash between validation
    // insert and pending-row update), skip — the UNIQUE(game_id) on
    // prediction_validations guarantees a duplicate insert would no-op.
    const existing = await sql<{ prediction_id: string; result: string; target_multiplier: string | number | null; predicted_probability: string | number | null; resolved_at: string | Date }>`
      select prediction_id, result, target_multiplier, predicted_probability, resolved_at
      from prediction_validations
      where game_id = ${targetRound.gameId}
      limit 1
    `;
    if (existing.length > 0) {
      outcome.resolved += 1;
      // Recovery re-pass: target is unknown to the recovery branch, so signal
      // the worker with targetMultiplier=0 to skip re-notification.
      outcome.pairs.push({
        predictionId: existing[0]!.prediction_id,
        gameId: targetRound.gameId,
        result: existing[0]!.result as "WIN" | "LOSS",
        targetMultiplier: 0,
        actualMultiplier: targetRound.multiplier,
        probability: Number(existing[0]!.predicted_probability ?? 0),
        resolvedAt:
          existing[0]!.resolved_at instanceof Date
            ? existing[0]!.resolved_at.toISOString()
            : String(existing[0]!.resolved_at),
      });
      continue;
    }

    // NEW: Try to find prediction by target_game_id first (event-driven approach)
    // This ensures we validate against the correct prediction for the round
    const targetMatchedRows = await sql<{
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
      target_game_id: string | null;
    }>`
      select prediction_id, target_multiplier, probability, confidence,
             regime_name, regime_confidence, reasoning, feature_summary,
             model_version, requested_at, target_game_id
      from pending_predictions
      where target_game_id = ${targetRound.gameId} and matched = false
      limit 1
    `;

    if (targetMatchedRows.length > 0) {
      const p = targetMatchedRows[0]!;
      const actualMultiplier = targetRound.multiplier;
      const result = actualMultiplier >= p.target_multiplier ? "WIN" : "LOSS";
      const now = new Date().toISOString();

      // Durable anchor: stamp the pending row with its target_game_id BEFORE
      // the validation insert.  A crash between the two leaves the system in
      // a recoverable state (next run re-resolves the same round against the
      // same pending row thanks to the UNIQUE(game_id) index).
      await sql`
        update pending_predictions
           set matched = true,
               matched_game_id = ${targetRound.gameId},
               matched_at = ${now}
         where prediction_id = ${p.prediction_id}
           and matched = false
           and (target_game_id is null or target_game_id = ${targetRound.gameId})
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
      continue;
    }

    // FALLBACK: Oldest unmatched pending prediction (legacy approach)
    // The row may already have a target_game_id from a prior partial match — 
    // if so, the prediction was already paired, skip to avoid double-matching.
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
      target_game_id: string | null;
    }>`
      select prediction_id, target_multiplier, probability, confidence,
             regime_name, regime_confidence, reasoning, feature_summary,
             model_version, requested_at, target_game_id
      from pending_predictions
      where matched = false
      order by requested_at asc
      limit 1
    `;
    if (pending.length === 0) break; // no more pendings to resolve; the rest wait
    const p = pending[0]!;
    if (p.target_game_id && p.target_game_id !== targetRound.gameId) {
      // Pending row was already paired to a different round. Treat as no
      // longer available; the duplicate is unreachable here in practice (the
      // validation row already exists) — defensive guard.
      continue;
    }

    const actualMultiplier = targetRound.multiplier;
    const result = actualMultiplier >= p.target_multiplier ? "WIN" : "LOSS";
    const now = new Date().toISOString();

    // Durable anchor: stamp the pending row with its target_game_id BEFORE
    // the validation insert.  A crash between the two leaves the system in
    // a recoverable state (next run re-resolves the same round against the
    // same pending row thanks to the UNIQUE(game_id) index).
    await sql`
      update pending_predictions
         set target_game_id = ${targetRound.gameId},
             matched = true,
             matched_game_id = ${targetRound.gameId},
             matched_at = ${now}
       where prediction_id = ${p.prediction_id}
         and matched = false
         and (target_game_id is null or target_game_id = ${targetRound.gameId})
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

    await sql`
      update pending_predictions
         set matched = true,
             matched_game_id = ${targetRound.gameId},
             matched_at = ${now}
       where prediction_id = ${p.prediction_id}
    `;

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