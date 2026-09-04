import { randomUUID } from "node:crypto";
import { getSql } from "@/lib/db";
import type { Sql } from "@/lib/db";
import { PredictionEngine } from "./prediction-engine";
import type {
  PredictionSignal,
  HistoricalRound,
  ThresholdTarget,
} from "./types";
import type { CrashRound } from "@/lib/crash/types";

const DEFAULT_TARGET: ThresholdTarget = 1.3;
const MIN_HISTORY = 20;

export type QueuedPrediction = {
  signal: PredictionSignal;
  lastRound: CrashRound | null;
};

function mapCrashRoundToHistorical(r: CrashRound): HistoricalRound {
  return {
    id: r.gameId,
    externalRoundId: r.gameId,
    sessionId: null,
    startedAt: r.beganAt,
    crashedAt: r.crashedAt,
    crashPoint: r.multiplier,
    observationSource: "bc-game",
    dataQuality: "high",
    createdAt: r.crashedAt,
  };
}

async function loadLastRoundSnapshot(sql: Sql): Promise<CrashRound | null> {
  const rows = await sql<{
    game_id: string;
    multiplier: string | number;
    began_at: string | Date | null;
    crashed_at: string | Date;
  }>`
    select game_id, multiplier, began_at, crashed_at
    from crash_rounds
    order by crashed_at desc, game_id desc
    limit 1
  `;
  const r = rows[0];
  if (!r) return null;
  return {
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
  };
}

/**
 * Generate a prediction for the next sequential target round (MAX(game_id)+1)
 * and insert it as PENDING. Returns null when generation is skipped
 * (insufficient history, target already crashed, or active pending exists).
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
): Promise<PredictionSignal | null> {
  if (rounds.length < MIN_HISTORY) return null;

  const engine = new PredictionEngine();
  const timestamp = new Date().toISOString();
  return engine.predict({
    priorRounds: rounds,
    targetRoundId: targetGameId,
    timestamp,
    target: DEFAULT_TARGET,
  });
}

export async function validateAgainstNewRounds(
  insertedRounds: CrashRound[],
): Promise<{
  resolved: number;
  insertedRounds: number;
  pairs: Array<{
    predictionId: string;
    gameId: string;
    targetMultiplier: number;
    actualMultiplier: number;
    probability: number;
    result: "WIN" | "LOSS";
    resolvedAt: string;
  }>;
}> {
  const sql = await getSql();
  const pairs: Array<{
    predictionId: string;
    gameId: string;
    targetMultiplier: number;
    actualMultiplier: number;
    probability: number;
    result: "WIN" | "LOSS";
    resolvedAt: string;
  }> = [];

  let resolved = 0;
  for (const round of insertedRounds) {
    const pending = await sql<{
      prediction_id: string;
      target_multiplier: string | number;
      probability: string | number;
    }>`
      select prediction_id, target_multiplier, probability
      from pending_predictions
      where target_game_id = ${round.gameId}
        and status = 'PENDING'
      limit 1
    `;
    if (pending.length === 0) continue;

    const p = pending[0]!;
    const target = Number(p.target_multiplier);
    const actual = Number(round.multiplier);
    const result = actual >= target ? "WIN" : "LOSS";
    const resolvedAt = new Date().toISOString();

    await sql`
      update pending_predictions
         set matched = true,
             matched_game_id = ${round.gameId},
             matched_at = ${resolvedAt},
             status = 'MATCHED'
       where prediction_id = ${p.prediction_id}
         and status = 'PENDING'
    `;

    await sql`
      insert into prediction_results (
        prediction_id, game_id, target_multiplier, predicted_probability,
        predicted_confidence, actual_multiplier, result, model_version,
        regime_name, requested_at, resolved_at
      )
      select
        prediction_id, ${round.gameId}, target_multiplier, probability,
        confidence, ${actual}, ${result}, model_version,
        regime_name, requested_at, ${resolvedAt}
      from pending_predictions
      where prediction_id = ${p.prediction_id}
      on conflict (prediction_id) do nothing
    `;

    pairs.push({
      predictionId: p.prediction_id,
      gameId: round.gameId,
      targetMultiplier: target,
      actualMultiplier: actual,
      probability: Number(p.probability),
      result,
      resolvedAt,
    });
    resolved += 1;
  }

  return { resolved, insertedRounds: insertedRounds.length, pairs };
}

export async function getDailyTarget(): Promise<{ dailyTarget: number }> {
  const sql = await getSql();
  const rows = await sql<{ daily_target: number }>`
    select daily_target from prediction_config limit 1
  `;
  const r = rows[0];
  return { dailyTarget: r?.daily_target ?? 100 };
}

export async function setDailyTarget(n: number): Promise<{ dailyTarget: number }> {
  const sql = await getSql();
  await sql`
    insert into prediction_config (id, daily_target)
    values (1, ${n})
    on conflict (id) do update set daily_target = excluded.daily_target
  `;
  const rows = await sql<{ daily_target: number }>`
    select daily_target from prediction_config limit 1
  `;
  const r = rows[0];
  return { dailyTarget: r?.daily_target ?? n };
}

export async function getTodayStats(): Promise<{
  total: number;
  wins: number;
  losses: number;
  remaining: number;
}> {
  const sql = await getSql();
  const [targetRows, resultRows] = await Promise.all([
    sql<{ daily_target: number }>`select daily_target from prediction_config limit 1`,
    sql<{ total: number; wins: number }>`
      select
        count(*)::int as total,
        count(*) filter (where result = 'WIN')::int as wins
      from prediction_results
      where resolved_at::date = (now() at time zone 'utc')::date
    `,
  ]);
  const dailyTarget = targetRows[0]?.daily_target ?? 100;
  const total = resultRows[0]?.total ?? 0;
  const wins = resultRows[0]?.wins ?? 0;
  return {
    total,
    wins,
    losses: total - wins,
    remaining: Math.max(0, dailyTarget - total),
  };
}

export async function listResults(page = 1, pageSize = 50): Promise<{
  records: Array<{
    predictionId: string;
    gameId: string;
    targetMultiplier: number;
    predictedProbability: number;
    predictedConfidence: number;
    actualMultiplier: number;
    result: "WIN" | "LOSS";
    modelVersion: string | null;
    regimeName: string | null;
    requestedAt: string | null;
    resolvedAt: string | null;
  }>;
  total: number;
  page: number;
  pageSize: number;
}> {
  const sql = await getSql();
  const offset = Math.max(0, (page - 1) * pageSize);
  const [countRows, rows] = await Promise.all([
    sql<{ total: number }>`select count(*)::int as total from prediction_results`,
    sql<{
      prediction_id: string;
      game_id: string;
      target_multiplier: string | number;
      predicted_probability: string | number;
      predicted_confidence: string | number;
      actual_multiplier: string | number;
      result: string;
      model_version: string | null;
      regime_name: string | null;
      requested_at: string | null;
      resolved_at: string | null;
    }>`
      select *
      from prediction_results
      order by resolved_at desc
      limit ${pageSize} offset ${offset}
    `,
  ]);
  const total = countRows[0]?.total ?? 0;
  const records = rows.map((r) => ({
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
  // Prefer status='PENDING' (post-0013). Fall back also excludes matched rows
  // so cancelled/expired legacy rows never starve generation.
  const rows = await sql<{
    count: number;
    oldest: string | null;
  }>`
    select count(*)::int as count, min(requested_at) as oldest
    from pending_predictions
    where status = 'PENDING' OR (status IS NULL AND matched = false)
  `;
  const r = rows[0];
  return {
    hasPending: (r?.count ?? 0) > 0,
    pendingCount: r?.count ?? 0,
    oldestPendingAt: r?.oldest ?? null,
  };
}
