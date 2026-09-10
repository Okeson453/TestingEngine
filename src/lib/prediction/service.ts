import { getSql } from "@/lib/db";

/** Timestamps can surface as Date objects or strings depending on the binder. */
function toIsoText(value: unknown): string {
  return value instanceof Date ? value.toISOString() : String(value);
}

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
      where resolved_at >= date_trunc('day', now())
        and resolved_at < date_trunc('day', now()) + interval '1 day'
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
    requestedAt: toIsoText(r.requested_at),
    resolvedAt: toIsoText(r.resolved_at),
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

/** P0: single snapshot for the prediction dashboard (one pool client). */
/** Delivery-stage timestamps for one prediction signal (durable, from the
 *  prediction_delivery_timeline view — never inferred from logs). */
export interface DeliveryTimeline {
  predictionId: string;
  sourceGameId: string | null;
  targetGameId: string | null;
  generatedAt: string | null;
  queuedAt: string | null;
  dispatchStartedAt: string | null;
  telegramAcceptedAt: string | null;
  targetRoundStartedAt: string | null;
  deliveryOutcome: "EARLY" | "ON_TIME" | "LATE" | "UNKNOWN" | "EXPIRED" | "FAILED" | null;
  leadTimeMs: number | null;
}

/** Lead-time aggregates over the recent delivery window.lead_time_ms and
 *  delivery_outcome are computed by the worker from authoritative timestamps:
 *  lead_time_ms = target_round_started_at - telegram_accepted_at
 *  (positive = ON_TIME); delivery_outcome per migration 0029 rules. */
export interface LeadTimeSnapshot {
  windowHours: number;
  total: number;
  early: number;
  onTime: number;
  late: number;
  unknown: number;
  expired: number;
  failed: number;
  /** Real pending count: outbox rows in 'pending'/'inflight' status.
   *  NEVER derived from UNKNOWN (the old algebra manufactured fake pending). */
  pending: number;
  /** Delivered but forensically unresolved (outcome missing/incomplete). */
  unknownDelivered: number;
  /** Not yet delivered (legitimately unclassifiable yet). */
  unknownUndelivered: number;
  /** Trust audit: rows the raw timeline says were LATE but whose stored
   *  outcome does not say LATE (forensic write failure / masked late). */
  maskedLate: number;
  /** Trust audit: stored delivery_outcome disagrees with raw-timestamp
   *  derivation. Nonzero means the stored cache is drifting. */
  outcomeMismatches: number;
  latestLeadTimeMs: number | null;
  p50LeadTimeMs: number | null;
  p95LeadTimeMs: number | null;
  minLeadTimeMs: number | null;
  latest: DeliveryTimeline | null;
}

export interface DashboardSnapshot {
  dailyTarget: DailyTarget;
  today: TodayStats;
  lifetime: LifetimeStats;
  streaks: StreakSnapshot;
  recent: ValidationRecord[];
  pending: PendingStatus;
  worker: {
    running: boolean;
    ownerId: string | null;
    expiresAt: string | null;
    heartbeatAt: string | null;
    lastSyncAt: string | null;
    lastSyncOk: boolean;
    lastError: string | null;
    lastFetchCount: number;
    lastInsertedCount: number;
    lastOnlinePlayers: number | null;
    lastSeenGameId: string | null;
    cyclesTotal: number;
    pendingCount: number;
    resolvedToday: number;
    dailyTarget: number;
    remainingToday: number;
    telegramEnabled: boolean;
    telegramLastSentAt: string | null;
    telegramLastError: string | null;
    healthKind: "RUNNING" | "DEGRADED" | "DATABASE_ERROR" | "OFFLINE" | "UNKNOWN";
    pool: { total: number; idle: number; waiting: number; max: number } | null;
  };
  delivery: LeadTimeSnapshot | null;
  generatedAt: string;
  dbOk: boolean;
  dbError: string | null;
}


/** Lead-time aggregates over the recent delivery window. Classification is
 *  sourced from the view's delivery_status, which DERIVES the outcome from
 *  the authoritative raw timestamps (telegram_accepted_at vs target start)
 *  whenever the stored delivery_outcome cache is missing — a failed forensic
 *  write can no longer silently convert a real LATE into UNKNOWN.
 *  PENDING is counted directly from the outbox status, never derived by
 *  subtracting terminal counts from UNKNOWN (the old algebra manufactured
 *  fake pending rows out of forensic gaps). */
export async function getLeadTimeSnapshot(windowHours = 24): Promise<LeadTimeSnapshot | null> {
  const { getSql } = await import("@/lib/db");
  const sql = await getSql();
  try {
    const window = `${Number(windowHours)} hours`;
    const aggRows = await sql<{
      total: number;
      early: number;
      on_time: number;
      late: number;
      unknown: number;
      expired: number;
      failed: number;
      pending: number;
      unknown_delivered: number;
      unknown_undelivered: number;
      masked_late: number;
      outcome_mismatches: number;
    }>`
      select
        count(*)::int as total,
        count(*) filter (where delivery_status = 'EARLY')::int as early,
        count(*) filter (where delivery_status = 'ON_TIME')::int as on_time,
        count(*) filter (where delivery_status = 'LATE')::int as late,
        count(*) filter (where delivery_status = 'UNKNOWN')::int as unknown,
        count(*) filter (where delivery_status = 'EXPIRED')::int as expired,
        count(*) filter (where delivery_status = 'FAILED')::int as failed,
        count(*) filter (where outbox_status in ('pending', 'inflight'))::int as pending,
        count(*) filter (where outbox_status = 'delivered' and delivery_status = 'UNKNOWN')::int as unknown_delivered,
        count(*) filter (where outbox_status <> 'delivered' and delivery_status = 'UNKNOWN')::int as unknown_undelivered,
        count(*) filter (where lead_time_computed_ms is not null
          and lead_time_computed_ms < 0
          and coalesce(delivery_outcome, '') <> 'LATE')::int as masked_late,
        count(*) filter (where delivery_outcome is not null
          and delivery_outcome <> delivery_status)::int as outcome_mismatches
      from prediction_delivery_timeline
      where generated_at >= now() - ${window}::interval
    `;
    const pctRows = await sql<{ p50: number | null; p95: number | null; min_ms: number | null; latest_ms: number | null; n: number }>`
      select
        percentile_cont(0.5) within group (order by lead_ms) as p50,
        percentile_cont(0.95) within group (order by lead_ms) as p95,
        min(lead_ms) as min_ms,
        (array_agg(lead_ms order by generated_at desc))[1] as latest_ms,
        count(*)::int as n
      from (
        select generated_at, coalesce(lead_time_ms, round(lead_time_computed_ms))::numeric as lead_ms
        from prediction_delivery_timeline
        where coalesce(lead_time_ms, lead_time_computed_ms) is not null
          and generated_at >= now() - ${window}::interval
      ) t
    `;
    const latestRows = await sql<Record<string, unknown>>`
      select prediction_id, source_game_id, target_game_id, generated_at, queued_at,
             dispatch_started_at, telegram_accepted_at, target_round_started_at,
             delivery_outcome, delivery_status, lead_time_ms
      from prediction_delivery_timeline
      order by generated_at desc
      limit 1
    `;
    const agg = aggRows[0];
    const latest = latestRows[0];
    const iso = (v: unknown): string | null => {
      if (v == null) return null;
      return v instanceof Date ? v.toISOString() : new Date(String(v)).toISOString();
    };
    return {
      windowHours,
      total: agg?.total ?? 0,
      early: agg?.early ?? 0,
      onTime: agg?.on_time ?? 0,
      late: agg?.late ?? 0,
      unknown: agg?.unknown ?? 0,
      expired: agg?.expired ?? 0,
      failed: agg?.failed ?? 0,
      pending: agg?.pending ?? 0,
      unknownDelivered: agg?.unknown_delivered ?? 0,
      unknownUndelivered: agg?.unknown_undelivered ?? 0,
      maskedLate: agg?.masked_late ?? 0,
      outcomeMismatches: agg?.outcome_mismatches ?? 0,
      latestLeadTimeMs: pctRows[0]?.latest_ms != null ? Math.round(Number(pctRows[0].latest_ms)) : null,
      p50LeadTimeMs: pctRows[0]?.p50 != null ? Math.round(Number(pctRows[0].p50)) : null,
      p95LeadTimeMs: pctRows[0]?.p95 != null ? Math.round(Number(pctRows[0].p95)) : null,
      minLeadTimeMs: pctRows[0]?.min_ms != null ? Number(pctRows[0].min_ms) : null,
      latest: latest
        ? {
            predictionId: String(latest.prediction_id),
            sourceGameId: latest.source_game_id != null ? String(latest.source_game_id) : null,
            targetGameId: latest.target_game_id != null ? String(latest.target_game_id) : null,
            generatedAt: iso(latest.generated_at),
            queuedAt: iso(latest.queued_at),
            dispatchStartedAt: iso(latest.dispatch_started_at),
            telegramAcceptedAt: iso(latest.telegram_accepted_at),
            targetRoundStartedAt: iso(latest.target_round_started_at),
            deliveryOutcome:
              (latest.delivery_status ?? latest.delivery_outcome) != null
                ? (String(latest.delivery_status ?? latest.delivery_outcome) as DeliveryTimeline["deliveryOutcome"])
                : null,
            leadTimeMs: latest.lead_time_ms != null ? Number(latest.lead_time_ms) : null,
          }
        : null,
    };
  } catch {
    // View may not exist yet (pre-0029 database) — no fabricated numbers.
    return null;
  }
}

export async function getDashboardSnapshot(): Promise<DashboardSnapshot> {
  const { withDashboardClient, withPinnedClient, getPoolStats, getPgPool } = await import("@/lib/db");
  const generatedAt = new Date().toISOString();

  const poolSnap = () => {
    const s = getPoolStats();
    return s
      ? { total: s.totalCount, idle: s.idleCount, waiting: s.waitingCount, max: s.max }
      : null;
  };

  const emptyWorker = (): DashboardSnapshot["worker"] => ({
    running: false,
    ownerId: null,
    expiresAt: null,
    heartbeatAt: null,
    lastSyncAt: null,
    lastSyncOk: false,
    lastError: null,
    lastFetchCount: 0,
    lastInsertedCount: 0,
    lastOnlinePlayers: null,
    lastSeenGameId: null,
    cyclesTotal: 0,
    pendingCount: 0,
    resolvedToday: 0,
    dailyTarget: 100,
    remainingToday: 100,
    telegramEnabled: false,
    telegramLastSentAt: null,
    telegramLastError: null,
    healthKind: "UNKNOWN",
    pool: poolSnap(),
  });

  type Q = <T>(text: string, params?: unknown[]) => Promise<T[]>;

  async function build(query: Q): Promise<DashboardSnapshot> {
    const targetRows = await query<{ daily_target: number }>(
      `select daily_target from validation_config limit 1`,
    );
    const dailyTargetNum = targetRows[0]?.daily_target ?? 100;
    const dailyTarget: DailyTarget = {
      dailyTarget: dailyTargetNum,
      updatedAt: generatedAt,
    };

    const todayRows = await query<{ result: string; count: number }>(
      `select result, count(*)::int as count
       from prediction_validations
       where resolved_at >= date_trunc('day', now())
         and resolved_at < date_trunc('day', now()) + interval '1 day'
       group by result`,
    );
    const wins = todayRows.find((r) => r.result === "WIN")?.count ?? 0;
    const losses = todayRows.find((r) => r.result === "LOSS")?.count ?? 0;
    const total = wins + losses;
    const today: TodayStats = {
      total,
      wins,
      losses,
      winRate: total === 0 ? 0 : wins / total,
      lossRate: total === 0 ? 0 : losses / total,
      remaining: Math.max(0, dailyTargetNum - total),
    };

    const lifeRows = await query<{ result: string; count: number }>(
      `select result, count(*)::int as count from prediction_validations group by result`,
    );
    const lw = lifeRows.find((r) => r.result === "WIN")?.count ?? 0;
    const ll = lifeRows.find((r) => r.result === "LOSS")?.count ?? 0;
    const lt = lw + ll;
    const lifetime: LifetimeStats = {
      total: lt,
      wins: lw,
      losses: ll,
      winRate: lt === 0 ? 0 : lw / lt,
      lossRate: lt === 0 ? 0 : ll / lt,
    };

    const streakRows = await query<{ result: string }>(
      `select result from prediction_validations order by resolved_at desc, id desc limit 2000`,
    );
    const results = streakRows.map((r) => r.result);
    let streaks: StreakSnapshot = {
      currentKind: "none",
      currentCount: 0,
      maxWin: 0,
      maxLoss: 0,
    };
    if (results.length > 0) {
      const first = results[0]!;
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
      streaks = {
        currentKind: first as "WIN" | "LOSS",
        currentCount,
        maxWin,
        maxLoss,
      };
    }

    const recentRows = await query<Record<string, unknown>>(
      `select prediction_id, game_id, result, target_multiplier, actual_multiplier,
              predicted_probability, predicted_confidence, model_version,
              regime_name, requested_at, resolved_at
       from prediction_validations
       order by resolved_at desc, id desc
       limit 20`,
    );
    const recent: ValidationRecord[] = recentRows.map((r) => ({
      predictionId: String(r.prediction_id),
      gameId: r.game_id != null ? String(r.game_id) : "",
      targetMultiplier: Number(r.target_multiplier),
      predictedProbability: Number(r.predicted_probability ?? 0),
      predictedConfidence: Number(r.predicted_confidence ?? 0),
      actualMultiplier: Number(r.actual_multiplier ?? 0),
      result: r.result === "WIN" ? "WIN" : "LOSS",
      modelVersion: String(r.model_version ?? ""),
      regimeName: r.regime_name != null ? String(r.regime_name) : null,
      requestedAt: String(r.requested_at),
      resolvedAt: String(r.resolved_at),
    }));

    const pendingRows = await query<{ count: number; oldest: string | null }>(
      `select count(*)::int as count, min(requested_at) as oldest
       from pending_predictions where matched = false`,
    );
    const pending: PendingStatus = {
      hasPending: (pendingRows[0]?.count ?? 0) > 0,
      pendingCount: pendingRows[0]?.count ?? 0,
      oldestPendingAt: pendingRows[0]?.oldest ?? null,
    };

    const lockRows = await query<{
      owner_id: string;
      expires_at: string | Date;
      heartbeat_at: string | Date;
    }>(
      `select owner_id, expires_at, heartbeat_at from worker_locks where lock_key = 'prediction_worker' limit 1`,
    );
    const stateRows = await query<{ key: string; value: string }>(
      `select key, value from worker_state`,
    );
    const state = new Map(stateRows.map((r) => [r.key, r.value]));
    const lock = lockRows[0];
    const now = Date.now();
    const expiresAt = lock?.expires_at
      ? lock.expires_at instanceof Date
        ? lock.expires_at.toISOString()
        : String(lock.expires_at)
      : null;
    const heartbeatAt = lock?.heartbeat_at
      ? lock.heartbeat_at instanceof Date
        ? lock.heartbeat_at.toISOString()
        : String(lock.heartbeat_at)
      : null;
    const lockRunning =
      !!lock && expiresAt != null && new Date(expiresAt).getTime() > now;
    const lastSyncOk = state.get("last_sync_ok") === "1";
    const lastError = state.get("last_error") ?? null;
    let healthKind: DashboardSnapshot["worker"]["healthKind"] = "OFFLINE";
    if (lockRunning) healthKind = lastSyncOk ? "RUNNING" : "DEGRADED";
    else if (lastError) healthKind = "DEGRADED";

    const worker: DashboardSnapshot["worker"] = {
      running: lockRunning,
      ownerId: lock?.owner_id ?? null,
      expiresAt,
      heartbeatAt,
      lastSyncAt: state.get("last_sync_at") ?? null,
      lastSyncOk,
      lastError,
      lastFetchCount: Number(state.get("last_fetch_count") ?? 0) || 0,
      lastInsertedCount: Number(state.get("last_inserted_count") ?? 0) || 0,
      lastOnlinePlayers:
        state.get("last_online_players") != null &&
        state.get("last_online_players") !== ""
          ? Number(state.get("last_online_players"))
          : null,
      lastSeenGameId: state.get("last_seen_game_id") ?? null,
      cyclesTotal: Number(state.get("cycles_total") ?? 0) || 0,
      pendingCount: pending.pendingCount,
      resolvedToday: today.total,
      dailyTarget: dailyTargetNum,
      remainingToday: today.remaining,
      telegramEnabled: state.get("telegram_enabled") === "1",
      telegramLastSentAt: state.get("telegram_last_sent_at") ?? null,
      telegramLastError: state.get("telegram_last_error") ?? null,
      healthKind,
      pool: poolSnap(),
    };

    const delivery = await getLeadTimeSnapshot();
    return {
      dailyTarget,
      today,
      lifetime,
      streaks,
      recent,
      pending,
      worker,
      delivery,
      generatedAt,
      dbOk: true,
      dbError: null,
    };
  }

  try {
    if (getPgPool()) {
      return await withDashboardClient(async (client) => {
        const query: Q = async <T,>(text: string, params: unknown[] = []) => {
          const res = await client.query(text, params);
          return res.rows as T[];
        };
        return build(query);
      });
    }
    const sql = await getSql();
    const query: Q = async <T,>(text: string, params: unknown[] = []) =>
      sql.query<T>(text, params);
    return await build(query);
  } catch (e) {
    const msg = String(e);
    const isTimeout = /timeout|POOL EXHAUSTION|connect/i.test(msg);
    const w = emptyWorker();
    w.lastError = msg;
    w.healthKind = isTimeout ? "DATABASE_ERROR" : "UNKNOWN";
    return {
      dailyTarget: { dailyTarget: 100, updatedAt: generatedAt },
      today: {
        total: 0,
        wins: 0,
        losses: 0,
        winRate: 0,
        lossRate: 0,
        remaining: 100,
      },
      lifetime: { total: 0, wins: 0, losses: 0, winRate: 0, lossRate: 0 },
      streaks: { currentKind: "none", currentCount: 0, maxWin: 0, maxLoss: 0 },
      recent: [],
      pending: { hasPending: false, pendingCount: 0, oldestPendingAt: null },
      worker: w,
      delivery: null,
      generatedAt,
      dbOk: false,
      dbError: msg,
    };
  }
}
