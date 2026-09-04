/**
 * Operator server functions.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.8
 *
 * 8 functions for the operator dashboard / on-call runbook:
 *   - getInvariantStatus
 *   - retryDeadNotifications
 *   - reEnqueuePrediction
 *   - getLatencyDashboard
 *   - getRecentLiveEvents
 *   - getSlaViolations
 *   - getStuckPredictions
 *   - cancelStalePrediction
 */
import { z } from "zod";
import { getSql, type Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("live-server");

/** Strict temporal invariant (Diagnosis §4 / §20).
 *
 *  Hard requirement for ED-driven N+1 predictions:
 *    prediction_generated_at < target_round_started_at
 *    (and when crashed_at is known: started_at < crashed_at)
 *
 *  Production path: ED(N) → onGameEndPredict writes generated_at with
 *  target_round_started_at NULL; BG(N+1) only backfills started_at.
 *  Therefore when started_at is present, generated_at MUST be strictly earlier.
 *
 *  Rows still PENDING with NULL started_at are excluded (not yet measurable).
 *  A 5-second "grace" is intentionally NOT applied — that weakened the gate.
 */
export async function getInvariantStatus(): Promise<{
  violations: number;
  total: number;
  measurable: number;
  measuredAt: string;
}> {
  const sql = await getSql();
  const rows = await sql<{ violations: number; total: number; measurable: number }>`
    select
      count(*) filter (
        where pp.target_round_started_at is not null
          and coalesce(pp.generated_at, pp.requested_at) >= pp.target_round_started_at
      )::int as violations,
      count(*)::int as total,
      count(*) filter (
        where pp.target_round_started_at is not null
      )::int as measurable
    from pending_predictions pp
    where coalesce(pp.generated_at, pp.requested_at) > now() - interval '24 hours'
      and pp.target_game_id is not null
  `;
  return {
    violations: rows[0]?.violations ?? 0,
    total: rows[0]?.total ?? 0,
    measurable: rows[0]?.measurable ?? 0,
    measuredAt: new Date().toISOString(),
  };
}

/**
 * Strict temporal violation listing (Diagnosis §4 / §20).
 *
 * A violation is any prediction where:
 *   coalesce(generated_at, requested_at) >= target_round_started_at
 * or started_at >= crashed_at when both exist.
 *
 * No 5-second grace — that allowed "predict after start" to report clean.
 */
export async function getInvariantViolations(opts: { limit?: number } = {}): Promise<
  Array<{
    predictionId: string;
    targetGameId: string;
    generatedAt: string;
    targetRoundStartedAt: string;
    targetCrashedAt: string | null;
    lagMs: number;
    reason: "generated_after_started" | "started_after_crashed";
  }>
> {
  const limit = Math.max(1, Math.min(100, opts.limit ?? 100));
  const sql = await getSql();
  const rows = await sql<{
    prediction_id: string;
    target_game_id: string;
    generated_at: string | Date;
    target_round_started_at: string | Date;
    target_crashed_at: string | Date | null;
    lag_ms: number;
    reason: "generated_after_started" | "started_after_crashed";
  }>`
    select
      pp.prediction_id,
      pp.target_game_id,
      coalesce(pp.generated_at, pp.requested_at) as generated_at,
      pp.target_round_started_at,
      cr.crashed_at as target_crashed_at,
      (extract(epoch from (
        coalesce(pp.generated_at, pp.requested_at) - pp.target_round_started_at
      )) * 1000)::int as lag_ms,
      case
        when coalesce(pp.generated_at, pp.requested_at) >= pp.target_round_started_at
          then 'generated_after_started'::text
        else 'started_after_crashed'::text
      end as reason
    from pending_predictions pp
    left join crash_rounds cr on cr.game_id = pp.target_game_id
    where pp.target_game_id is not null
      and pp.target_round_started_at is not null
      and (
        coalesce(pp.generated_at, pp.requested_at) >= pp.target_round_started_at
        or (cr.crashed_at is not null and pp.target_round_started_at >= cr.crashed_at)
      )
    order by coalesce(pp.generated_at, pp.requested_at) desc
    limit ${limit}
  `;
  return rows.map((r) => ({
    predictionId: r.prediction_id,
    targetGameId: r.target_game_id,
    generatedAt:
      r.generated_at instanceof Date ? r.generated_at.toISOString() : String(r.generated_at),
    targetRoundStartedAt:
      r.target_round_started_at instanceof Date
        ? r.target_round_started_at.toISOString()
        : String(r.target_round_started_at),
    targetCrashedAt:
      r.target_crashed_at == null
        ? null
        : r.target_crashed_at instanceof Date
          ? r.target_crashed_at.toISOString()
          : String(r.target_crashed_at),
    lagMs: Number(r.lag_ms ?? 0),
    reason: r.reason,
  }));
}

const RetryDeadInput = z.object({ limit: z.number().int().min(1).max(1000).default(100) });
export async function retryDeadNotifications(
  input: z.infer<typeof RetryDeadInput> = { limit: 100 },
) {
  const parsed = RetryDeadInput.parse(input);
  const sql = await getSql();
  const rows = await sql<{ count: number }>`
    with x as (
      update notification_outbox
      set status = 'pending',
          last_error = coalesce(last_error, '') || ' [operator-retry]',
          next_attempt_at = now(),
          attempt_count = 0
      where status = 'dead_letter'
      returning 1
    )
    select count(*)::int as count from x
  `;
  logger.info(
    { component: "live-server", retried: rows[0]?.count ?? 0, limit: parsed.limit },
    "operator retried dead notifications",
  );
  return { retried: rows[0]?.count ?? 0, limit: parsed.limit };
}

const ReEnqueueInput = z.object({
  predictionId: z.string().min(1),
  chatId: z.string().min(1).optional(),
});
export async function reEnqueuePrediction(input: z.infer<typeof ReEnqueueInput>) {
  const parsed = ReEnqueueInput.parse(input);
  const sql = await getSql();
  const prediction = await sql<{
    prediction_id: string;
    target_multiplier: number;
    probability: number;
    confidence: number;
  }>`
    select prediction_id, target_multiplier, probability, confidence
    from pending_predictions
    where prediction_id = ${parsed.predictionId}
    limit 1
  `;
  if (prediction.length === 0) {
    return { ok: false, reason: "prediction_not_found" };
  }
  const p = prediction[0]!;
  const chatIds = parsed.chatId
    ? [parsed.chatId]
    : (await import("@/lib/notifications/telegram")).getConfiguredChatIds();
  if (chatIds.length === 0) {
    return { ok: false, reason: "no_chat_ids" };
  }
  let enqueued = 0;
  for (const chatId of chatIds) {
    const r = await sql<{ id: number }>`
      insert into notification_outbox (
        notification_id, type, content, metadata, status, priority
      ) values (
        ${crypto.randomUUID()}, 'prediction',
        ${`[operator re-enqueue] target=${p.prediction_id}`},
        ${JSON.stringify({
          predictionId: p.prediction_id,
          chatId,
          kind: "prediction",
        })},
        'pending', 2
      )
      on conflict do nothing
      returning id
    `;
    if (r.length > 0) enqueued += 1;
  }
  return { ok: true, enqueued };
}

export async function getLatencyDashboard() {
  const sql = await getSql();
  // BG backfill lag (observability only — not the prediction critical path)
  const bg = await sql<{
    p50_ms: number | null;
    p95_ms: number | null;
    p99_ms: number | null;
  }>`
    select
      percentile_cont(0.5) within group (order by processor_latency_ms) as p50_ms,
      percentile_cont(0.95) within group (order by processor_latency_ms) as p95_ms,
      percentile_cont(0.99) within group (order by processor_latency_ms) as p99_ms
    from live_event_log
    where event_kind = 'BG'
      and received_at > now() - interval '1 hour'
  `;
  // ED critical path: processor_latency on ED events
  const ed = await sql<{
    p50_ms: number | null;
    p95_ms: number | null;
    p99_ms: number | null;
  }>`
    select
      percentile_cont(0.5) within group (order by processor_latency_ms) as p50_ms,
      percentile_cont(0.95) within group (order by processor_latency_ms) as p95_ms,
      percentile_cont(0.99) within group (order by processor_latency_ms) as p99_ms
    from live_event_log
    where event_kind in ('ED', 'PREDICT')
      and received_at > now() - interval '1 hour'
  `;
  // Ahead-of-time window: started_at - generated_at (positive = good)
  const windows = await sql<{
    p50_ms: number | null;
    p95_ms: number | null;
    neg_rate: number | null;
    n: number;
  }>`
    select
      percentile_cont(0.5) within group (
        order by extract(epoch from (pp.target_round_started_at - coalesce(pp.generated_at, pp.requested_at))) * 1000
      ) as p50_ms,
      percentile_cont(0.95) within group (
        order by extract(epoch from (pp.target_round_started_at - coalesce(pp.generated_at, pp.requested_at))) * 1000
      ) as p95_ms,
      avg(
        case when coalesce(pp.generated_at, pp.requested_at) >= pp.target_round_started_at
          then 1.0 else 0.0 end
      ) as neg_rate,
      count(*)::int as n
    from pending_predictions pp
    where pp.target_round_started_at is not null
      and coalesce(pp.generated_at, pp.requested_at) > now() - interval '24 hours'
  `;
  const outbox = await sql<{ total: number; delivered: number; pending: number; dead: number }>`
    select
      count(*)::int as total,
      count(*) filter (where status = 'delivered')::int as delivered,
      count(*) filter (where status = 'pending')::int as pending,
      count(*) filter (where status = 'dead_letter')::int as dead
    from notification_outbox
    where created_at > now() - interval '24 hours'
  `;
  const round = (v: number | null | undefined) =>
    v != null && Number.isFinite(Number(v)) ? Math.round(Number(v)) : null;
  return {
    budgetMs: {
      edIngest: 100,
      predictionGeneration: 50,
      persistence: 20,
      outboxQueue: 250,
      telegramDelivery: 2000,
      totalPipeline: 500,
    },
    bg: {
      p50Ms: round(bg[0]?.p50_ms),
      p95Ms: round(bg[0]?.p95_ms),
      p99Ms: round(bg[0]?.p99_ms),
    },
    ed: {
      p50Ms: round(ed[0]?.p50_ms),
      p95Ms: round(ed[0]?.p95_ms),
      p99Ms: round(ed[0]?.p99_ms),
    },
    aheadOfTimeWindow: {
      p50Ms: round(windows[0]?.p50_ms),
      p95Ms: round(windows[0]?.p95_ms),
      lateRate: windows[0]?.neg_rate != null ? Number(windows[0].neg_rate) : null,
      sampleSize: windows[0]?.n ?? 0,
    },
    outbox: outbox[0] ?? { total: 0, delivered: 0, pending: 0, dead: 0 },
  };
}

const RecentEventsInput = z.object({ limit: z.number().int().min(1).max(500).default(50) });
export async function getRecentLiveEvents(input: z.infer<typeof RecentEventsInput> = { limit: 50 }) {
  const parsed = RecentEventsInput.parse(input);
  const sql = await getSql();
  return sql<{
    correlation_id: string;
    event_kind: string;
    game_id: string;
    processor_latency_ms: number | null;
    sla_violated: boolean;
    received_at: string;
  }>`
    select correlation_id, event_kind, game_id, processor_latency_ms, sla_violated, received_at
    from live_event_log
    order by received_at desc
    limit ${parsed.limit}
  `;
}

const SlaViolationsInput = z.object({ hours: z.number().int().min(1).max(168).default(24) });
export async function getSlaViolations(input: z.infer<typeof SlaViolationsInput> = { hours: 24 }) {
  const parsed = SlaViolationsInput.parse(input);
  const sql = await getSql();
  const rows = await sql<{ total: number; violated: number }>`
    select
      count(*)::int as total,
      count(*) filter (where sla_violated)::int as violated
    from live_event_log
    where event_kind = 'BG'
      and received_at > now() - (${parsed.hours}::int * interval '1 hour')
  `;
  return {
    windowHours: parsed.hours,
    total: rows[0]?.total ?? 0,
    violated: rows[0]?.violated ?? 0,
    violationRate: rows[0]?.total ? (rows[0].violated ?? 0) / rows[0].total : 0,
  };
}

const StuckInput = z.object({ minutes: z.number().int().min(1).max(120).default(15) });
export async function getStuckPredictions(input: z.infer<typeof StuckInput> = { minutes: 15 }) {
  const parsed = StuckInput.parse(input);
  const sql = await getSql();
  return sql<{
    prediction_id: string;
    target_game_id: string;
    target_round_started_at: string;
    requested_at: string;
  }>`
    select prediction_id, target_game_id, target_round_started_at, requested_at
    from pending_predictions
    where matched = false
      and target_round_started_at is not null
      and target_round_started_at < now() - (${parsed.minutes}::int * interval '1 minute')
    order by target_round_started_at asc
  `;
}

const CancelInput = z.object({ predictionId: z.string().min(1), reason: z.string().optional() });
export async function cancelStalePrediction(input: z.infer<typeof CancelInput>) {
  const parsed = CancelInput.parse(input);
  const sql = await getSql();
  const rows = await sql<{ prediction_id: string }>`
    update pending_predictions
    set matched = true,
        matched_at = now(),
        correlation_id = coalesce(correlation_id, '') || ' [cancelled:' || ${parsed.reason ?? "operator"} || ']'
    where prediction_id = ${parsed.predictionId} and matched = false
    returning prediction_id
  `;
  return { ok: rows.length > 0, predictionId: parsed.predictionId };
}

/**
 * §6.2 Surface per-model performance (read-only).
 * Returns recent win-rate proxies, EWMA metrics, suppression heuristic.
 * Does not mutate prediction state.
 */
export async function getModelPerformanceSummary(): Promise<{
  measuredAt: string;
  models: Array<{
    modelName: string;
    count: number;
    ewmaBrier: number;
    ewmaLogLoss: number;
    recentWinRate: number | null;
    suppressed: boolean;
  }>;
}> {
  const { globalModelPerformance } = await import("../ensemble/model-performance.ts");
  const all = globalModelPerformance.all();
  const models: Array<{
    modelName: string;
    count: number;
    ewmaBrier: number;
    ewmaLogLoss: number;
    recentWinRate: number | null;
    suppressed: boolean;
  }> = [];
  for (const [name, p] of all) {
    const recentWinRate = p.recentTotal > 0 ? p.recentCorrect / p.recentTotal : null;
    const suppressed = p.count >= 50 && p.ewmaBrier > 0.28;
    models.push({
      modelName: name,
      count: p.count,
      ewmaBrier: p.ewmaBrier,
      ewmaLogLoss: p.ewmaLogLoss,
      recentWinRate,
      suppressed,
    });
  }
  models.sort((a, b) => a.modelName.localeCompare(b.modelName));
  return { measuredAt: new Date().toISOString(), models };
}

/** Diagnosis §2 / §14 — socket connection health for operator dashboard. */
export async function getSocketHealth() {
  try {
    const { bcGameSocket } = await import("@/lib/crash/socket-client");
    return bcGameSocket.getState();
  } catch {
    return {
      status: "stopped" as const,
      lastError: "socket module unavailable",
      lastConnectedAt: null,
      lastDisconnectedAt: null,
      reconnectAttempts: 0,
      socketId: null,
      transport: null,
      lastEdAt: null,
      lastBgAt: null,
      lastEventAt: null,
      lastEventKind: null,
      eventLagMs: null,
      totalReconnects: 0,
    };
  }
}

/**
 * Diagnosis §12 — inter-round prediction window vs actual pipeline latency.
 * prediction_window_ms = target_round_started_at - source crash time (approx via requested_at).
 */
export async function getPredictionWindows(limit = 50) {
  const sql = await getSql();
  const rows = await sql<{
    target_game_id: string;
    generated_at: string | Date | null;
    target_round_started_at: string | Date | null;
    window_ms: number | null;
    too_late: boolean;
  }>`
    SELECT
      pp.target_game_id,
      pp.generated_at,
      pp.target_round_started_at,
      CASE
        WHEN pp.generated_at IS NOT NULL AND pp.target_round_started_at IS NOT NULL
        THEN EXTRACT(EPOCH FROM (pp.target_round_started_at - pp.generated_at)) * 1000
        ELSE NULL
      END AS window_ms,
      CASE
        WHEN pp.generated_at IS NOT NULL AND pp.target_round_started_at IS NOT NULL
             AND pp.generated_at >= pp.target_round_started_at
        THEN true
        ELSE false
      END AS too_late
    FROM pending_predictions pp
    WHERE pp.generated_at IS NOT NULL
    ORDER BY pp.requested_at DESC
    LIMIT ${limit}
  `;
  return rows.map((r) => ({
    targetGameId: r.target_game_id,
    generatedAt: r.generated_at ? new Date(r.generated_at).toISOString() : null,
    targetStartedAt: r.target_round_started_at
      ? new Date(r.target_round_started_at).toISOString()
      : null,
    windowMs: r.window_ms != null ? Math.round(Number(r.window_ms)) : null,
    tooLate: r.too_late,
  }));
}

/** Diagnosis P0-3 — full path probe for Socket.IO / Cloudflare. */
export async function getSocketDiagnostics() {
  const { runSocketDiagnostics } = await import("@/lib/crash/socket-diagnostics");
  return runSocketDiagnostics();
}
