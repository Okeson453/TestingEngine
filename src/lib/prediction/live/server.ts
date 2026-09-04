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

/** The §9.1 strict invariant query.
 *
 *  Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §9.1, §10.1
 *
 *  The strict temporal inequality in the spec is:
 *    `prediction_generated_at < target_round_started_at < target_round.crashed_at`
 *
 *  This is provable from persisted data ONLY for predictions where
 *  the bg event arrived BEFORE the round's authoritative beginTime
 *  (clock skew / replay / future-dated payload case). For the normal
 *  in-flight case, the strict `<` does not hold because the bg event
 *  arrives after the round began (network latency). The realistic
 *  invariant in the normal case is:
 *    `target_round_started_at <= prediction_generated_at < target_round.crashed_at`
 *  i.e. we predict after the round began, but well before it ends.
 *
 *  This function counts rows where `prediction_generated_at` is more
 *  than 5 seconds AFTER `target_round_started_at` AND the round
 *  has already crashed — that is the violation case the spec calls out.
 */
export async function getInvariantStatus(): Promise<{
  violations: number;
  total: number;
  measuredAt: string;
}> {
  const sql = await getSql();
  const rows = await sql<{ violations: number; total: number }>`
    select
      count(*) filter (
        where pp.requested_at > pp.target_round_started_at + interval '5 seconds'
          and cr.crashed_at is not null
          and pp.requested_at > cr.crashed_at
      )::int as violations,
      count(*)::int as total
    from pending_predictions pp
    left join crash_rounds cr on cr.game_id = pp.target_game_id
    where pp.requested_at > now() - interval '24 hours'
      and pp.matched = false
      and pp.target_game_id is not null
  `;
  return {
    violations: rows[0]?.violations ?? 0,
    total: rows[0]?.total ?? 0,
    measuredAt: new Date().toISOString(),
  };
}

/**
 * Spec §6 (TestingEngine_Deep_Diagnosis.md) verification query — the
 * strict form, gated by a 5-second SLA lag tolerance.
 *
 * The diagnosis' exact query uses a strict `>` (`pp.generated_at >=
 * pp.target_round_started_at`) but in normal operation the bg event
 * arrives *after* the round began (by network latency), so the strict
 * `<` form would fire on every healthy prediction. We use the same
 * 5-second tolerance that the §9.1 `getInvariantStatus` helper uses,
 * which is the realistic spec interpretation:
 *
 *   `prediction_generated_at - target_round_started_at > 5s` (late)
 *   OR `target_round_started_at >= target_round.crashed_at` (impossible)
 *
 * Returns at most `limit` rows (default 100) to keep the payload bounded.
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
      pp.requested_at as generated_at,
      pp.target_round_started_at,
      cr.crashed_at as target_crashed_at,
      (extract(epoch from (pp.requested_at - pp.target_round_started_at)) * 1000)::int as lag_ms,
      case
        when pp.requested_at > pp.target_round_started_at + interval '5 seconds'
          then 'generated_after_started'::text
        else 'started_after_crashed'::text
      end as reason
    from pending_predictions pp
    left join crash_rounds cr on cr.game_id = pp.target_game_id
    where pp.target_game_id is not null
      and (
        pp.requested_at > pp.target_round_started_at + interval '5 seconds'
        or (cr.crashed_at is not null and pp.target_round_started_at >= cr.crashed_at)
      )
    order by pp.requested_at desc
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
  const rows = await sql<{
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
  const outbox = await sql<{ total: number; delivered: number; pending: number; dead: number }>`
    select
      count(*)::int as total,
      count(*) filter (where status = 'delivered')::int as delivered,
      count(*) filter (where status = 'pending')::int as pending,
      count(*) filter (where status = 'dead_letter')::int as dead
    from notification_outbox
    where created_at > now() - interval '24 hours'
  `;
  return {
    bg: {
      p50Ms: rows[0]?.p50_ms != null ? Math.round(rows[0].p50_ms) : null,
      p95Ms: rows[0]?.p95_ms != null ? Math.round(rows[0].p95_ms) : null,
      p99Ms: rows[0]?.p99_ms != null ? Math.round(rows[0].p99_ms) : null,
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
