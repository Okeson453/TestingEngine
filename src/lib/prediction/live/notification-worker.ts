/**
 * Outbox dispatcher.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.4
 *
 * Long-lived worker that drains the `notification_outbox` table at a fixed
 * tick cadence. One row per `prediction_id × kind × chat_id`. Each pass
 * claims rows with `FOR UPDATE SKIP LOCKED` (so multiple instances do not
 * double-send) and POSTs to `api.telegram.org` via the existing
 * `sendTelegramMessage` adapter.
 *
 * Status state machine:
 *   PENDING  --tick-->  INFLIGHT  --2xx-->  DELIVERED
 *                            \--4xx (non-429)--> DEAD (no retry)
 *                            \--429/5xx/timeout--> PENDING (with backoff)
 *   INFLIGHT stuck > STALE_MS  --tick-->  PENDING (recovered)
 *   attempts >= MAX_ATTEMPTS  --tick-->  DEAD
 */
import { getCriticalSql, getSql, type Sql } from "@/lib/db";
import { runInTransaction } from "@/lib/prediction/live/tx";
import {
  sendTelegramMessage,
  sendTelegramMessagePrimaryFirst,
  type SendResult,
} from "@/lib/notifications/telegram";
import { getLogger } from "@/lib/observability/logger";
import { isAuthoritative } from "@/lib/prediction/live/fencing";
import { isTargetPastBettingWindow } from "@/lib/prediction/live/live-round-registry";
import { getWakeStats } from "@/lib/prediction/live/outbox-wake";

const logger = getLogger("outbox-dispatcher");

/** Tunables (env-overridable for tests). */
// P2.5: Reduced default from 50ms to 25ms to halve max queue wait time.
// FAST-LANE FIX (production trace 18:35-18:39): the wake channel gives
// immediate drain after every durable enqueue (notifyOutbox is latched, so
// wakes are never lost); a 10ms timer on top of that was issuing a
// claim query against the CRITICAL pool ~100x/sec even when the outbox was
// empty. The timer is now RECOVERY/FALLBACK ONLY (covers missed wakes,
// clock drift, operator re-enqueue without wake). Delivery latency is the
// wake path's job — measured 1ms warm (18:36:33.169 -> .171).
// Recovery/fallback only — wake path is the primary drain trig
ger. 500ms
// bounds missed-wake latency without hammering the critical pool (was 2s,
// which alone could account for a large fraction of 2.5–4s outbox lag).
export const TICK_MS = Number(process.env.OUTBOX_TICK_MS ?? 500);
export const BATCH_SIZE = Number(process.env.OUTBOX_BATCH_SIZE ?? 16);
/** Prediction lane is strictly single-item: freshness over throughput.
 * There is normally only one actionable N+1 prediction at a time. A new
 * prediction must never wait behind a batch of other predictions. */
export const PREDICTION_BATCH_SIZE = Number(
  process.env.OUTBOX_PREDICTION_BATCH_SIZE ?? 1,
);
export const STALE_INFLIGHT_MS = Number(process.env.OUTBOX_STALE_MS ?? 30_000);
export const MAX_ATTEMPTS = Number(process.env.OUTBOX_MAX_ATTEMPTS ?? 5);
/**
 * Max concurrent Telegram sends within a claimed batch (normal lane).
 * POOL-BUDGET FIX: the dispatcher runs on the critical pool (max=3). Previous
 * default of 8 let one drain pass enqueue up to 8 concurrent critical-pool
 * operations per row (temporal check, send stamp, finalize) — the dispatcher
 * created its own queue and competing rows starved the prediction persist
 * path. 2 keeps one connection in reserve for claim/finalize/recovery while
 * still overlapping Telegram RTT (the long leg) with the next row's DB work.
 * Env-overridable as before.
 *
 * Prediction lane uses PREDICTION_PARALLELISM = 1 (see below).
 */
export const BATCH_PARALLELISM = Number(process.env.OUTBOX_BATCH_PARALLELISM ?? 2);
/** Prediction lane concurrency is hard-capped at 1 so a newly arriving N+1
 * signal is never blocked behind an in-flight prediction send. */
export const PREDICTION_PARALLELISM = Number(
  process.env.OUTBOX_PREDICTION_PARALLELISM ?? 1,
);
/**
 * MINIMUM REMAINING LEAD SAFETY GATE (remediation plan §6): before a
 * prediction send begins, require at least this much residual deadline
 * budget. The budget covers the Telegram P99 send leg + delivery-finalize
 * leg + safety margin, so a send that would straddl
e the semantic validity
 * boundary is refused (MISSED) instead of accepted and suppressed. Env-
 * configurable — never a hard-coded guess. Measured production legs are
 * well under 250ms warm; 500ms default is conservative.
 */
export const MIN_REMAINING_LEAD_MS = Number(process.env.DELIVERY_MIN_LEAD_MS ?? 500);
// First-retry backoff lowered 1000→300ms (investigation report): a single
// transient Telegram timeout shouldn't cost a full second before the retry.
// Curve: 300/600/1200/2400... capped at MAX_BACKOFF_MS.
const BASE_BACKOFF_MS = 300;
const MAX_BACKOFF_MS = 60_000;

interface OutboxRow {
  id: number;
  notification_id: string;
  type: string;
  content: string;
  metadata: Record<string, unknown> | null;
  status: string;
  attempt_count: number;
  next_attempt_at: string;
  created_at: string;
  telegram_deadline_at?: string | Date | null;
  priority?: number;
  /** Prediction rows: the round this signal trades (temporal contract). */
  target_game_id?: string | null;
  /** Server-stamped claim time (canonical per-row dispatch start). */
  dispatch_claimed_at?: string | Date | null;
}

/** Per-attempt lifecycle timestamps captured in memory and persisted at the
 * terminal transition (delivered / dead / requeued). Zero extra DB round-trips:
 * the claim stamps dispatch_claimed_at server-side, everything else rides the
 * completion UPDATE. */
interface RowLifecycle {
  claimClientMs: number;
  sendStartedMs: number | null;
  telegramAcceptedMs: number | null;
}

function lifecycleLogFields(
  row: OutboxRow,
  lc: RowLifecycle,
  nowMs: number,
  status: string,
): Record<string, unknown> {
  const createdMs = new Date(row.created_at).getTime();
  const ageMs = Number.isFinite(createdMs) ? Math.max(0, nowMs - createdMs) : null;
  const queueWaitMs = Number.isFinite(createdMs)
    ? Math.max(0, lc.claimClientMs - createdMs)
    : null;
  const dispatchMs =
    lc.sendStartedMs != null ? Math.max(0, lc.sendStartedMs - lc.claimClientMs) : null;
  const 
sendMs =
    lc.sendStartedMs != null && lc.telegramAcceptedMs != null
      ? Math.max(0, lc.telegramAcceptedMs - lc.sendStartedMs)
      : null;
  // Correlation (investigation report): join an outbox delivery log back to
  // its originating ED event without a DB hop. Prediction rows carry both in
  // metadata (written by predictor.ts at enqueue).
  const meta = (row.metadata ?? {}) as Record<string, unknown>;
  return {
    id: row.id,
    notificationId: row.notification_id,
    predictionId: typeof meta.predictionId === "string" ? meta.predictionId : null,
    correlationId: typeof meta.correlationId === "string" ? meta.correlationId : null,
    targetGameId:
      (row.target_game_id as string | null) ??
      (typeof meta.targetGameId === "string" ? meta.targetGameId : null),
    sourceGameId: typeof meta.sourceGameId === "string" ? meta.sourceGameId : null,
    // Canonical timeline aliases for operators (created_at ≈ queued_at)
    queuedAt: row.created_at,
    type: row.type,
    status,
    attempt: row.attempt_count,
    ageMs: ageMs != null ? Math.round(ageMs) : null,
    queueWaitMs: queueWaitMs != null ? Math.round(queueWaitMs) : null,
    dispatchMs: dispatchMs != null ? Math.round(dispatchMs) : null,
    sendMs: sendMs != null ? Math.round(sendMs) : null,
    totalDeliveryMs:
      ageMs != null && lc.telegramAcceptedMs != null
        ? Math.round(lc.telegramAcceptedMs - createdMs)
        : null,
  };
}

export interface DispatcherStats {
  tickCount: number;
  recoveredInflight: number;
  claimed: number;
  delivered: number;
  dead: number;
  requeued: number;
  /** Times the backlog health check found rows pending >5s. */
  backlogWarnings: number;
  lastError: string | null;
}

export class OutboxDispatcher {
  private running = false;
  private stats: DispatcherStats = {
    tickCount: 0,
    recoveredInflight: 0,
    claimed: 0,
    delivered: 0,
    dead: 0,
    requeued: 0,
    backlogWarnings: 0,
    lastError: null,
  };
  private getS
qlFn: () => Promise<Sql> = getCriticalSql;
  /** Normal lane (validation/alert rows) pool. CO-DELIVERY FIX 3: defaults
   * to the GENERAL pool — normal-lane claim/auth/stamp/finalize DB work was
   * riding the critical pool (max=3) and starving the N+1 prediction persist
   * TX (measured 570-890ms persists during detached normal-lane passes).
   * Lanes are disjoint row sets, so this never crosses the prediction path. */
  private getNormalSqlFn: () => Promise<Sql> = getSql;
  private now: () => number = Date.now;

  constructor(opts?: {
    getSqlFn?: () => Promise<Sql>;
    getNormalSqlFn?: () => Promise<Sql>;
    now?: () => number;
  }) {
    if (opts?.getSqlFn) this.getSqlFn = opts.getSqlFn;
    if (opts?.getNormalSqlFn) this.getNormalSqlFn = opts.getNormalSqlFn;
    if (opts?.now) this.now = opts.now;
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    // Fix plan Phase 4: exactly ONE drain loop per worker. The previous
    // scheduleNext() registered a fresh `once` wake listener per cycle; when
    // the timer won the race the listener leaked and later fired overlapping
    // runOneTick()s. The loop below is the only executor — wake events merely
    // shorten its wait.
    void this.drainLoop();
  }

  async stop(): Promise<void> {
    this.running = false;
    // Nudge the loop so a pending wait resolves immediately and the loop exits.
    try {
      const { notifyOutbox } = await import("@/lib/prediction/live/outbox-wake");
      notifyOutbox();
    } catch {
      /* wake module optional in tests */
    }
  }

  getStats(): DispatcherStats {
    return { ...this.stats };
  }

  /** Run a single drain pass (both lanes, awaited). Returns the result for
   * testability. The production drain loop uses processLane directly: the
   * prediction lane runs inline, the normal lane runs DETACHED so a slow
   * result batch can never delay a new prediction (remediation plan §2/§12). */
  async tickOnce(): Promise
<{
    recovered: number;
    delivered: number;
    dead: number;
    requeued: number;
  }> {
    const pred = await this.processLane("prediction");
    const bg = await this.processLane("normal");
    return {
      recovered: 0,
      delivered: pred.delivered + bg.delivered,
      dead: pred.dead + bg.dead,
      requeued: pred.requeued + bg.requeued,
    };
  }

  /** Guard so at most ONE background (normal-lane) pass runs at a time. */
  private bgRunning = false;

  /** Guard so at most ONE forensic reconciliation sweep runs at a time.
   * General pool only — maintenance, never the prediction critical path. */
  private reconcileRunning = false;

  private async reconcileForensics(): Promise<void> {
    if (this.reconcileRunning) return;
    this.reconcileRunning = true;
    try {
      const sql = await getSql();
      const { reconcileForensicOutcomes } = await import(
        "@/lib/prediction/live/delivery-forensics"
      );
      const r = await reconcileForensicOutcomes(sql);
      if (r.reclassified > 0 || r.maskedLate > 0 || r.mismatches > 0) {
        logger.info(
          {
            component: "outbox-dispatcher",
            scanned: r.scanned,
            reclassified: r.reclassified,
            maskedLate: r.maskedLate,
            mismatches: r.mismatches,
          },
          "FORENSIC_RECONCILE sweep complete",
        );
      }
    } catch (e) {
      logger.warn(
        { component: "outbox-dispatcher", error: String(e) },
        "forensic reconciliation sweep failed (will retry next interval)",
      );
    } finally {
      this.reconcileRunning = false;
    }
  }

  /** Kick a detached normal-lane pass. Never awaited by the drain loop —
   * a slow result/validation Telegram send must not occupy the scheduling
   * path a new N+1 prediction needs (plan §2, §12). SKIP LOCKED claiming
   * makes concurrent lane passes row-safe. */
  private runBackgroundDetached(): void {
    if (this.bgRunning) return;
    this.bgRunning = tru
e;
    void (async () => {
      // Bounded backlog drain: keep claiming while the batch comes back full
      // (max 10 passes) so a large queued backlog drains promptly without
      // ever blocking the prediction lane.
      for (let i = 0; i < 10; i += 1) {
        const r = await this.processLane("normal");
        if (r.delivered + r.dead + r.requeued < BATCH_SIZE) break;
      }
    })()
      .catch((e) => {
        logger.warn(
          { component: "outbox-dispatcher", error: String(e) },
          "background lane pass failed (detached)",
        );
      })
      .finally(() => {
        this.bgRunning = false;
      });
  }

  /** Process one lane: claim + dispatch. Lanes are disjoint row sets
   * (prediction vs non-prediction), so concurrent passes never contend. */
  async processLane(lane: "prediction" | "normal"): Promise<{
    delivered: number;
    dead: number;
    requeued: number;
  }> {
    this.stats.tickCount += 1;
    let delivered = 0;
    let dead = 0;
    const claimGateDead = 0;
    let requeued = 0;
    const tickStartMs = this.now();

    // CO-DELIVERY FIX 3: prediction lane keeps the critical pool; the normal
    // lane (validation/alerts) runs its claim/auth/finalize on the general
    // pool so a detached background pass can never starve the N+1 persist TX.
    const sql = await (lane === "prediction" ? this.getSqlFn() : this.getNormalSqlFn());
    // Exclude rows past telegram_deadline_at so we never deliver "predicts the past".
    // Single round-trip claim inside one TX (UPDATE…FROM…RETURNING).
    // At ~800ms Neon RTT, N per-row UPDATEs were costing seconds per tick.
    // LANE FILTER: each pass claims only its own lane's rows — a prediction
    // dispatch opportunity never depends on unrelated notifications.
    // Claim batch: SELECT FOR UPDATE SKIP LOCKED → set status=inflight → COMMIT.
    // Hot path for prediction: claim ONLY (no temporal sweep). Expired-row
    // cleanup lives in recoverStale() on the general
/maintenance pool so it
    // never adds DB work to the N+1 critical path. Pre-send atomic temporal
    // authorization + finalization gate remain the safety invariants.
    const claimLimit =
      lane === "prediction" ? Math.max(1, PREDICTION_BATCH_SIZE) : BATCH_SIZE;
    const claimed = await runInTransaction(sql, async (tx) => {
      try {
        if (lane === "prediction") {
          return await tx<OutboxRow>`
            WITH picked AS (
              SELECT id
              FROM notification_outbox
              WHERE status = 'pending'::text
                AND type = 'prediction'
                AND next_attempt_at <= now()
                AND (telegram_deadline_at IS NULL OR telegram_deadline_at > now())
              ORDER BY priority DESC, next_attempt_at ASC, id ASC
              LIMIT ${claimLimit}
              FOR UPDATE SKIP LOCKED
            )
            UPDATE notification_outbox n
            SET status = 'inflight',
                attempt_count = attempt_count + 1,
                dispatch_claimed_at = now()
            FROM picked
            WHERE n.id = picked.id AND n.status = 'pending'
            RETURNING n.id, n.notification_id, n.type, n.content, n.metadata, n.status,
                      n.attempt_count, n.next_attempt_at, n.created_at,
                      n.telegram_deadline_at, n.priority, n.target_game_id,
                      n.dispatch_claimed_at
          `;
        }
        return await tx<OutboxRow>`
          WITH picked AS (
            SELECT id
            FROM notification_outbox
            WHERE status = 'pending'::text
              AND type <> 'prediction'
              AND next_attempt_at <= now()
              AND (telegram_deadline_at IS NULL OR telegram_deadline_at > now())
              -- RESULT-AFTER-SIGNAL GATE (sep 11): a result for round N must
              -- never claim while the correlated N+1 signal (prediction row
              -- with sourceGameId = N) is still pending/inflight. The 800ms

              -- VALIDATION_DISPATCH_DELAY_MS is measured from ENQUEUE, but
              -- measured dispatch latency is 1.4-2.2s — the delay loses the
              -- race and signal + result deliver together. This gate binds
              -- the result to the signal's terminal state instead. The 30s
              -- bound stops a stuck (retrying) signal from holding the
              -- result hostage; alerts have no gameId and never match.
              AND NOT EXISTS (
                SELECT 1 FROM notification_outbox p
                WHERE p.type = 'prediction'
                  AND p.status IN ('pending', 'inflight')
                  AND p.created_at > now() - interval '30 seconds'
                  AND (p.metadata->>'targetGameId' = notification_outbox.metadata->>'gameId' OR p.metadata->>'gameId' = notification_outbox.metadata->>'gameId')
              )
            ORDER BY priority DESC, next_attempt_at ASC, id ASC
            LIMIT ${claimLimit}
            FOR UPDATE SKIP LOCKED
          )
          UPDATE notification_outbox n
          SET status = 'inflight',
              attempt_count = attempt_count + 1,
              dispatch_claimed_at = now()
          FROM picked
          WHERE n.id = picked.id AND n.status = 'pending'
          RETURNING n.id, n.notification_id, n.type, n.content, n.metadata, n.status,
                    n.attempt_count, n.next_attempt_at, n.created_at,
                    n.telegram_deadline_at, n.priority, n.target_game_id,
                    n.dispatch_claimed_at
        `;
      } catch {
        // Legacy fallback (older drivers without UPDATE…FROM): plain SELECT
        // FOR UPDATE + per-row UPDATE. Lane filter is duplicated explicitly —
        // the pinned tx sql builds raw text and cannot compose fragments.
        const rows =
          lane === "prediction"
            ? await tx<OutboxRow>`
                select id, notification_id, type, content, metadata, status, attempt_count,
                       next_attempt_at, created_at, telegram_deadline_at, priority
         
       from notification_outbox
                where status = 'pending'::text
                  and type = 'prediction'
                  and next_attempt_at <= now()
                  and (telegram_deadline_at is null or telegram_deadline_at > now())
                order by priority desc, next_attempt_at asc, id asc
                limit ${claimLimit}
                for update skip locked
              `
            : await tx<OutboxRow>`
                select id, notification_id, type, content, metadata, status, attempt_count,
                       next_attempt_at, created_at, telegram_deadline_at, priority
                from notification_outbox
                where status = 'pending'::text
                  and type <> 'prediction'
                  and next_attempt_at <= now()
                  and (telegram_deadline_at is null or telegram_deadline_at > now())
                  and not exists (
                    select 1 from notification_outbox p
                    where p.type = 'prediction'
                      and p.status in ('pending', 'inflight')
                      and p.created_at > now() - interval '30 seconds'
                      and (p.metadata->>'targetGameId' = notification_outbox.metadata->>'gameId' OR p.metadata->>'gameId' = notification_outbox.metadata->>'gameId')
                  )
                order by priority desc, next_attempt_at asc, id asc
                limit ${claimLimit}
                for update skip locked
              `;
        for (const r of rows) {
          if (lane === "prediction") {
            await tx`
              update notification_outbox
              set status = 'inflight', attempt_count = attempt_count + 1,
                  dispatch_claimed_at = now()
              where id = ${r.id} and status = 'pending' and type = 'prediction'
            `;
          } else {
            await tx`
              update notification_outbox
              set status = 'inflight', attempt_count = attempt_count + 1,
                  dispatch_claimed_at = now()
              wh
ere id = ${r.id} and status = 'pending' and type <> 'prediction'
            `;
          }
          r.attempt_count = (r.attempt_count ?? 0) + 1;
        }
        return rows;
      }
    });
    // claimGateDead no longer used (sweep moved to recoverStale); keep for
    // type/stats compatibility if any residual path set it.
    dead += claimGateDead;

    if (claimed.length > 0) {
      this.stats.claimed += claimed.length;
    }
    // Client-side claim clock: consistent basis for all in-process durations
    // (DB now() vs client clock skew must not pollute queue_wait/dispatch ms).
    const claimClientMs = this.now();

    // Parallel dispatch: prediction lane is strictly single-item (P0).
    // Freshness > throughput for N+1. Normal lane retains BATCH_PARALLELISM.
    const parallelism =
      lane === "prediction"
        ? Math.max(1, Math.min(PREDICTION_PARALLELISM, 1))
        : Math.max(1, BATCH_PARALLELISM);
    const laneChunks: OutboxRow[][] = [];
    for (let i = 0; i < claimed.length; i += parallelism) {
      laneChunks.push(claimed.slice(i, i + parallelism));
    }
    for (const chunk of laneChunks) {
      const results = await Promise.all(
        chunk.map(async (row) => {
          const lc: RowLifecycle = {
            claimClientMs,
            sendStartedMs: null,
            telegramAcceptedMs: null,
          };
          try {
            // Deadline-aware pre-send check (P0): stop if past telegram_deadline_at
            const deadlineRaw = row.telegram_deadline_at;
            const deadlineMs = deadlineRaw
              ? new Date(deadlineRaw as string | Date).getTime()
              : NaN;
            const remainingMs = Number.isFinite(deadlineMs)
              ? deadlineMs - this.now()
              : Number.POSITIVE_INFINITY;
            if (Number.isFinite(remainingMs) && remainingMs < 0) {
              await sql`
                update notification_outbox
                set status = 'dead_letter',
                    las
t_error = 'expired_before_send: telegram_deadline_at passed'
                where id = ${row.id}
              `;
              this.stats.dead += 1;
              logger.warn(
                {
                  component: "outbox-dispatcher",
                  ...lifecycleLogFields(row, lc, this.now(), "dead_expired_before_send"),
                  remainingMs,
                },
                "OUTBOX_DISPATCH expired before send — not delivering late signal",
              );
              return "dead" as const;
            }

            // MINIMUM REMAINING LEAD SAFETY GATE (plan §6): for predictions,
            // refuse to start a send whose residual budget cannot plausibly
            // cover the Telegram leg + finalize + margin. Better to mark
            // MISSED now than to race the round start and suppress after the
            // fact. DELIVERY_MIN_LEAD_MS is configuration, not a guess.
            if (row.type === "prediction" && Number.isFinite(remainingMs) && remainingMs < MIN_REMAINING_LEAD_MS) {
              await sql`
                update notification_outbox
                set status = 'dead_letter',
                    last_error = 'missed: insufficient remaining lead before target start (min_lead_ms gate)'
                where id = ${row.id} and status = 'inflight'
              `;
              this.stats.dead += 1;
              logger.warn(
                {
                  component: "outbox-dispatcher",
                  notificationId: row.notification_id,
                  remainingMs: Math.round(remainingMs),
                  minRemainingLeadMs: MIN_REMAINING_LEAD_MS,
                },
                "OUTBOX_DISPATCH missed — remaining lead below minimum safety budget",
              );
              return "dead" as const;
            }

            // ZERO-RTT TEMPORAL GATE (sep 11): in-memory registry written
            // synchronously at ED/BG handler entry in the same process
            // (worker fencing). The D
B gates below read live_round_state /
            // crash_rounds that LAG the real crash by 1-3s (crash_rounds is
            // persisted detached after the N+1 attempt completes) — in that
            // window a late signal passed auth and delivered into a crashed
            // round. This gate closes it with zero DB round trips; the DB
            // auth below remains the contract for every other case.
            if (row.type === "prediction") {
              const regMeta = (row.metadata ?? {}) as Record<string, unknown>;
              const regTarget =
                (row.target_game_id as string | null) ??
                ((regMeta.targetGameId as string) || (regMeta.target_game_id as string) || null);
              if (regTarget && isTargetPastBettingWindow(regTarget)) {
                await sql`
                  update notification_outbox
                  set status = 'dead_letter',
                      last_error = 'expired_late_signal: target started/crashed (registry gate)'
                  where id = ${row.id} and status = 'inflight'
                `;
                this.stats.dead += 1;
                logger.warn(
                  {
                    component: "outbox-dispatcher",
                    notificationId: row.notification_id,
                    targetGameId: regTarget,
                    ...lifecycleLogFields(row, lc, this.now(), "dead_registry_temporal_gate"),
                  },
                  "OUTBOX_DISPATCH refused by in-memory round registry — target started/crashed",
                );
                return "dead" as const;
              }
            }

            // For predictions: HARD temporal contract — a signal for a target
            // that has ALREADY STARTED is semantically wrong (false-timing).
            // Late delivery is REMOVED (was "delivering late signal anyway").
            //
            // OPTIMIZATION: The in-memory registry gate above (isTargetPastBettingWindow)
            // alread
y checked the temporal validity with zero DB RTT. The authorization
            // UPDATE below only needs to:
            // 1. Verify the row is still inflight (status check)
            // 2. Verify deadline hasn't passed (timestamp check)
            // 3. Write send_started_at (authorization stamp)
            // The live_round_state and crash_rounds subqueries are redundant and
            // expensive (2 additional RTTs per prediction). We've already refused
            // stale targets via the registry gate, so the DB auth can be simplified.
            //
            // POOL-BUDGET FIX: the temporal gate and the send_started_at stamp
            // used to be two separate round trips (SELECT live/crash state,
            // then UPDATE send_started_at). They are now ONE atomic
            // authorization UPDATE: the send_started stamp is only written
            // when the row is still inflight, within deadline. Fail
            // closed: a DB error requeues without sending.
            // CLOCK HYGIENE (forensic report Issue 3): the client send-start
            // clock is captured AFTER the authorization resolves, so
            // telegramSendMs (client accepted - client send-start) measures
            // only the Telegram leg. The server-side send_started_at from the
            // auth RETURNING is the dispatch-leg source of truth.
            let sendStartedServerIso: string | null = null;
            let authorized: { id: number; send_started_at: string | Date }[];
            try {
              authorized = await sql<{ id: number; send_started_at: string | Date }>`
                update notification_outbox o
                set send_started_at = clock_timestamp()
                where o.id = ${row.id}
                  and o.status = 'inflight'
                  and (o.telegram_deadline_at is null or o.telegram_deadline_at > clock_timestamp())
                returning o.id, o.send_started_at
              `;
              lc.sendStartedMs = 
this.now();
              if (authorized.length > 0) {
                sendStartedServerIso = new Date(authorized[0]!.send_started_at).toISOString();
              }
            } catch (authErr) {
              // Fail closed: do not send while authorization is unverifiable.
              await sql`
                update notification_outbox
                set status = 'pending',
                    next_attempt_at = now() + interval '250 milliseconds',
                    last_error = ${'send_auth_db_error: ' + String(authErr).slice(0, 180)}
                where id = ${row.id} and status = 'inflight'
              `.catch(() => undefined);
              this.stats.requeued += 1;
              logger.warn(
                {
                  component: "outbox-dispatcher",
                  notificationId: row.notification_id,
                  error: String(authErr),
                },
                "SEND_AUTH_DB_ERROR — fail-closed; requeued without send",
              );
              return "requeued" as const;
            }
            if (authorized.length === 0) {
              // OPTIMIZATION: Authorization did not match. Since we already checked
              // temporal validity via the in-memory registry gate above, the most
              // likely reasons are: row no longer inflight, or deadline passed.
              // We can determine this without additional DB queries by checking
              // the row's current state. This eliminates 2 RTTs (live_round_state + crash_rounds
              // subqueries) per failed authorization.
              let reason = "row_no_longer_inflight_or_deadline_passed";
              try {
                // Check current row state - single query, no subqueries
                const state = await sql<{
                  status: string;
                  deadline_passed: boolean;
                }>`
                  select o.status,
                    (o.telegram_deadline_at is not null and o.telegram_deadline_at 
<= now()) as deadline_passed
                  from notification_outbox o
                  where o.id = ${row.id}
                `;
                const s = state[0];
                if (s?.deadline_passed) reason = "expired_before_send: telegram_deadline_at passed";
                else if (s && s.status !== "inflight") reason = `row_no_longer_inflight: ${s.status}`;
                
                await sql`
                  update notification_outbox
                  set status = 'dead_letter',
                      last_error = ${reason}
                  where id = ${row.id} and status = 'inflight'
                `.catch(() => undefined);
                this.stats.dead += 1;
                logger.warn(
                  {
                    component: "outbox-dispatcher",
                    notificationId: row.notification_id,
                    targetGameId: row.target_game_id,
                    expiration_reason: reason,
                  },
                  "SIGNAL_EXPIRED — pre-send authorization refused delivery",
                );
                return "dead" as const;
              } catch { /* classification best-effort */ }
              this.stats.dead += 1;
              logger.warn(
                {
                  component: "outbox-dispatcher",
                  notificationId: row.notification_id,
                  type: row.type,
                },
                "OUTBOX_DISPATCH aborted before send — authorization refused",
              );
              return "dead" as const;
            }

            // Cap Telegram timeout by remaining deadline.
            // P0/P1: never start a send whose minimum timeout exceeds the residual budget.
            // If residual < 250ms, expire rather than racing past the semantic deadline.
            if (Number.isFinite(remainingMs) && remainingMs < 250) {
              await sql`
                update notification_outbox
                set status = 'dead_letter',
                 
   last_error = 'expired_insufficient_send_budget'
                where id = ${row.id} and status = 'inflight'
              `;
              this.stats.dead += 1;
              logger.warn(
                {
                  component: "outbox-dispatcher",
                  notificationId: row.notification_id,
                  remainingMs: Math.round(remainingMs),
                },
                "OUTBOX_DISPATCH expired — residual budget too small to send safely",
              );
              return "dead" as const;
            }
            const sendTimeout = Math.min(
              5_000,
              Number.isFinite(remainingMs)
                ? Math.max(50, remainingMs - 50)
                : 5_000,
            );
            

... [Content truncated]