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
import { getWakeStats, notifyOutbox, waitForOutboxWake } from "@/lib/prediction/live/outbox-wake";

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
// Recovery/fallback only — wake path is the primary drain trigger. 500ms
// bounds missed-wake latency without hammering the critical pool (was 2s,
// which alone could account for a large fraction of 2.5–4s outbox lag).
export const TICK_MS = Number(process.env.OUTBOX_TICK_MS ?? 100);
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
 * leg + safety margin, so a send that would straddle the semantic validity
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
  const sendMs =
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
  private getSqlFn: () => Promise<Sql> = getCriticalSql;
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
  async tickOnce(): Promise<{
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
    this.bgRunning = true;
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
    // cleanup lives in recoverStale() on the general/maintenance pool so it
    // never adds DB work to the N+1 critical path. Pre-send atomic temporal
    // authorization + finalization gate remain the safety invariants.
    // RTT FIX (sep 11): the primary claim is ONE statement (picked CTE +
    // UPDATE ... FROM) — atomic by itself, so the explicit BEGIN/COMMIT was
    // two round trips of pure overhead on every dispatch tick (~370ms at
    // ~185ms Neon RTT). Primary runs direct; the legacy fallback (SELECT
    // FOR UPDATE + per-row UPDATEs, genuinely multi-statement) keeps a real
    // transaction.
    const claimLimit =
      lane === "prediction" ? Math.max(1, PREDICTION_BATCH_SIZE) : BATCH_SIZE;
    const claimed = await (async () => {
      try {
        if (lane === "prediction") {
          return await sql<OutboxRow>`
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
        return await sql<OutboxRow>`
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
                  AND p.metadata->>'sourceGameId' = notification_outbox.metadata->>'gameId'
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
        // Genuinely multi-statement → runs inside a real transaction.
        return runInTransaction(sql, async (tx) => {
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
                      and p.metadata->>'sourceGameId' = notification_outbox.metadata->>'gameId'
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
              where id = ${r.id} and status = 'pending' and type <> 'prediction'
            `;
          }
          r.attempt_count = (r.attempt_count ?? 0) + 1;
        }
        return rows;
        });
      }
    })();
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
                    last_error = 'expired_before_send: telegram_deadline_at passed'
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
            // (worker fencing). The DB gates below read live_round_state /
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
            // POOL-BUDGET FIX: the temporal gate and the send_started_at stamp
            // used to be two separate round trips (SELECT live/crash state,
            // then UPDATE send_started_at). They are now ONE atomic
            // authorization UPDATE: the send_started stamp is only written
            // when the row is still inflight, within deadline, and — for
            // predictions — the target has not started or crashed. Fail
            // closed: a DB error requeues without sending.
            // CLOCK HYGIENE (forensic report Issue 3): the client send-start
            // clock is captured AFTER the authorization resolves, so
            // telegramSendMs (client accepted − client send-start) measures
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
                  and (
                    o.type <> 'prediction'
                    or coalesce(o.target_game_id, o.metadata->>'targetGameId', o.metadata->>'target_game_id') is null
                    or (
                      not exists (
                        select 1 from live_round_state lrs
                        where lrs.game_id = coalesce(o.target_game_id, o.metadata->>'targetGameId', o.metadata->>'target_game_id')
                          and lrs.began_at is not null
                          and lrs.began_at <= clock_timestamp()
                      )
                      and not exists (
                        select 1 from crash_rounds cr
                        where cr.game_id = coalesce(o.target_game_id, o.metadata->>'targetGameId', o.metadata->>'target_game_id')
                          and cr.crashed_at is not null
                          and cr.crashed_at <= clock_timestamp()
                      )
                    )
                  )
                returning o.id, o.send_started_at
              `;
              lc.sendStartedMs = this.now();
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
              // Authorization did not match: classify WHY with one follow-up
              // read (rare path) so the row lands in the right terminal state.
              let reason = "row_no_longer_inflight_bg_or_expiry";
              try {
                const meta = (row.metadata ?? {}) as Record<string, unknown>;
                const targetGameId =
                  (row.target_game_id as string | null) ??
                  ((meta.targetGameId as string) ||
                    (meta.target_game_id as string) ||
                    null);
                const state = await sql<{
                  status: string;
                  deadline_passed: boolean;
                  target_started: boolean;
                  target_crashed: boolean;
                }>`
                  select o.status,
                    (o.telegram_deadline_at is not null and o.telegram_deadline_at <= now()) as deadline_passed,
                    exists (
                      select 1 from live_round_state lrs
                      where lrs.game_id = ${targetGameId} and lrs.began_at is not null and lrs.began_at <= now()
                    ) as target_started,
                    exists (
                      select 1 from crash_rounds cr
                      where cr.game_id = ${targetGameId} and cr.crashed_at is not null and cr.crashed_at <= now()
                    ) as target_crashed
                  from notification_outbox o
                  where o.id = ${row.id}
                `;
                const s = state[0];
                if (s?.target_started) reason = "expired_late_signal: target started before delivery";
                else if (s?.target_crashed) reason = "target_already_crashed_before_delivery";
                else if (s?.deadline_passed) reason = "expired_before_send: telegram_deadline_at passed";
                else if (s && s.status !== "inflight") reason = `row_no_longer_inflight: ${s.status}`;
                if (s?.target_started || s?.target_crashed || s?.deadline_passed || (s && s.status !== "inflight")) {
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
                      targetGameId,
                      expiration_reason: reason,
                    },
                    "SIGNAL_EXPIRED — pre-send authorization refused delivery",
                  );
                  return "dead" as const;
                }
              } catch { /* classification best-effort */ }
              this.stats.dead += 1;
              logger.warn(
                {
                  component: "outbox-dispatcher",
                  notificationId: row.notification_id,
                  type: row.type,
                },
                "OUTBOX_DISPATCH aborted before send — authorization refused (BG/expiry)",
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
            // send_started_at is now stamped by the atomic authorization
            // UPDATE above — no separate pre-send round trip.
            // P0: prediction lane uses primary-first so a slow secondary
            // Telegram destination cannot occupy the dispatcher slot after
            // the user already accepted the signal.
            const sendResults =
              row.type === "prediction"
                ? await sendTelegramMessagePrimaryFirst(row.content, {
                    timeout: sendTimeout,
                  })
                : await sendTelegramMessage(row.content, {
                    timeout: sendTimeout,
                  });
            const allOk =
              sendResults.length > 0 && sendResults.every((r) => r.ok);
            const anyOk = sendResults.some((r) => r.ok);
            // Fan-out is independently addressed. If at least one destination
            // accepted the message, mark the row delivered instead of retrying
            // and duplicating it to healthy chats because another chat id is stale.
            // For prediction + primary-first, anyOk means primary accepted.
            if (allOk || anyOk) {
              if (!allOk && row.type !== "prediction") {
                const failures = sendResults
                  .filter((r) => !r.ok)
                  .map((r) => ({ chatId: r.chatId, status: r.status, error: r.error ?? "send_failed" }));
                logger.warn(
                  {
                    component: "outbox-dispatcher",
                    notificationId: row.notification_id,
                    failures,
                    deliveredChats: sendResults.filter((r) => r.ok).map((r) => r.chatId),
                  },
                  "partial Telegram fan-out — delivered to healthy chats",
                );
              }
              const acceptedMs = this.now();
              lc.telegramAcceptedMs = acceptedMs;
              // P0 TOCTOU fix: finalize ONLY if still inflight and within deadline.
              // BG may have set dead_letter while Telegram was in flight — do not
              // overwrite that expiration with delivered.
              // LATE ACCEPTANCE GATE (plan §7): for predictions the finalize
              // ALSO refuses when the target started/crashed between
              // authorization and Telegram acceptance — the invariant
              // telegram_accepted_at < target_round_started_at is enforced at
              // the last write, not just at authorization time.
              // CLOCK HYGIENE: delivered_at / telegram_accepted_at are stamped
              // SERVER-side (clock_timestamp) — the authoritative lead-time
              // chain (target_started − telegram_accepted) must live on one
              // clock. Client clock survives only in the Telegram-send
              // duration (acceptedMs − lc.sendStartedMs).
              const finalized =
                row.type === "prediction"
                  ? await sql<{ id: number; telegram_accepted_at: string | Date }>`
                      update notification_outbox
                      set status = 'delivered',
                          delivered_at = clock_timestamp(),
                          telegram_accepted_at = clock_timestamp(),
                          last_error = null
                      where id = ${row.id}
                        and status = 'inflight'
                        and (telegram_deadline_at is null or telegram_deadline_at > now())
                        and not exists (
                          select 1 from live_round_state lrs
                          where lrs.game_id = coalesce(${row.target_game_id ?? null}, metadata->>'targetGameId', metadata->>'target_game_id')
                            and lrs.began_at is not null
                            and lrs.began_at <= clock_timestamp()
                        )
                        and not exists (
                          select 1 from crash_rounds cr
                          where cr.game_id = coalesce(${row.target_game_id ?? null}, metadata->>'targetGameId', metadata->>'target_game_id')
                            and cr.crashed_at is not null
                            and cr.crashed_at <= clock_timestamp()
                        )
                      returning id, telegram_accepted_at
                    `
                  : await sql<{ id: number; telegram_accepted_at: string | Date }>`
                      update notification_outbox
                      set status = 'delivered',
                          delivered_at = clock_timestamp(),
                          telegram_accepted_at = clock_timestamp(),
                          last_error = null
                      where id = ${row.id}
                        and status = 'inflight'
                        and (telegram_deadline_at is null or telegram_deadline_at > now())
                      returning id, telegram_accepted_at
                    `;
              if (finalized.length === 0) {
                // Race lost to BG/expiry — Telegram may have been accepted, but
                // we must not record a valid delivery after semantic invalidation.
                this.stats.dead += 1;
                // LATE classification (plan §7): if the target started between
                // auth and acceptance, this is not merely "suppressed" — it is
                // a LATE acceptance. Persist the outcome and exact reason.
                if (row.type === "prediction") {
                  const targetGameIdLate =
                    (row.target_game_id as string | null) ??
                    ((row.metadata as Record<string, unknown> | null)?.targetGameId as string) ??
                    null;
                  let lateAccepted = false;
                  if (targetGameIdLate) {
                    try {
                      const st = await sql<{ started: boolean }>`
                        select (
                          exists (
                            select 1 from live_round_state lrs
                            where lrs.game_id = ${targetGameIdLate}
                              and lrs.began_at is not null and lrs.began_at <= clock_timestamp()
                          ) or exists (
                            select 1 from crash_rounds cr
                            where cr.game_id = ${targetGameIdLate}
                              and cr.crashed_at is not null and cr.crashed_at <= clock_timestamp()
                          )
                        ) as started
                      `;
                      lateAccepted = st[0]?.started === true;
                    } catch { /* classification best-effort */ }
                  }
                  await sql`
                    update notification_outbox
                    set status = 'dead_letter',
                        last_error = 'late_acceptance: telegram_accepted_at >= target_round_started_at — never a successful delivery'
                    where id = ${row.id} and status = 'inflight'
                  `.catch(() => undefined);
                  if (lateAccepted) {
                    setImmediate(() => {
                      void import("@/lib/prediction/live/delivery-forensics")
                        .then(({ persistDeliveryOutcome }) =>
                          persistDeliveryOutcome(
                            sql,
                            row.notification_id,
                            "LATE",
                            null,
                          ),
                        )
                        .catch(() => undefined);
                    });
                  }
                  logger.warn(
                    {
                      component: "outbox-dispatcher",
                      ...lifecycleLogFields(row, lc, acceptedMs, "late_acceptance"),
                      notificationId: row.notification_id,
                      type: row.type,
                      lateAccepted,
                    },
                    lateAccepted
                      ? "OUTBOX_DISPATCH telegram accepted AFTER target start — recorded LATE, never delivered"
                      : "OUTBOX_DISPATCH telegram accepted but row no longer inflight/valid — delivery suppressed (BG or deadline won)",
                  );
                  return "dead" as const;
                }
                logger.warn(
                  {
                    component: "outbox-dispatcher",
                    ...lifecycleLogFields(row, lc, acceptedMs, "suppressed_after_telegram"),
                    notificationId: row.notification_id,
                    type: row.type,
                  },
                  "OUTBOX_DISPATCH telegram accepted but row no longer inflight/valid — delivery suppressed (BG or deadline won)",
                );
                return "dead" as const;
              }
              this.stats.delivered += 1;
              // Server-accepted ISO from the finalize RETURNING — the
              // authoritative clock for every downstream lead-time number.
              const acceptedAt = finalized[0]
                ? new Date(finalized[0].telegram_accepted_at).toISOString()
                : new Date(acceptedMs).toISOString();
              logger.info(
                {
                  component: "outbox-dispatcher",
                  ...lifecycleLogFields(row, lc, acceptedMs, "delivered"),
                  remainingBudgetMs: Number.isFinite(remainingMs)
                    ? Math.round(remainingMs)
                    : null,
                  // Per-destination Telegram timing (plan §13): first accept,
                  // slowest destination, full fan-out completion — measured,
                  // so timeout policy is never adjusted on speculation.
                  telegramDestinations: sendResults.map((r) => ({
                    chatId: r.chatId,
                    ok: r.ok,
                    status: r.status ?? null,
                    durationMs: r.durationMs ?? null,
                  })),
                  firstAcceptMs:
                    sendResults.filter((r) => r.ok && r.durationMs != null).length > 0
                      ? Math.round(
                          Math.min(
                            ...sendResults
                              .filter((r) => r.ok && r.durationMs != null)
                              .map((r) => r.durationMs!),
                          ),
                        )
                      : null,
                  slowestDestinationMs:
                    sendResults.some((r) => r.durationMs != null)
                      ? Math.round(
                          Math.max(
                            ...sendResults
                              .filter((r) => r.durationMs != null)
                              .map((r) => r.durationMs!),
                          ),
                        )
                      : null,
                },
                "OUTBOX_DISPATCH delivered",
              );
              logger.info(
                {
                  component: "outbox-dispatcher",
                  event: "OUTBOX_DISPATCH",
                  notificationType:
                    row.type === "prediction"
                      ? "PREDICTION_SIGNAL"
                      : row.type === "validation"
                        ? "RESULT_NOTIFICATION"
                        : "OTHER_NOTIFICATION",
                  predictionId: (row.metadata as Record<string, unknown> | null)?.predictionId ?? null,
                  notificationId: row.notification_id,
                  sourceGameId: (row.metadata as Record<string, unknown> | null)?.sourceGameId ?? null,
                  targetGameId: row.target_game_id ?? (row.metadata as Record<string, unknown> | null)?.targetGameId ?? null,
                  type: row.type,
                },
                "OUTBOX_DISPATCH correlated",
              );
              // Forensics: classify ON_TIME/LATE/UNKNOWN vs target start if known.
              // POOL-BUDGET FIX: forensics is analytics, never a delivery gate.
              // It used to be awaited on the dispatcher's CRITICAL sql — an
              // analytics read+write series sitting between Telegram accepted
              // and the dispatcher task returning. It now runs detached on the
              // GENERAL pool; delivery success/failure is decided before this.
              setImmediate(() => {
                void (async () => {
                  const { recordDeliveredForensics } = await import(
                    "@/lib/prediction/live/delivery-forensics"
                  );
                  const generalSql = await getSql();
                  const metaF = (row.metadata ?? {}) as Record<string, unknown>;
                  await recordDeliveredForensics(generalSql, {
                    notificationId: row.notification_id,
                    predictionId:
                      typeof metaF.predictionId === "string" ? metaF.predictionId : null,
                    correlationId:
                      typeof metaF.correlationId === "string" ? metaF.correlationId : null,
                    sourceGameId:
                      typeof metaF.sourceGameId === "string" ? metaF.sourceGameId : null,
                    targetGameId:
                      (row.target_game_id as string | null) ??
                      (typeof metaF.targetGameId === "string" ? metaF.targetGameId : null),
                    createdAt: row.created_at,
                    // Canonical per-row dispatch start, stamped server-side by
                    // the claim query — never derived from batch timing.
                    dispatchClaimedAt: row.dispatch_claimed_at
                      ? new Date(row.dispatch_claimed_at).toISOString()
                      : null,
                    sendStartedAtMs: lc.sendStartedMs,
                    sendStartedAtServerIso: sendStartedServerIso,
                    telegramAcceptedAtMs: acceptedMs,
                    serverAcceptedAtIso: acceptedAt,
                  });
                })().catch(() => {
                  /* soft — forensics must never break delivery */
                });
              });
              logger.info(
                {
                  component: "timing",
                  path: "outbox_delivery",
                  notificationId: row.notification_id,
                  type: row.type,
                  deliveryAcceptedAt: acceptedAt,
                  remainingBudgetMs: Number.isFinite(remainingMs)
                    ? Math.round(remainingMs)
                    : null,
                },
                "delivery accepted",
              );
              try {
                const { outboxDeliveryMs, outboxTotalDeliveryMs } = await import(
                  "@/lib/observability/performance/latency"
                );
                // Honest measurement: actual claim → Telegram accepted for THIS
                // attempt (was previously approximated from next_attempt_at).
                outboxDeliveryMs.observe(Math.max(0, acceptedMs - claimClientMs));
                // End-to-end: row INSERT → Telegram accepted (splits queue wait
                // from send when read next to outboxDeliveryMs).
                const createdMs = new Date(row.created_at).getTime();
                if (Number.isFinite(createdMs)) {
                  outboxTotalDeliveryMs.observe(Math.max(0, acceptedMs - createdMs));
                }
                // Fix 13: feed the delivery leg into the latency trace chain
                const { recordDeliveryLatency } = await import(
                  "@/lib/prediction/live/latency-trace"
                );
                recordDeliveryLatency(Math.max(0, acceptedMs - claimClientMs));
              } catch { /* metrics optional */ }

              // Phase 19 — record lead times when target_round_started_at is known
              // POOL-BUDGET FIX: runs detached on the GENERAL pool. The block
              // used to close over the dispatcher's critical-pool `sql`, so
              // lead-time analytics competed with claim/finalize on the
              // reserved pool. Delivery never depends on this completing.
              setImmediate(() => {
                void (async () => {
                const generalSql = await getSql();
                const meta = (row.metadata ?? {}) as Record<string, unknown>;
                const targetGameId =
                  (meta.targetGameId as string) ||
                  (meta.target_game_id as string) ||
                  null;
                if (targetGameId) {
                  const predRows = await generalSql<{
                    requested_at: string | Date | null;
                    target_round_started_at: string | Date | null;
                  }>`
                    SELECT requested_at, target_round_started_at
                    FROM pending_predictions
                    WHERE target_game_id = ${targetGameId}
                    ORDER BY requested_at DESC
                    LIMIT 1
                  `.catch(() => [] as { requested_at: string | Date | null; target_round_started_at: string | Date | null }[]);
                  let startedAt: string | Date | null =
                    predRows[0]?.target_round_started_at ?? null;
                  const generatedAt: string | Date | null =
                    predRows[0]?.requested_at ?? null;
                  if (!startedAt) {
                    const live = await generalSql<{ began_at: string | Date | null }>`
                      SELECT began_at FROM live_round_state
                      WHERE game_id = ${targetGameId} LIMIT 1
                    `.catch(() => [] as { began_at: string | Date | null }[]);
                    startedAt = live[0]?.began_at ?? null;
                  }
                  if (startedAt && generatedAt) {
                    const { recordLeadTimes } = await import(
                      "@/lib/observability/metrics/lifecycle-metrics"
                    );
                    const lt = recordLeadTimes({
                      predictionGeneratedAt: generatedAt,
                      notificationSentAt: acceptedAt,
                      nextRoundStartAt: startedAt,
                    });
                    logger.info(
                      {
                        component: "timing",
                        path: "lead_time",
                        targetGameId,
                        predictionLeadMs: lt.predictionLeadMs,
                        notificationLeadMs: lt.notificationLeadMs,
                        stale: lt.stale,
                      },
                      lt.stale
                        ? "stale signal: notification after target start"
                        : "lead times recorded",
                    );
                  }
                }
              })().catch(() => {
                /* soft — lead time must never break delivery */
              });
              });

              return "delivered" as const;
            }
            // Deadline-aware retry: if remaining budget too small for another attempt, dead-letter
            if (Number.isFinite(remainingMs) && remainingMs < 300) {
              await sql`
                update notification_outbox
                set status = 'dead_letter',
                    send_started_at = ${new Date(lc.sendStartedMs).toISOString()}::timestamptz,
                    last_error = ${'expired_after_failed_send: ' + (sendResults.find((r) => !r.ok)?.error ?? 'send_failed')}
                where id = ${row.id}
              `;
              this.stats.dead += 1;
              logger.warn(
                {
                  component: "outbox-dispatcher",
                  ...lifecycleLogFields(row, lc, this.now(), "dead_expired_after_failed_send"),
                },
                "OUTBOX_DISPATCH dead-lettered: no budget for another attempt",
              );
              return "dead" as const;
            }
            const updated = await this.handleFailure(sql, row, sendResults, lc);
            return updated;
          } catch (e) {
            this.stats.lastError = String(e);
            logger.warn(
              {
                component: "outbox-dispatcher",
                notificationId: row.notification_id,
                error: String(e),
              },
              "send threw; treating as retryable",
            );
            const updated = await this.handleFailure(sql, row, [], lc);
            return updated;
          }
        }),
      );
      for (const r of results) {
        if (r === "delivered") delivered += 1;
        else if (r === "dead") dead += 1;
        else requeued += 1;
      }
    }
    if (claimed.length > 0) {
      // Per-tick observability: proves the dispatcher is alive, how much it
      // claims per pass, and how long a full drain takes (identifies whether
      // one slow Telegram request dominates a pass — chunk parallelism makes
      // drain time ≈ ceil(claimed/parallelism) × slowest send).
      // Wake-to-dispatch instrumentation (plan §10): notify→claim latency
      // proves whether notifyOutbox() is immediate but the dispatcher busy.
      const wake = getWakeStats();
      const lastNotifyAt =
        lane === "prediction" ? wake.lastPredictionNotifyAt : wake.lastNormalNotifyAt;
      const notifyToClaimMs =
        lastNotifyAt != null
          ? Math.max(0, claimClientMs - lastNotifyAt)
          : null;
      logger.info(
        {
          component: "outbox-dispatcher",
          lane,
          claimed: claimed.length,
          batchSize: BATCH_SIZE,
          parallelism,
          delivered,
          dead,
          requeued,
          drainMs: Math.round(this.now() - tickStartMs),
          notifyToClaimMs: notifyToClaimMs != null ? Math.round(notifyToClaimMs) : null,
          wakeKindCounts: {
            prediction: wake.predictionWakeCount,
            normal: wake.normalWakeCount,
          },
        },
        "OUTBOX_TICK drained claimed rows",
      );
    }
    return { delivered, dead, requeued };
  }

  /** Recover stale INFLIGHT (legacy status) or stuck pending rows.
   * PLAN §8: recovery is MAINTENANCE — it runs on the GENERAL pool and must
   * never compete with prediction persistence/dispatch for critical-pool
   * capacity. Stale lease semantics, idempotency and row ownership are
   * unchanged. */
  async recoverStale(): Promise<number> {
    const sql = await getSql();

    // Health signal: pending rows older than 5s mean the dispatcher is lagging.
    // Split into CLAIMABLE (dispatcher can and should have taken them) vs
    // EXPIRED (past telegram_deadline_at — the claim query will never take
    // them; they are zombie rows awaiting recover cleanup). Conflating the
    // two made healthy dispatches look stalled whenever a non-prediction row
    // missed its deadline.
    try {
      const backlog = await sql<{
        claimable: number;
        expired: number;
        oldest_claimable_ms: number | null;
        oldest_expired_ms: number | null;
      }>`
        SELECT count(*) FILTER (WHERE telegram_deadline_at IS NULL OR telegram_deadline_at > now())::int AS claimable,
               count(*) FILTER (WHERE telegram_deadline_at IS NOT NULL AND telegram_deadline_at <= now())::int AS expired,
               EXTRACT(EPOCH FROM (now() - min(next_attempt_at) FILTER (WHERE telegram_deadline_at IS NULL OR telegram_deadline_at > now()))) * 1000 AS oldest_claimable_ms,
               EXTRACT(EPOCH FROM (now() - min(created_at) FILTER (WHERE telegram_deadline_at IS NOT NULL AND telegram_deadline_at <= now()))) * 1000 AS oldest_expired_ms
        FROM notification_outbox
        WHERE status = 'pending'
          AND next_attempt_at < now() - interval '5 seconds'
      `.catch(
        () =>
          [] as {
            claimable: number;
            expired: number;
            oldest_claimable_ms: number | null;
            oldest_expired_ms: number | null;
          }[],
      );
      const row = backlog[0];
      if (row && (row.claimable > 0 || row.expired > 0)) {
        this.stats.backlogWarnings += 1;
        if (row.claimable > 0) {
          logger.warn(
            {
              component: "outbox-dispatcher",
              kind: "claimable_backlog",
              pendingOlderThan5s: row.claimable,
              expiredPending: row.expired,
              oldestClaimableMs:
                row.oldest_claimable_ms != null
                  ? Math.round(Number(row.oldest_claimable_ms))
                  : null,
            },
            "outbox backlog: claimable pending rows older than 5s — dispatcher may be stalled",
          );
        } else {
          logger.warn(
            {
              component: "outbox-dispatcher",
              kind: "expired_backlog",
              expiredPending: row.expired,
              oldestExpiredMs:
                row.oldest_expired_ms != null
                  ? Math.round(Number(row.oldest_expired_ms))
                  : null,
            },
            "outbox backlog: expired pending rows (unclaimable, awaiting recovery cleanup)",
          );
        }
      }
    } catch {
      /* soft */
    }

    // Dead-letter overdue rows past telegram deadline (or very old, any type).
    // Previously restricted to type='prediction', which let expired
    // validation/alert/summary rows strand as unclaimable pending zombies and
    // trip the backlog health check forever. Re-queueing them only produces
    // permanent failure / noise and never helps.
    try {
      await sql`
        update notification_outbox
        set status = 'dead_letter',
            last_error = coalesce(last_error, '') || ' [expired on recover: past deadline or stale]'
        where status in ('pending', 'inflight')
          and (
            (telegram_deadline_at is not null and telegram_deadline_at < now())
            or created_at < now() - interval '2 minutes'
          )
      `;
    } catch { /* soft */ }

    // NO-RESURRECT + claim-time sweep moved off hot path (P0):
    // Dead-letter pending or inflight predictions whose target already
    // started/crashed. This used to run inside every prediction claim TX;
    // it now runs only on the maintenance/general pool so it cannot add
    // latency to the N+1 critical path. Pre-send authorization remains the
    // final gate for any row that reaches dispatch.
    try {
      await sql`
        update notification_outbox
        set status = 'dead_letter',
            last_error = 'expired_late_signal: target started/crashed (recovery temporal gate)'
        where status in ('pending', 'inflight')
          and type = 'prediction'
          and coalesce(target_game_id, metadata->>'targetGameId', metadata->>'target_game_id') is not null
          and coalesce(target_game_id, metadata->>'targetGameId', metadata->>'target_game_id') in (
            select game_id from live_round_state
            where began_at is not null and began_at <= clock_timestamp()
            union
            select game_id from crash_rounds
            where crashed_at is not null and crashed_at <= clock_timestamp()
          )
      `;
    } catch { /* soft */ }

    // Reset stuck inflight rows (crash mid-send) and very-old pending rows.
    const result = await sql<{ id: number }>`
      update notification_outbox
      set status = 'pending',
          last_error = coalesce(last_error, '') || ' [recovered from inflight]',
          next_attempt_at = now()
      where status = 'inflight'
        and updated_at < now() - (${STALE_INFLIGHT_MS}::int * interval '1 millisecond')
      returning id
    `;
    const n = result.length;
    if (n > 0) {
      this.stats.recoveredInflight += n;
      logger.info(
        { component: "outbox-dispatcher", recovered: n },
        "recovered stale inflight/pending outbox rows",
      );
    }
    return n;
  }


  private async handleFailure(
    sql: Sql,
    row: OutboxRow,
    results: SendResult[],
    lc?: RowLifecycle,
  ): Promise<"dead" | "requeued"> {
    // attempt_count was already incremented at claim time
    const attempts = row.attempt_count;
    const firstFailure = results.find((r) => !r.ok);
    // Only true client errors are permanent. 408/425/429 and 5xx retry.
    // 401 often means transient token misread — retry a few times before dead.
    const errText = String(firstFailure?.error ?? "");
    // not_configured / network / timeout are operational — never permanent.
    const isOpsMiss =
      errText === "not_configured" ||
      errText.includes("not_configured") ||
      firstFailure?.status === 0 ||
      firstFailure?.status == null ||
      errText.startsWith("timeout_") ||
      errText.includes("network") ||
      errText.includes("ECONNRESET") ||
      errText.includes("fetch failed") ||
      errText.includes("socket hang up");
    const isPermanent =
      !isOpsMiss &&
      firstFailure != null &&
      typeof firstFailure.status === "number" &&
      (firstFailure.status === 400 ||
        firstFailure.status === 403 ||
        firstFailure.status === 404 ||
        firstFailure.status === 422);
    const lastError = firstFailure?.error ?? "send_failed";

    if (isPermanent || attempts >= MAX_ATTEMPTS) {
      await sql`
        update notification_outbox
        set status = 'dead_letter',
            last_error = ${lastError},
            next_attempt_at = now(),
            send_started_at = ${lc?.sendStartedMs != null ? new Date(lc.sendStartedMs).toISOString() : null}::timestamptz,
            telegram_accepted_at = ${lc?.telegramAcceptedMs != null ? new Date(lc.telegramAcceptedMs).toISOString() : null}::timestamptz
        where id = ${row.id}
      `;
      this.stats.dead += 1;
      if (lc) {
        logger.info(
          {
            component: "outbox-dispatcher",
            ...lifecycleLogFields(row, lc, this.now(), "dead"),
            httpStatus: firstFailure?.status ?? null,
            providerError: lastError,
          },
          "OUTBOX_DISPATCH dead-lettered",
        );
      }
      // Rich diagnostics for operators (Diagnosis P0-6)
      let telegramChatId: string | null = firstFailure?.chatId ?? null;
      let predictionId: string | null = null;
      let targetGameId: string | null = null;
      try {
        const meta = row.metadata ?? {};
        const contentRaw = row.content;
        const content =
          typeof contentRaw === "string"
            ? (() => {
                try {
                  return JSON.parse(contentRaw) as Record<string, unknown>;
                } catch {
                  return {} as Record<string, unknown>;
                }
              })()
            : typeof contentRaw === "object" && contentRaw
              ? (contentRaw as Record<string, unknown>)
              : {};
        telegramChatId =
          telegramChatId ??
          (meta.chatId as string) ??
          (meta.chat_id as string) ??
          (content.chatId as string) ??
          null;
        predictionId =
          (meta.predictionId as string) ??
          (meta.prediction_id as string) ??
          (content.predictionId as string) ??
          null;
        targetGameId =
          (meta.targetGameId as string) ??
          (meta.target_game_id as string) ??
          (content.targetGameId as string) ??
          null;
      } catch { /* ignore */ }
      logger.warn(
        {
          component: "outbox-dispatcher",
          outboxId: row.id,
          notificationId: row.notification_id,
          predictionId,
          targetGameId,
          attempts,
          httpStatus: firstFailure?.status ?? null,
          providerError: lastError,
          errorCode:
            typeof firstFailure?.status === "number"
              ? `http_${firstFailure.status}`
              : "send_failed",
          telegramChatId,
          isPermanent,
        },
        isPermanent ? "permanent failure; dead-lettering" : "max attempts reached; dead-lettering",
      );
      return "dead";
    }

    const backoff = Math.min(BASE_BACKOFF_MS * Math.pow(2, attempts - 1), MAX_BACKOFF_MS);
    await sql`
      update notification_outbox
      set status = 'pending',
          last_error = ${lastError},
          next_attempt_at = now() + (${backoff}::int * interval '1 millisecond'),
          send_started_at = ${lc?.sendStartedMs != null ? new Date(lc.sendStartedMs).toISOString() : null}::timestamptz,
          telegram_accepted_at = ${lc?.telegramAcceptedMs != null ? new Date(lc.telegramAcceptedMs).toISOString() : null}::timestamptz,
          dispatch_claimed_at = null
      where id = ${row.id}
    `;
    this.stats.requeued += 1;
    if (lc) {
      logger.info(
        {
          component: "outbox-dispatcher",
          ...lifecycleLogFields(row, lc, this.now(), "requeued"),
          backoffMs: backoff,
          httpStatus: firstFailure?.status ?? null,
          providerError: lastError,
        },
        "OUTBOX_DISPATCH requeued for retry",
      );
    }
    return "requeued";
  }

  private async drainLoop(): Promise<void> {
    // First pass: drain any rows already pending at start (no wake kinds yet).
    let wake: { prediction: boolean; normal: boolean } | null = {
      prediction: true,
      normal: true,
    };
    while (this.running) {
      // Fencing gate (fix plan Phase 1): a worker that lost authority must not
      // dispatch. Covers the window between lock loss and cascade teardown.
      if (!isAuthoritative()) {
        logger.warn(
          { component: "outbox-dispatcher" },
          "dispatcher stopping — worker authority lost",
        );
        this.running = false;
        break;
      }
      try {
        // PREDICTION LANE: always checked first. A prediction wake resolves
        // immediately and this claim runs before any validation/result work.
        // CO-DELIVERY FIX 2: recoverStale was awaited INLINE before this
        // claim every 10th tick, adding its full general-pool round trips
        // (~1s on slow rounds) to the N+1 dispatch path — dispatch landed
        // 1.6-2.2s after enqueue, the WIN/LOSS became claimable meanwhile,
        // and slow-round predictions missed target-start (LATE refusals).
        // Recovery now runs AFTER the prediction claim; it is maintenance,
        // one tick of extra staleness is immaterial (STALE_INFLIGHT_MS 30s).
        await this.processLane("prediction");
        // P2.4: Throttle recoverStale to every 10 ticks (250ms at 25ms tick)
        // instead of every tick. Stale recovery is non-critical and the DB
        // UPDATE it runs was consuming ~5-10ms on every tick.
        // PLAN §8: recoverStale now runs on the GENERAL pool (maintenance),
        // never on the prediction-critical pool.
        if (this.stats.tickCount % 10 === 0) {
          // DETACHED (sep 11 advisor D12): recovery is maintenance — awaiting
          // it inline delayed the loop's return to the wake wait, so a
          // prediction wake latching during a slow sweep waited for the full
          // general-pool round trips. Nothing downstream needs it synchronously.
          void this.recoverStale().catch(() => undefined);
        }
        // DURABLE FORENSIC RECONCILIATION (remediation §5-§7): throttled
        // sweep that repairs stored delivery_outcome from the authoritative
        // raw timestamps. Detached — forensic repair never occupies the
        // scheduling path. Every 30 ticks (~60s at a 2s fallback tick).
        if (this.stats.tickCount % 30 === 0) {
          void this.reconcileForensics();
        }
        // NORMAL LANE: only when woken for normal work or on timer recovery
        // (wake == null). Skip on pure prediction wakes so WIN/LOSS does not
        // race the N+1 signal on the same ED tick (ordering fix).
        const runNormal =
          wake == null || wake.normal || (!wake.prediction && !wake.normal);
        if (runNormal) {
          this.runBackgroundDetached();
        }
      } catch (e) {
        this.stats.lastError = String(e);
        logger.error(
          { component: "outbox-dispatcher", error: String(e) },
          "tick error",
        );
      }
      if (!this.running) break;
      wake = await this.waitForNextTick();
    }
  }

  /**
   * Wait between ticks: resolves on a producer wake (immediate drain) or
   * after TICK_MS, whichever first. The coalescing wake channel guarantees
   * bursts collapse into a single immediate tick — never overlapping ones.
   * Returns the latched wake kinds so the caller knows which lanes need
   * work (lane-aware wake, plan §11).
   */
  private async waitForNextTick(): Promise<{
    prediction: boolean;
    normal: boolean;
  } | null> {
    try {
      return await waitForOutboxWake(TICK_MS);
    } catch {
      // wake module unavailable in some test contexts — plain timer fallback
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, TICK_MS);
        t.unref?.();
      });
      return null;
    }
  }
}