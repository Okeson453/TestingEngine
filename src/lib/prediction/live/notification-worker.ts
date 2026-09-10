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
import { getSql, type Sql } from "@/lib/db";
import { runInTransaction } from "@/lib/prediction/live/tx";
import { sendTelegramMessage, type SendResult } from "@/lib/notifications/telegram";
import { getLogger } from "@/lib/observability/logger";
import { isAuthoritative } from "@/lib/prediction/live/fencing";

const logger = getLogger("outbox-dispatcher");

/** Tunables (env-overridable for tests). */
// P2.5: Reduced default from 50ms to 25ms to halve max queue wait time.
export const TICK_MS = Number(process.env.OUTBOX_TICK_MS ?? 10);
export const BATCH_SIZE = Number(process.env.OUTBOX_BATCH_SIZE ?? 16);
export const STALE_INFLIGHT_MS = Number(process.env.OUTBOX_STALE_MS ?? 30_000);
export const MAX_ATTEMPTS = Number(process.env.OUTBOX_MAX_ATTEMPTS ?? 5);
/**
 * Max concurrent Telegram sends within a claimed batch (P0 / 6.5).
 * Batch 3 fix: with a ~800ms Neon RTT per DB round-trip, parallelism 2 meant
 * a full batch took multiple seconds per drain pass — the direct cause of the
 * repeated "outbox backlog >5s" warnings. 8 concurrent sends drain a full
 * batch in roughly one RTT. Env-overridable as before.
 */
export const BATCH_PARALLELISM = Number(process.env.OUTBOX_BATCH_PARALLELISM ?? 8);
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
  private getSqlFn: () => Promise<Sql> = getSql;
  private now: () => number = Date.now;

  constructor(opts?: { getSqlFn?: () => Promise<Sql>; now?: () => number }) {
    if (opts?.getSqlFn) this.getSqlFn = opts.getSqlFn;
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

  /** Run a single drain pass. Returns the result for testability. */
  async tickOnce(): Promise<{
    recovered: number;
    delivered: number;
    dead: number;
    requeued: number;
  }> {
    this.stats.tickCount += 1;
    let delivered = 0;
    let dead = 0;
    let requeued = 0;
    const tickStartMs = this.now();

    const sql = await this.getSqlFn();

    // Claim batch: SELECT FOR UPDATE SKIP LOCKED → set status=inflight → COMMIT.
    // Exclude rows past telegram_deadline_at so we never deliver "predicts the past".
    // Single round-trip claim inside one TX (UPDATE…FROM…RETURNING).
    // At ~800ms Neon RTT, N per-row UPDATEs were costing seconds per tick.
    const claimed = await runInTransaction(sql, async (tx) => {
      try {
        return await tx<OutboxRow>`
          WITH picked AS (
            SELECT id
            FROM notification_outbox
            WHERE status = 'pending'::text
              AND next_attempt_at <= now()
              AND (telegram_deadline_at IS NULL OR telegram_deadline_at > now())
            ORDER BY
              CASE WHEN type = 'prediction' THEN 0 ELSE 1 END,
              priority DESC,
              next_attempt_at ASC,
              id ASC
            LIMIT ${BATCH_SIZE}
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
                    n.telegram_deadline_at, n.priority, n.target_game_id
        `;
      } catch {
        const rows = await tx<OutboxRow>`
          select id, notification_id, type, content, metadata, status, attempt_count,
                 next_attempt_at, created_at, telegram_deadline_at, priority
          from notification_outbox
          where status = 'pending'::text
            and next_attempt_at <= now()
            and (telegram_deadline_at is null or telegram_deadline_at > now())
          order by
            case when type = 'prediction' then 0 else 1 end,
            priority desc,
            next_attempt_at asc,
            id asc
          limit ${BATCH_SIZE}
          for update skip locked
        `;
        for (const r of rows) {
          await tx`
            update notification_outbox
            set status = 'inflight', attempt_count = attempt_count + 1,
                dispatch_claimed_at = now()
            where id = ${r.id} and status = 'pending'
          `;
          r.attempt_count = (r.attempt_count ?? 0) + 1;
        }
        return rows;
      }
    });

    if (claimed.length > 0) {
      this.stats.claimed += claimed.length;
    }
    // Client-side claim clock: consistent basis for all in-process durations
    // (DB now() vs client clock skew must not pollute queue_wait/dispatch ms).
    const claimClientMs = this.now();

    // Parallel dispatch bounded by BATCH_PARALLELISM (P0 / 6.5)
    const parallelism = Math.max(1, BATCH_PARALLELISM);
    for (let i = 0; i < claimed.length; i += parallelism) {
      const chunk = claimed.slice(i, i + parallelism);
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

            // For predictions: HARD temporal contract — a signal for a target
            // that has ALREADY STARTED is semantically wrong (false-timing).
            // Late delivery is REMOVED (was "delivering late signal anyway").
            // The check is unconditional now: the old remainingMs<800 /
            // FORCE-flag gate made the safety net inactive for exactly the
            // rows that need it (creation-relative 8s deadline outlives the
            // target's start). Cost: one indexed lookup per prediction send.
            if (row.type === "prediction") {
              try {
                const meta = (row.metadata ?? {}) as Record<string, unknown>;
                const targetGameId =
                  (row.target_game_id as string | null) ??
                  ((meta.targetGameId as string) ||
                    (meta.target_game_id as string) ||
                    null);
                if (targetGameId) {
                  // P1.7: Consolidated pre-send check — 1 query instead of 2.
                  // LEFT JOIN live_round_state and crash_rounds in a single pass.
                  const live = await sql<{ began_at: string | Date | null; crashed_at: string | Date | null }>`
                    SELECT lrs.began_at, cr.crashed_at
                    FROM (SELECT 1) AS dummy
                    LEFT JOIN live_round_state lrs ON lrs.game_id = ${targetGameId}
                    LEFT JOIN crash_rounds cr ON cr.game_id = ${targetGameId}
                    LIMIT 1
                  `.catch(() => [] as { began_at: string | Date | null; crashed_at: string | Date | null }[]);

                  // HARD GATE: target started => signal expired, never sent.
                  const beganAt = live[0]?.began_at;
                  if (beganAt) {
                    const began = new Date(beganAt).getTime();
                    if (Number.isFinite(began) && began <= this.now()) {
                      await sql`
                        update notification_outbox
                        set status = 'dead_letter',
                            last_error = 'expired_late_signal: target started before delivery'
                        where id = ${row.id}
                      `;
                      this.stats.dead += 1;
                      logger.warn(
                        {
                          component: "outbox-dispatcher",
                          notificationId: row.notification_id,
                          targetGameId,
                          target_started_at: new Date(began).toISOString(),
                          expiration_reason: "target_already_started",
                        },
                        "SIGNAL_EXPIRED — target already started; NOT delivering late signal",
                      );
                      return "dead" as const;
                    }
                  }

                  // Check both live_round_state.crashed_at and crash_rounds.crashed_at
                  const crashedAt = live[0]?.crashed_at;
                  if (crashedAt) {
                    const crashed = new Date(crashedAt).getTime();
                    if (Number.isFinite(crashed) && crashed <= this.now()) {
                      await sql`
                        update notification_outbox
                        set status = 'dead_letter',
                            last_error = 'target_already_crashed_before_delivery'
                        where id = ${row.id}
                      `;
                      this.stats.dead += 1;
                      logger.warn(
                        { component: "outbox-dispatcher", notificationId: row.notification_id, targetGameId },
                        "target already crashed before delivery — expiring signal",
                      );
                      return "dead" as const;
                    }
                  }
                }
              } catch { /* soft */ }
            }

            // Cap Telegram timeout by remaining deadline (rec 93)
            const sendTimeout = Math.max(
              200,
              Math.min(5_000, Number.isFinite(remainingMs) ? remainingMs - 50 : 5_000),
            );
            lc.sendStartedMs = this.now();
            const sendResults = await sendTelegramMessage(row.content, {
              timeout: sendTimeout,
            });
            const allOk =
              sendResults.length > 0 && sendResults.every((r) => r.ok);
            const anyOk = sendResults.some((r) => r.ok);
            // Fan-out is independently addressed. If at least one destination
            // accepted the message, mark the row delivered instead of retrying
            // and duplicating it to healthy chats because another chat id is stale.
            if (allOk || anyOk) {
              if (!allOk) {
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
              const acceptedAt = new Date(acceptedMs).toISOString();
              await sql`
                update notification_outbox
                set status = 'delivered',
                    delivered_at = ${acceptedAt}::timestamptz,
                    send_started_at = ${new Date(lc.sendStartedMs).toISOString()}::timestamptz,
                    telegram_accepted_at = ${acceptedAt}::timestamptz,
                    last_error = null
                where id = ${row.id}
              `;
              this.stats.delivered += 1;
              logger.info(
                {
                  component: "outbox-dispatcher",
                  ...lifecycleLogFields(row, lc, acceptedMs, "delivered"),
                  remainingBudgetMs: Number.isFinite(remainingMs)
                    ? Math.round(remainingMs)
                    : null,
                },
                "OUTBOX_DISPATCH delivered",
              );
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
              // P1.7: Defer to background so it doesn't block the outbox dispatch loop.
              setImmediate(() => {
                void (async () => {
                const meta = (row.metadata ?? {}) as Record<string, unknown>;
                const targetGameId =
                  (meta.targetGameId as string) ||
                  (meta.target_game_id as string) ||
                  null;
                if (targetGameId) {
                  const predRows = await sql<{
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
                    const live = await sql<{ began_at: string | Date | null }>`
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
      logger.info(
        {
          component: "outbox-dispatcher",
          claimed: claimed.length,
          batchSize: BATCH_SIZE,
          parallelism: BATCH_PARALLELISM,
          delivered,
          dead,
          requeued,
          drainMs: Math.round(this.now() - tickStartMs),
        },
        "OUTBOX_TICK drained claimed rows",
      );
    }
    return { recovered: 0, delivered, dead, requeued };
  }

  /** Recover stale INFLIGHT (legacy status) or stuck pending rows. */
  async recoverStale(): Promise<number> {
    const sql = await this.getSqlFn();

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
        // P2.4: Throttle recoverStale to every 10 ticks (250ms at 25ms tick)
        // instead of every tick. Stale recovery is non-critical and the DB
        // UPDATE it runs was consuming ~5-10ms on every tick.
        if (this.stats.tickCount % 10 === 0) {
          await this.recoverStale();
        }
        await this.tickOnce();
      } catch (e) {
        this.stats.lastError = String(e);
        logger.error(
          { component: "outbox-dispatcher", error: String(e) },
          "tick error",
        );
      }
      if (!this.running) break;
      await this.waitForNextTick();
    }
  }

  /**
   * Wait between ticks: resolves on a producer wake (immediate drain) or
   * after TICK_MS, whichever first. The coalescing wake channel guarantees
   * bursts collapse into a single immediate tick — never overlapping ones.
   */
  private async waitForNextTick(): Promise<void> {
    try {
      const { waitForOutboxWake } = await import("@/lib/prediction/live/outbox-wake");
      await waitForOutboxWake(TICK_MS);
    } catch {
      // wake module unavailable in some test contexts — plain timer fallback
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, TICK_MS);
        t.unref?.();
      });
    }
  }
}