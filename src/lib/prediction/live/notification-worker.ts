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

const logger = getLogger("outbox-dispatcher");

/** Tunables (env-overridable for tests). */
export const TICK_MS = Number(process.env.OUTBOX_TICK_MS ?? 50); // P1.11: Already 50ms
export const BATCH_SIZE = Number(process.env.OUTBOX_BATCH_SIZE ?? 16);
export const STALE_INFLIGHT_MS = Number(process.env.OUTBOX_STALE_MS ?? 30_000);
export const MAX_ATTEMPTS = Number(process.env.OUTBOX_MAX_ATTEMPTS ?? 5);
/** Max concurrent Telegram sends within a claimed batch (P0 / 6.5). */
export const BATCH_PARALLELISM = Number(process.env.OUTBOX_BATCH_PARALLELISM ?? 4); // P1.4: Changed from 2 to 4
const BASE_BACKOFF_MS = 1_000;
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
  telegram_deadline_at?: string | Date | null;
  priority?: number;
}

export interface DispatcherStats {
  tickCount: number;
  recoveredInflight: number;
  claimed: number;
  delivered: number;
  dead: number;
  requeued: number;
  lastError: string | null;
}

export class OutboxDispatcher {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private running = false;
  private stats: DispatcherStats = {
    tickCount: 0,
    recoveredInflight: 0,
    claimed: 0,
    delivered: 0,
    dead: 0,
    requeued: 0,
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
    this.scheduleNext(0);
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
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

    const sql = await this.getSqlFn();

    // Claim batch: SELECT FOR UPDATE SKIP LOCKED → set status=inflight → COMMIT.
    // Exclude rows past telegram_deadline_at so we never deliver "predicts the past".
    const claimed = await runInTransaction(sql, async (tx) => {
      const rows = await tx<OutboxRow>`
        select id, notification_id, type, content, metadata, status, attempt_count,
               next_attempt_at, telegram_deadline_at, priority
        from notification_outbox
        where status = 'pending'::text
          and next_attempt_at <= now()
          and (telegram_deadline_at is null or telegram_deadline_at > now())
        order by priority desc, next_attempt_at asc, id asc
        limit ${BATCH_SIZE}
        for update skip locked
      `;
      for (const r of rows) {
        await tx`
          update notification_outbox
          set status = 'inflight',
              attempt_count = attempt_count + 1
          where id = ${r.id} and status = 'pending'
        `;
        // Keep in-memory attempt_count in sync with the DB increment so
        // handleFailure does not double-count.
        r.attempt_count = (r.attempt_count ?? 0) + 1;
      }
      return rows;
    });

    if (claimed.length > 0) {
      this.stats.claimed += claimed.length;
    }

    // Parallel dispatch bounded by BATCH_PARALLELISM (P0 / 6.5)
    const parallelism = Math.max(1, BATCH_PARALLELISM);
    for (let i = 0; i < claimed.length; i += parallelism) {
      const chunk = claimed.slice(i, i + parallelism);
      const results = await Promise.all(
        chunk.map(async (row) => {
          try {
            // Deadline-aware pre-send check (P0): stop if past telegram_deadline_at
            const deadlineRaw = row.telegram_deadline_at;
            const deadlineMs = deadlineRaw
              ? new Date(deadlineRaw as string | Date).getTime()
              : NaN;
            const remainingMs = Number.isFinite(deadlineMs)
              ? deadlineMs - this.now()
              : Number.POSITIVE_INFINITY;
            if (remainingMs < 50) {
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
                  notificationId: row.notification_id,
                  remainingMs,
                },
                "expired before send — not delivering late signal",
              );
              return "dead" as const;
            }

            // For predictions: only expire if target already CRASHED (past is fully over).
            // Do NOT drop on "started" — poll recovery often delivers after N+1 has begun;
            // operators still need the signal, and WIN/LOSS otherwise arrives with no prior alert.
            if (row.type === "prediction") {
              try {
                const meta = (row.metadata ?? {}) as Record<string, unknown>;
                const targetGameId =
                  (meta.targetGameId as string) ||
                  (meta.target_game_id as string) ||
                  null;
                if (targetGameId) {
                  const live = await sql<{ began_at: string | Date | null; crashed_at: string | Date | null }>`
                    SELECT began_at, crashed_at FROM live_round_state
                    WHERE game_id = ${targetGameId} LIMIT 1
                  `.catch(() => [] as { began_at: string | Date | null; crashed_at: string | Date | null }[]);

                  // Soft note only if already started (still deliver)
                  if (live[0]?.began_at) {
                    const began = new Date(live[0].began_at).getTime();
                    if (Number.isFinite(began) && began <= this.now()) {
                      logger.info(
                        { component: "outbox-dispatcher", notificationId: row.notification_id, targetGameId },
                        "target already started — delivering late signal anyway",
                      );
                      if (!String(row.content).includes("(late)")) {
                        row.content = `${row.content}\n\n(late: target already started)`;
                      }
                    }
                  }

                  if (live[0]?.crashed_at) {
                    const crashed = new Date(live[0].crashed_at).getTime();
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

                  // Fallback: check crash_rounds directly if live state is missing
                  const crashedInHistory = await sql<{ crashed_at: string | Date | null }>`
                    SELECT crashed_at FROM crash_rounds
                    WHERE game_id = ${targetGameId} LIMIT 1
                  `.catch(() => [] as { crashed_at: string | Date | null }[]);

                  if (crashedInHistory[0]?.crashed_at) {
                    const crashed = new Date(crashedInHistory[0].crashed_at).getTime();
                    if (Number.isFinite(crashed) && crashed <= this.now()) {
                      await sql`
                        update notification_outbox
                        set status = 'dead_letter',
                            last_error = 'target_already_crashed_before_delivery (crash_rounds)'
                        where id = ${row.id}
                      `;
                      this.stats.dead += 1;
                      logger.warn(
                        { component: "outbox-dispatcher", notificationId: row.notification_id, targetGameId },
                        "target already crashed before delivery (crash_rounds) — expiring signal",
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
            const sendResults = await sendTelegramMessage(row.content, {
              timeout: sendTimeout,
            });
            const allOk =
              sendResults.length > 0 && sendResults.every((r) => r.ok);
            if (allOk) {
              const t0 = this.now();
              const acceptedAt = new Date(t0).toISOString();
              await sql`
                update notification_outbox
                set status = 'delivered',
                    delivered_at = ${acceptedAt}::timestamptz,
                    last_error = null
                where id = ${row.id}
              `;
              this.stats.delivered += 1;
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
                const { outboxDeliveryMs } = await import(
                  "@/lib/observability/performance/latency"
                );
                const claimedAt = row.next_attempt_at
                  ? new Date(row.next_attempt_at).getTime()
                  : NaN;
                if (Number.isFinite(claimedAt)) {
                  outboxDeliveryMs.observe(Math.max(0, t0 - claimedAt));
                }
              } catch { /* metrics optional */ }

              // Phase 19 — record lead times when target_round_started_at is known
              try {
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
              } catch {
                /* soft — lead time must never break delivery */
              }

              return "delivered" as const;
            }
            // Deadline-aware retry: if remaining budget too small for another attempt, dead-letter
            if (Number.isFinite(remainingMs) && remainingMs < 300) {
              await sql`
                update notification_outbox
                set status = 'dead_letter',
                    last_error = ${'expired_after_failed_send: ' + (sendResults.find((r) => !r.ok)?.error ?? 'send_failed')}
                where id = ${row.id}
              `;
              this.stats.dead += 1;
              return "dead" as const;
            }
            const updated = await this.handleFailure(sql, row, sendResults);
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
            const updated = await this.handleFailure(sql, row, []);
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
    return { recovered: 0, delivered, dead, requeued };
  }

  /** Recover stale INFLIGHT (legacy status) or stuck pending rows. */
  async recoverStale(): Promise<number> {
    const sql = await this.getSqlFn();
    // Reset stuck inflight rows (crash mid-send) and very-old pending rows.
    const result = await sql<{ id: number }>`
      update notification_outbox
      set status = 'pending',
          last_error = coalesce(last_error, '') || ' [recovered from inflight]',
          next_attempt_at = now()
      where (
          status = 'inflight'
          and updated_at < now() - (${STALE_INFLIGHT_MS}::int * interval '1 millisecond')
        )
        or (
          status = 'pending'
          and next_attempt_at < now() - (${STALE_INFLIGHT_MS}::int * interval '1 millisecond')
          and coalesce(last_error, '') not like '%recovered from inflight%'
        )
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
  ): Promise<"dead" | "requeued"> {
    // attempt_count was already incremented at claim time
    const attempts = row.attempt_count;
    const firstFailure = results.find((r) => !r.ok);
    const isPermanent =
      firstFailure != null &&
      typeof firstFailure.status === "number" &&
      firstFailure.status >= 400 &&
      firstFailure.status < 500 &&
      firstFailure.status !== 429;
    const lastError = firstFailure?.error ?? "send_failed";

    if (isPermanent || attempts >= MAX_ATTEMPTS) {
      await sql`
        update notification_outbox
        set status = 'dead_letter',
            last_error = ${lastError},
            next_attempt_at = now()
        where id = ${row.id}
      `;
      this.stats.dead += 1;
      // Rich diagnostics for operators (Diagnosis P0-6)
      let telegramChatId: string | null = null;
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
          next_attempt_at = now() + (${backoff}::int * interval '1 millisecond')
      where id = ${row.id}
    `;
    this.stats.requeued += 1;
    return "requeued";
  }

  private scheduleNext(delayMs: number): void {
    if (!this.running) return;
    this.timer = setTimeout(() => {
      void this.runOneTick();
    }, delayMs);
    this.timer.unref?.();
  }

  private async runOneTick(): Promise<void> {
    if (!this.running) return;
    try {
      await this.recoverStale();
      await this.tickOnce();
    } catch (e) {
      this.stats.lastError = String(e);
      logger.error(
        { component: "outbox-dispatcher", error: String(e) },
        "tick error",
      );
    }
    this.scheduleNext(TICK_MS);
  }
}