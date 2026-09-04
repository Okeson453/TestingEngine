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
export const TICK_MS = Number(process.env.OUTBOX_TICK_MS ?? 200);
export const BATCH_SIZE = Number(process.env.OUTBOX_BATCH_SIZE ?? 16);
export const STALE_INFLIGHT_MS = Number(process.env.OUTBOX_STALE_MS ?? 30_000);
export const MAX_ATTEMPTS = Number(process.env.OUTBOX_MAX_ATTEMPTS ?? 5);
const BASE_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 60_000;

interface OutboxRow {
  id: number;
  notification_id: string;
  type: string;
  content: string;
  metadata: Record<string, unknown>;
  status: string;
  attempt_count: number;
  next_attempt_at: string;
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
    const recovered = 0;
    let delivered = 0;
    let dead = 0;
    let requeued = 0;

    const sql = await this.getSqlFn();

    // Stale-row recovery is performed by `recoverStale()` which is called
    // by the runOneTick loop *before* tickOnce; tickOnce itself only
    // claims + sends.

    // Step 1: claim a batch. SELECT FOR UPDATE SKIP LOCKED → mark inflight → COMMIT.
    const claimed = await runInTransaction(sql, async (tx) => {
      const rows = await tx<OutboxRow>`
        select id, notification_id, type, content, metadata, status, attempt_count, next_attempt_at
        from notification_outbox
        where status = 'pending'::text
          and next_attempt_at <= now()
        order by next_attempt_at asc, id asc
        limit ${BATCH_SIZE}
        for update skip locked
      `;
      for (const r of rows) {
        await tx`
          update notification_outbox
          set attempt_count = attempt_count + 1,
              last_error = case
                when last_error like '%recovered from inflight%' then ' [recovered from inflight]'
                else last_error
              end
          where id = ${r.id}
        `;
      }
      return rows;
    });

    if (claimed.length > 0) {
      this.stats.claimed += claimed.length;
    }

    // Step 3: send each.
    for (const row of claimed) {
      try {
        const results = await sendTelegramMessage(row.content);
        const allOk = results.length > 0 && results.every((r) => r.ok);
        if (allOk) {
          await sql`
            update notification_outbox
            set status = 'delivered', delivered_at = now(), last_error = null
            where id = ${row.id}
          `;
          delivered += 1;
          this.stats.delivered += 1;
        } else {
          const updated = await this.handleFailure(sql, row, results);
          if (updated === "dead") dead += 1;
          else requeued += 1;
        }
      } catch (e) {
        this.stats.lastError = String(e);
        logger.warn(
          { component: "outbox-dispatcher", notificationId: row.notification_id, error: String(e) },
          "send threw; treating as retryable",
        );
        const updated = await this.handleFailure(sql, row, []);
        if (updated === "dead") dead += 1;
        else requeued += 1;
      }
    }
    return { recovered, delivered, dead, requeued };
  }

  /** Recover stale INFLIGHT (legacy status) or stuck pending rows. */
  async recoverStale(): Promise<number> {
    const sql = await this.getSqlFn();
    // Pending rows whose `next_attempt_at` is more than STALE_INFLIGHT_MS
    // in the past are stuck (the dispatcher crashed mid-send). Reset
    // `attempt_count` so the row retries from a clean state. (Some
    // older schema variants had a separate 'inflight' status — that path
    // is preserved for safety.)
    const res = await sql<{ count: number }>`
      with x as (
        update notification_outbox
        set status = 'pending',
            last_error = coalesce(last_error, '') || ' [recovered from inflight]'
        where status in ('inflight', 'pending')
          and next_attempt_at < now() - (${STALE_INFLIGHT_MS}::int * interval '1 millisecond')
          and coalesce(last_error, '') not like '%recovered from inflight%'
        returning 1
      )
      select count(*)::int as count from x
    `;
    const count = res[0]?.count ?? 0;
    this.stats.recoveredInflight += count;
    return count;
  }

  private async handleFailure(
    sql: Sql,
    row: OutboxRow,
    results: SendResult[],
  ): Promise<"dead" | "requeued"> {
    const attempts = row.attempt_count + 1;
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
