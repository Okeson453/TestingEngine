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
/** Parallelism cap for `Promise.all` per tick (Spec §6.5). */
export const BATCH_PARALLELISM = Math.max(
  1,
  Number(process.env.OUTBOX_BATCH_PARALLELISM ?? 4) || 4,
);
export const STALE_INFLIGHT_MS = Number(process.env.OUTBOX_STALE_MS ?? 30_000);
export const MAX_ATTEMPTS = Number(process.env.OUTBOX_MAX_ATTEMPTS ?? 5);
/** Telegram send budget per chat (Spec §6.17). Alarms at p95 > 1000ms. */
export const TELEGRAM_SEND_BUDGET_MS = Number(
  process.env.TELEGRAM_SEND_BUDGET_MS ?? 5_000,
);
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
  telegram_deadline_at: string | null;
  claimed_at: string;
}

export interface DispatcherStats {
  tickCount: number;
  recoveredInflight: number;
  claimed: number;
  delivered: number;
  dead: number;
  requeued: number;
  /** Spec §6.17 — claim→Telegram-200 latency in ms (p95 over recent window). */
  deliveryLatencyP95Ms: number | null;
  /** Recent claim→200 samples (capped ring buffer). */
  deliveryLatencySamples: number;
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
    deliveryLatencyP95Ms: null,
    deliveryLatencySamples: 0,
    lastError: null,
  };
  private deliveryLatencyRing: number[] = [];
  private readonly deliveryLatencyRingCap = 200;
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
    skipped_deadline: number;
  }> {
    this.stats.tickCount += 1;
    let delivered = 0;
    let dead = 0;
    let requeued = 0;
    let skippedDeadline = 0;
    const recovered = 0; // Spec §B11: dead counter; recovery runs in runOneTick → recoverStale()

    const sql = await this.getSqlFn();

    // Step 1: claim a batch atomically.
    //   * SELECT … FOR UPDATE SKIP LOCKED — multiple instances cannot win
    //     the same row.
    //   * Exclude rows whose `telegram_deadline_at` has passed (Spec §6.12).
    //   * Immediately flip `status='inflight'` inside the same transaction
    //     (Spec §6.9) so a crash mid-send is visible to `recoverStale`
    //     without waiting for `next_attempt_at`.
    const claimed = await runInTransaction(sql, async (tx) => {
      const rows = await tx<OutboxRow>`
        select id, notification_id, type, content, metadata, status,
               attempt_count, next_attempt_at, telegram_deadline_at, now() as claimed_at
        from notification_outbox
        where status = 'pending'::text
          and next_attempt_at <= now()
          and (telegram_deadline_at is null or telegram_deadline_at > now())
        order by next_attempt_at asc, id asc
        limit ${BATCH_SIZE}
        for update skip locked
      `;
      if (rows.length === 0) return [] as Array<OutboxRow & { claimed_at: string }>;
      const ids = rows.map((r) => r.id);
      await tx`
        update notification_outbox
        set status = 'inflight',
            attempt_count = attempt_count + 1,
            last_error = case
              when last_error like '%recovered from inflight%' then ' [recovered from inflight]'
              else last_error
            end
        where id = any(${ids}::int[])
      `;
      return rows as Array<OutboxRow & { claimed_at: string }>;
    });

    // Step 2: rows whose deadline has passed while waiting for a slot.
    // Mark them as `dead_letter` so the operator sees them in the stats
    // view; do NOT send. This is the "predicts the past" defense.
    if (claimed.length > 0) {
      const nowMs = this.now();
      const expired = claimed.filter(
        (r) => r.telegram_deadline_at && new Date(r.telegram_deadline_at).getTime() <= nowMs,
      );
      if (expired.length > 0) {
        const expiredIds = expired.map((r) => r.id);
        await sql`
          update notification_outbox
          set status = 'dead_letter',
              last_error = coalesce(last_error, '') || ' [telegram_deadline_exceeded]'
          where id = any(${expiredIds}::int[])
        `;
        skippedDeadline = expired.length;
        this.stats.dead += expired.length;
        dead += expired.length;
        logger.warn(
          { component: "outbox-dispatcher", expiredIds, count: expired.length },
          "expired outbox rows skipped before send (telegram_deadline_at reached)",
        );
      }
    }

    if (claimed.length > 0) {
      this.stats.claimed += claimed.length;
    }

    // Step 3: parallel send, bounded by BATCH_PARALLELISM. A single slow
    // chat no longer blocks delivery to the other chats in the batch.
    const sendable = claimed.filter(
      (r) => !r.telegram_deadline_at || new Date(r.telegram_deadline_at).getTime() > this.now(),
    );
    const sendOne = async (row: OutboxRow & { claimed_at: string }): Promise<void> => {
      const t0 = this.now();
      try {
        const results = await sendTelegramMessage(row.content);
        const allOk = results.length > 0 && results.every((r) => r.ok);
        if (allOk) {
          await sql`
            update notification_outbox
            set status = 'delivered', delivered_at = now(), last_error = null
            where id = ${row.id}
          `;
          const latencyMs = this.now() - t0;
          this.observeDeliveryLatency(latencyMs);
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
          {
            component: "outbox-dispatcher",
            notificationId: row.notification_id,
            error: String(e),
          },
          "send threw; treating as retryable",
        );
        const updated = await this.handleFailure(sql, row, []);
        if (updated === "dead") dead += 1;
        else requeued += 1;
      }
    };

    for (let i = 0; i < sendable.length; i += BATCH_PARALLELISM) {
      const slice = sendable.slice(i, i + BATCH_PARALLELISM);
      await Promise.all(slice.map(sendOne));
    }

    // Spec §6.17 — alarm at p95 > 1s. Persist the alarm so the
    // dashboard can surface it.
    const p95 = this.stats.deliveryLatencyP95Ms;
    if (p95 != null && p95 > TELEGRAM_SEND_BUDGET_MS) {
      try {
        await sql`
          insert into worker_state (key, value) values (
            'delivery_latency_alarm_at', ${new Date().toISOString()}
          )
          on conflict (key) do update set value = excluded.value, updated_at = now()
        `;
        await sql`
          insert into worker_state (key, value) values (
            'delivery_latency_alarm_p95_ms', ${String(p95)}
          )
          on conflict (key) do update set value = excluded.value, updated_at = now()
        `;
        logger.warn(
          { component: "outbox-dispatcher", p95, budgetMs: TELEGRAM_SEND_BUDGET_MS },
          "delivery latency p95 exceeds budget",
        );
      } catch {
        /* non-fatal */
      }
    }

    return { recovered, delivered, dead, requeued, skipped_deadline: skippedDeadline };
  }

  private observeDeliveryLatency(ms: number): void {
    if (!Number.isFinite(ms) || ms < 0) return;
    this.deliveryLatencyRing.push(ms);
    if (this.deliveryLatencyRing.length > this.deliveryLatencyRingCap) {
      this.deliveryLatencyRing.shift();
    }
    this.stats.deliveryLatencySamples = this.deliveryLatencyRing.length;
    this.stats.deliveryLatencyP95Ms = this.computeP95(this.deliveryLatencyRing);
  }

  private computeP95(samples: number[]): number | null {
    if (samples.length === 0) return null;
    const sorted = [...samples].sort((a, b) => a - b);
    const idx = Math.min(
      sorted.length - 1,
      Math.max(0, Math.ceil(0.95 * sorted.length) - 1),
    );
    return sorted[idx]!;
  }

  /** Recover stuck INFLIGHT (claimed but never completed) or stuck
   *  pending rows. After the inflight-state change, an inflight row
   *  whose `next_attempt_at` is in the past is unambiguous evidence the
   *  previous dispatcher instance died mid-send — reset it to pending so
   *  a healthy dispatcher can reclaim it. The legacy "stuck pending"
   *  case (a row that was updated by a `pending → inflight` flip that
   *  was then re-pushed by a partial update) is preserved for safety. */
  async recoverStale(): Promise<number> {
    const sql = await this.getSqlFn();
    const res = await sql<{ count: number }>`
      with x as (
        update notification_outbox
        set status = 'pending',
            last_error = coalesce(last_error, '') || ' [recovered from inflight]'
        where (
          (status = 'inflight'
            and next_attempt_at < now() - (${STALE_INFLIGHT_MS}::int * interval '1 millisecond'))
          or (status = 'pending'
            and coalesce(last_error, '') not like '%recovered from inflight%'
            and next_attempt_at < now() - (${STALE_INFLIGHT_MS}::int * interval '1 millisecond'))
        )
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
