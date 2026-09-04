/**
 * Clock-skew monitor.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.9
 *
 * Runs periodically and writes the p50 / p95 of `received_at - beginTime`
 * for the last hour's `BG` events to `worker_state` so an operator can
 * alert when BC.Game's clock drifts relative to ours. The DB CHECK
 * constraint `target_round_started_at < prediction_generated_at` catches
 * any future-dated payload that slips through; this monitor is the early
 * warning.
 */
import { getSql, type Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("clock-skew-monitor");

export const SKEW_INTERVAL_MS = Number(process.env.CLOCK_SKEW_INTERVAL_MS ?? 5 * 60 * 1_000);

export interface SkewSnapshot {
  samples: number;
  p50LagMs: number | null;
  p95LagMs: number | null;
  measuredAt: string;
}

export class ClockSkewMonitor {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private running = false;
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

  async measureOnce(): Promise<SkewSnapshot> {
    const sql = await this.getSqlFn();
    const rows = await sql<{ samples: number; p50: number | null; p95: number | null }>`
      select
        count(*)::int as samples,
        percentile_cont(0.5) within group (
          order by extract(epoch from (received_at - (payload->>'beginTime')::timestamptz))
        ) * 1000 as p50,
        percentile_cont(0.95) within group (
          order by extract(epoch from (received_at - (payload->>'beginTime')::timestamptz))
        ) * 1000 as p95
      from live_event_log
      where event_kind = 'BG'
        and received_at > now() - interval '1 hour'
        and (payload ? 'beginTime')
    `;
    const snap: SkewSnapshot = {
      samples: rows[0]?.samples ?? 0,
      p50LagMs: rows[0]?.p50 != null ? Math.round(rows[0].p50) : null,
      p95LagMs: rows[0]?.p95 != null ? Math.round(rows[0].p95) : null,
      measuredAt: new Date(this.now()).toISOString(),
    };
    await sql`
      insert into worker_state (key, value) values ('last_bg_to_recv_lag_ms_p95', ${String(snap.p95LagMs ?? "")})
      on conflict (key) do update set value = excluded.value, updated_at = now()
    `;
    await sql`
      insert into worker_state (key, value) values ('last_bg_to_recv_lag_ms_p50', ${String(snap.p50LagMs ?? "")})
      on conflict (key) do update set value = excluded.value, updated_at = now()
    `;
    logger.info(
      { component: "clock-skew-monitor", ...snap },
      "clock-skew sample recorded",
    );
    return snap;
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
      await this.measureOnce();
    } catch (e) {
      logger.warn(
        { component: "clock-skew-monitor", error: String(e) },
        "measure failed",
      );
    }
    this.scheduleNext(SKEW_INTERVAL_MS);
  }
}
