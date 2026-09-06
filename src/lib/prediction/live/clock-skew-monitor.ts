/**
 * Clock-skew monitor.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.9
 *
 * 1. One-sided: p50/p95 of received_at - beginTime for BG events (existing).
 * 2. Bidirectional wall-clock probe against BC.Game (P0 / 6.6): workerMs - serverMs.
 *    Alerts when |skew| > 1s so a wrong system clock cannot silently break
 *    the temporal invariant.
 */
import { getSql, type Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("clock-skew-monitor");

export const SKEW_INTERVAL_MS = Number(process.env.CLOCK_SKEW_INTERVAL_MS ?? 5 * 60 * 1_000);
export const WALL_CLOCK_SKEW_WARN_MS = Number(process.env.WALL_CLOCK_SKEW_WARN_MS ?? 1_000);

export interface SkewSnapshot {
  samples: number;
  p50LagMs: number | null;
  p95LagMs: number | null;
  measuredAt: string;
  wallClockSkewMs?: number | null;
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

  /**
   * Probe BC.Game for a server-side timestamp and estimate worker clock skew.
   * Uses a lightweight history page; fails soft if the network is unavailable.
   */
  async probeWallClockSkew(): Promise<number | null> {
    const t0 = this.now();
    try {
      const response = await fetch("https://bc.game/api/game/bet/multi/history", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ page: 1, pageSize: 1, gameType: "crash" }),
        signal: AbortSignal.timeout(5_000),
      });
      const t1 = this.now();
      if (!response.ok) return null;
      const data = (await response.json()) as {
        data?: { list?: Array<{ endTime?: number; beginTime?: number; crashedAt?: string }> };
        list?: Array<{ endTime?: number; beginTime?: number }>;
      };
      const list = data?.data?.list ?? data?.list ?? [];
      const first = list[0];
      if (!first) return null;
      // BC.Game timestamps are often epoch-ms numbers
      const serverMsRaw =
        first.endTime ??
        first.beginTime ??
        (typeof (first as { crashedAt?: string }).crashedAt === "string"
          ? Date.parse((first as { crashedAt: string }).crashedAt)
          : NaN);
      if (!Number.isFinite(serverMsRaw)) return null;
      const serverMs = serverMsRaw < 1e12 ? serverMsRaw * 1000 : serverMsRaw;
      const rtt = t1 - t0;
      const skew = this.now() - (serverMs + rtt / 2);
      return Math.round(skew);
    } catch (e) {
      logger.debug(
        { component: "clock-skew-monitor", error: String(e) },
        "wall-clock skew probe failed (soft)",
      );
      return null;
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
    const wallClockSkewMs = await this.probeWallClockSkew();
    const snap: SkewSnapshot = {
      samples: rows[0]?.samples ?? 0,
      p50LagMs: rows[0]?.p50 != null ? Math.round(rows[0].p50) : null,
      p95LagMs: rows[0]?.p95 != null ? Math.round(rows[0].p95) : null,
      measuredAt: new Date(this.now()).toISOString(),
      wallClockSkewMs,
    };
    await sql`
      insert into worker_state (key, value) values ('last_bg_to_recv_lag_ms_p95', ${String(snap.p95LagMs ?? "")})
      on conflict (key) do update set value = excluded.value, updated_at = now()
    `;
    await sql`
      insert into worker_state (key, value) values ('last_bg_to_recv_lag_ms_p50', ${String(snap.p50LagMs ?? "")})
      on conflict (key) do update set value = excluded.value, updated_at = now()
    `;
    if (wallClockSkewMs != null) {
      await sql`
        insert into worker_state (key, value) values ('wall_clock_skew_ms', ${String(wallClockSkewMs)})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
      if (Math.abs(wallClockSkewMs) > WALL_CLOCK_SKEW_WARN_MS) {
        logger.error(
          { component: "clock-skew-monitor", wallClockSkewMs, threshold: WALL_CLOCK_SKEW_WARN_MS },
          "Wall clock skew exceeds threshold — temporal invariant at risk",
        );
        // Corrective action: raise residual skip threshold and surface operator flag
        // P2.7: Cap at 1000ms (was 3000ms). A 3000ms skip threshold re-introduces
        // the original skip-gate problem: with 3-5s inter-round gaps, predictions
        // skip when >1-2s has elapsed since the ED event, which is most of the time.
        // 1000ms is still safe for 3-5s rounds while preventing excessive skipping.
        const adjusted = Math.min(
          1_000,
          Math.max(250, Math.abs(wallClockSkewMs) + 200),
        );
        await sql`
          insert into worker_state (key, value) values
            ('clock_skew_action', ${'raise_skip_threshold:' + String(adjusted)}),
            ('effective_skip_below_ms', ${String(adjusted)})
          on conflict (key) do update set value = excluded.value, updated_at = now()
        `;
        // Tighten sheath warn rate temporarily via env-like state
        await sql`
          insert into worker_state (key, value)
          values ('sheath_force_warn', ${Math.abs(wallClockSkewMs) > 5_000 ? '1' : '0'})
          on conflict (key) do update set value = excluded.value, updated_at = now()
        `;
      }
    }
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
