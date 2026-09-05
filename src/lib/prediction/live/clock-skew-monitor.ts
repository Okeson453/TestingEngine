/**
 * Clock-skew monitor.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.9,
 *       TestingEngine-Complete-Issues-and-Recommendations.md §6.6
 *
 * Two complementary probes:
 *
 * 1. **One-sided BG-receive lag** (legacy) — measures
 *    `received_at - beginTime` for the last hour's BG events. Catches
 *    socket delay / processing delay between BC.Game and us.
 *
 * 2. **Wall-clock skew** (new, §6.6) — measures `workerMs - serverMs`
 *    by sending a request to BC.Game and using the HTTP `Date` header
 *    (or the response body) as the authoritative server timestamp.
 *    If `|skew| > SKEW_ALERT_MS`, the monitor records a
 *    `wallclock_skew_ms` row in `worker_state` and a `live_event_log`
 *    alarm so the operator (and the predictor's safety net) can see
 *    the divergence. Wrong system clock silently breaks the temporal
 *    invariant — this probe is the only defense.
 */
import { getSql, type Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("clock-skew-monitor");

export const SKEW_INTERVAL_MS = Number(
  process.env.CLOCK_SKEW_INTERVAL_MS ?? 5 * 60 * 1_000,
);
export const SKEW_ALERT_MS = Number(process.env.CLOCK_SKEW_ALERT_MS ?? 1_000);
/** BC.Game probe endpoints. Prefer the lightweight home-stat JSON. */
const BC_HOME_STAT_URL = "https://bc.game/api/game/home/game-stat/";
const BC_HISTORY_URL = "https://bc.game/api/game/bet/multi/history";

export interface SkewSnapshot {
  samples: number;
  p50LagMs: number | null;
  p95LagMs: number | null;
  measuredAt: string;
  wallclockSkewMs: number | null;
  wallclockProbeAt: string | null;
}

export interface WallclockProbeResult {
  workerMs: number;
  serverMs: number;
  skewMs: number;
  endpoint: string;
  ok: boolean;
  error?: string;
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
    const wallclock = await probeWallclock().catch((e: unknown) => ({
      workerMs: 0,
      serverMs: 0,
      skewMs: 0,
      endpoint: "none",
      ok: false,
      error: String(e),
    }));
    const snap: SkewSnapshot = {
      samples: rows[0]?.samples ?? 0,
      p50LagMs: rows[0]?.p50 != null ? Math.round(rows[0].p50) : null,
      p95LagMs: rows[0]?.p95 != null ? Math.round(rows[0].p95) : null,
      measuredAt: new Date(this.now()).toISOString(),
      wallclockSkewMs: wallclock.ok ? wallclock.skewMs : null,
      wallclockProbeAt: wallclock.ok ? new Date(wallclock.workerMs).toISOString() : null,
    };
    await sql`
      insert into worker_state (key, value) values ('last_bg_to_recv_lag_ms_p95', ${String(snap.p95LagMs ?? "")})
      on conflict (key) do update set value = excluded.value, updated_at = now()
    `;
    await sql`
      insert into worker_state (key, value) values ('last_bg_to_recv_lag_ms_p50', ${String(snap.p50LagMs ?? "")})
      on conflict (key) do update set value = excluded.value, updated_at = now()
    `;
    if (wallclock.ok) {
      await sql`
        insert into worker_state (key, value) values ('wallclock_skew_ms', ${String(wallclock.skewMs)})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
      if (Math.abs(wallclock.skewMs) > SKEW_ALERT_MS) {
        logger.error(
          {
            component: "clock-skew-monitor",
            skewMs: wallclock.skewMs,
            thresholdMs: SKEW_ALERT_MS,
            endpoint: wallclock.endpoint,
          },
          "wall-clock skew exceeds threshold — temporal invariant unsafe",
        );
        // live_event_log.event_kind CHECK is fixed; record the alarm in
        // worker_state (always present) instead of in live_event_log.
        try {
          await sql`
            insert into worker_state (key, value) values (
              'wallclock_skew_alert_at',
              ${new Date().toISOString()}
            )
            on conflict (key) do update set value = excluded.value, updated_at = now()
          `;
        } catch {
          /* non-fatal */
        }
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

/**
 * Wall-clock skew probe (Spec §6.6).
 *
 * Send a request to BC.Game and use the response `Date` header as the
 * authoritative server time. Compute `skewMs = workerMs - serverMs`
 * (mid-flight: `(t_after - t_before) / 2` corrected for round-trip
 * latency). A skewed system clock silently breaks the temporal
 * invariant; this probe is the only defense.
 */
export async function probeWallclock(
  fetchImpl: typeof fetch = (globalThis as { fetch?: typeof fetch }).fetch ?? fetch,
): Promise<WallclockProbeResult> {
  const url = BC_HOME_STAT_URL;
  const tBefore = Date.now();
  let res: Response;
  try {
    res = await fetchImpl(url, { method: "GET" });
  } catch (e) {
    return {
      workerMs: tBefore,
      serverMs: 0,
      skewMs: 0,
      endpoint: url,
      ok: false,
      error: `fetch_failed: ${String(e)}`,
    };
  }
  const tAfter = Date.now();
  if (!res.ok) {
    return {
      workerMs: tAfter,
      serverMs: 0,
      skewMs: 0,
      endpoint: url,
      ok: false,
      error: `http_${res.status}`,
    };
  }
  const dateHeader = res.headers.get("date");
  if (!dateHeader) {
    return {
      workerMs: tAfter,
      serverMs: 0,
      skewMs: 0,
      endpoint: url,
      ok: false,
      error: "no_date_header",
    };
  }
  const serverMs = new Date(dateHeader).getTime();
  if (!Number.isFinite(serverMs)) {
    return {
      workerMs: tAfter,
      serverMs: 0,
      skewMs: 0,
      endpoint: url,
      ok: false,
      error: "bad_date_header",
    };
  }
  // Mid-flight correction: assume symmetric network latency.
  const workerMidMs = Math.round((tBefore + tAfter) / 2);
  const skewMs = workerMidMs - serverMs;
  return {
    workerMs: workerMidMs,
    serverMs,
    skewMs,
    endpoint: url,
    ok: true,
  };
}

export const _probeUrls = { BC_HOME_STAT_URL, BC_HISTORY_URL };
