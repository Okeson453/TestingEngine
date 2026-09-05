/**
 * Late-rate monitor + auto-sheath.
 *
 * Spec: TestingEngine-Complete-Issues-and-Recommendations.md §6.10 + §6.15
 *
 * The predictor's deadline gate emits `gate` events into `live_event_log`
 * with `decision = 'skipped_late' | 'predicted' | 'timeout'`. This module
 * computes a rolling 1-minute late rate:
 *
 *   late_rate = skipped_late / (skipped_late + predicted)
 *
 *   > 0.05 (5%)  → warn
 *   > 0.20 (20%) → auto-sheath: set `sheath_mode_active = 1` in
 *                  `worker_state`; the predictor's gate consults this
 *                  row and short-circuits to `skipped_late` without
 *                  running the model. The dispatcher stops draining the
 *                  outbox until the rate drops below 5% (auto-recovery).
 *
 *   The rate is also written to `worker_state` so the dashboard
 *   (§6.15) can surface it.
 */
import { getSql, type Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("late-rate-monitor");

export const LATE_RATE_WARN = Number(process.env.LATE_RATE_WARN ?? 0.05);
export const LATE_RATE_SHEATH = Number(process.env.LATE_RATE_SHEATH ?? 0.20);
export const LATE_RATE_WINDOW_MS = 60_000;
export const SHEATH_STATE_KEY = "sheath_mode_active";
export const LATE_RATE_STATE_KEY = "late_rate_1m";

export interface LateRateSnapshot {
  samples: number;
  late: number;
  predicted: number;
  rate: number | null;
  sheathed: boolean;
  measuredAt: string;
}

export interface LateRateMonitorDeps {
  getSqlFn?: () => Promise<Sql>;
  now?: () => number;
}

export class LateRateMonitor {
  private getSqlFn: () => Promise<Sql> = getSql;
  private now: () => number = Date.now;

  constructor(deps: LateRateMonitorDeps = {}) {
    if (deps.getSqlFn) this.getSqlFn = deps.getSqlFn;
    if (deps.now) this.now = deps.now;
  }

  async measureOnce(): Promise<LateRateSnapshot> {
    const sql = await this.getSqlFn();
    // Look at the last 60s of `live_event_log` rows that carry a
    // gate decision in their payload. We use JSONB operators.
    const rows = await sql<{ late: number; predicted: number; total: number }>`
      select
        count(*) filter (where payload->>'decision' = 'skipped_late')::int as late,
        count(*) filter (where payload->>'decision' = 'predicted')::int as predicted,
        count(*)::int as total
      from live_event_log
      where event_kind = 'PREDICT'
        and received_at > now() - (${LATE_RATE_WINDOW_MS}::int * interval '1 millisecond')
        and payload->>'kind' = 'gate'
    `;
    const late = rows[0]?.late ?? 0;
    const predicted = rows[0]?.predicted ?? 0;
    const total = late + predicted;
    const rate = total > 0 ? late / total : null;
    let sheathed = false;
    if (rate !== null) {
      sheathed = rate > LATE_RATE_SHEATH;
    }
    const snap: LateRateSnapshot = {
      samples: total,
      late,
      predicted,
      rate,
      sheathed,
      measuredAt: new Date(this.now()).toISOString(),
    };
    await sql`
      insert into worker_state (key, value) values (${LATE_RATE_STATE_KEY}, ${rate == null ? "" : String(rate)})
      on conflict (key) do update set value = excluded.value, updated_at = now()
    `;
    await sql`
      insert into worker_state (key, value) values (${SHEATH_STATE_KEY}, ${sheathed ? "1" : "0"})
      on conflict (key) do update set value = excluded.value, updated_at = now()
    `;
    if (rate !== null && rate > LATE_RATE_SHEATH) {
      logger.warn(
        { component: "late-rate-monitor", ...snap, threshold: LATE_RATE_SHEATH },
        "auto-sheath engaged: late rate exceeds threshold",
      );
    } else if (rate !== null && rate > LATE_RATE_WARN) {
      logger.warn(
        { component: "late-rate-monitor", ...snap, threshold: LATE_RATE_WARN },
        "late rate above warn threshold",
      );
    }
    return snap;
  }
}

/** Predicate consulted by the predictor gate (Spec §6.10). */
export async function isSheathed(sql: Sql): Promise<boolean> {
  const rows = await sql<{ value: string }>`
    select value from worker_state where key = ${SHEATH_STATE_KEY} limit 1
  `;
  return rows[0]?.value === "1";
}
