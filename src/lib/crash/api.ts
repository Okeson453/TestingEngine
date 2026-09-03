import { createServerFn } from "@tanstack/react-start";
import { getSql } from "@/lib/db";
import { computeRanges, computeStats, computeStreaks } from "./stats";
import {
  loadRounds,
  recomputeDaily,
  toIso,
  type RoundRow,
} from "./ingest";
import {
  HISTORY_LIMIT,
  STATS_LIMIT,
  type CrashRound,
  type CrashDaily,
  type DailyPayload,
  type DashboardPayload,
  type FeedStatus,
} from "./types";

type DailyRow = {
  date: string;
  total_rounds: number;
  avg_multiplier: string | number | null;
  median_multiplier: string | number | null;
  highest_multiplier: string | number | null;
  lowest_multiplier: string | number | null;
  low_count: number;
  high_count: number;
  moon_count: number;
  sum_multipliers: string | number | null;
  updated_at: string;
};

function mapDailyRow(row: DailyRow): CrashDaily {
  return {
    date: row.date,
    totalRounds: Number(row.total_rounds),
    avgMultiplier: row.avg_multiplier == null ? null : Number(row.avg_multiplier),
    medianMultiplier: row.median_multiplier == null ? null : Number(row.median_multiplier),
    highestMultiplier: row.highest_multiplier == null ? null : Number(row.highest_multiplier),
    lowestMultiplier: row.lowest_multiplier == null ? null : Number(row.lowest_multiplier),
    lowCount: Number(row.low_count),
    highCount: Number(row.high_count),
    moonCount: Number(row.moon_count),
    updatedAt: row.updated_at,
  };
}

function buildPayload(
  all: CrashRound[],
  feed: DashboardPayload["feed"],
): DashboardPayload {
  const rounds = all.slice(0, HISTORY_LIMIT);
  const chart = [...rounds].slice(0, 60).reverse();
  return {
    latest: rounds[0] ?? null,
    rounds,
    chart,
    stats: computeStats(all),
    ranges: computeRanges(all),
    streaks: computeStreaks(rounds),
    feed,
  };
}

/**
 * Read the feed status written by the autonomous background worker into
 * `worker_state`. The dashboard is a pure read: it does NOT poll BC.Game, does
 * NOT generate predictions and does NOT validate — the worker owns all of that
 * server-side, independently of any dashboard being open.
 */
async function readWorkerFeed(sql: Awaited<ReturnType<typeof getSql>>): Promise<FeedStatus> {
  const rows = await sql<{ key: string; value: string | null }>`
    select key, value
    from worker_state
    where key in (
      'last_sync_at', 'last_sync_ok', 'last_error', 'last_fetch_count',
      'last_inserted_count', 'last_online_players'
    )
  `;
  const m = new Map(rows.map((r) => [r.key, r.value]));
  const ok = m.get("last_sync_ok") === "1";
  const lastSyncAt = m.get("last_sync_at") ?? "";
  const fetched = Number(m.get("last_fetch_count") ?? 0) || 0;
  const inserted = Number(m.get("last_inserted_count") ?? 0) || 0;
  const rawPlayers = m.get("last_online_players");
  const onlinePlayers = rawPlayers != null && rawPlayers !== "" ? Number(rawPlayers) : null;
  return {
    ok,
    lastSyncAt,
    fetched,
    inserted,
    error: m.get("last_error") ?? null,
    onlinePlayers,
  };
}

/**
 * Refresh the dashboard view. READ ONLY: returns the currently persisted state
 * (rounds + worker sync feed). No BC.Game polling, no prediction generation, no
 * validation — those run in the background worker and are reflected here.
 */
export const refreshDashboard = createServerFn({ method: "POST" }).handler(
  async (): Promise<DashboardPayload> => {
    const sql = await getSql();
    const feed = await readWorkerFeed(sql);
    const all = await loadRounds(STATS_LIMIT);
    return buildPayload(all, feed);
  },
);

/* ── Daily query endpoints ─────────────────────────────────────────────── */

/** List daily stats for the last N calendar days (newest first). */
export const getDailyStats = createServerFn({ method: "GET" })
  .validator((data: unknown) => {
    const d = data as { days?: number } | undefined;
    const n = typeof d?.days === "number" ? d.days : 30;
    return { days: Math.max(1, Math.min(365, n)) };
  })
  .handler(async ({ data }): Promise<DailyPayload> => {
    const sql = await getSql();
    const rows = await sql<DailyRow>`
      select date, total_rounds, avg_multiplier, median_multiplier,
             highest_multiplier, lowest_multiplier,
             low_count, high_count, moon_count, updated_at
      from crash_daily
      order by date desc
      limit ${data.days}
    `;

    const days = rows.map(mapDailyRow);

    const overallRows = await sql<{
      count: number;
      avg: number | null;
      median: number | null;
      highest: number | null;
      lowest: number | null;
    }>`
      select
        coalesce(sum(total_rounds), 0)::int as count,
        (sum(sum_multipliers) / nullif(sum(total_rounds), 0))::numeric(12,4) as avg,
        percentile_cont(0.5) within group (order by avg_multiplier)::numeric(12,4) as median,
        max(highest_multiplier)::numeric(12,4) as highest,
        min(lowest_multiplier)::numeric(12,4) as lowest
      from crash_daily
      where total_rounds > 0
    `;

    const o = overallRows[0];
    const overall = {
      count: o?.count ?? 0,
      average: o?.avg == null ? null : Number(o.avg),
      median: o?.median == null ? null : Number(o.median),
      highest: o?.highest == null ? null : Number(o.highest),
      lowest: o?.lowest == null ? null : Number(o.lowest),
    };

    return { days, overall };
  });

/** Backfill crash_daily from all existing crash_rounds. Idempotent. */
export const backfillDaily = createServerFn({ method: "POST" }).handler(
  async (): Promise<{ dates: number }> => {
    const sql = await getSql();
    const rows = await sql<{ date: string }>`
      select distinct crashed_at::date as date
      from crash_rounds
      order by date
    `;
    await recomputeDaily(rows.map((r) => r.date));
    return { dates: rows.length };
  },
);

/* ── Storage maintenance ───────────────────────────────────────────────── */

/**
 * Purge raw crash_rounds older than `retentionDays` (default 30).
 * Daily aggregates in crash_daily are preserved forever.
 * Returns how many raw rows were deleted.
 */
export const cleanupOldRounds = createServerFn({ method: "POST" })
  .validator((data: unknown) => {
    const d = data as { retentionDays?: number } | undefined;
    const n = typeof d?.retentionDays === "number" ? d.retentionDays : 30;
    return { retentionDays: Math.max(1, Math.min(365, n)) };
  })
  .handler(async ({ data }): Promise<{ deleted: number }> => {
    const sql = await getSql();
    const rows = await sql<{ deleted: number }>`
      select purge_old_crash_rounds(${data.retentionDays}) as deleted
    `;
    await sql.query("vacuum analyze crash_rounds");
    return { deleted: rows[0]?.deleted ?? 0 };
  });

/* ── CSV export ────────────────────────────────────────────────────────── */

function csvCell(value: string | number | null): string {
  const text = value == null ? "" : String(value);
  if (/[",\n\r]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function csvRow(values: (string | number | null)[]): string {
  return values.map(csvCell).join(",") + "\n";
}

export const exportCrashCsv = createServerFn({ method: "POST" })
  .validator((data: unknown) => {
    const d = data as { mode?: "daily" | "rounds"; days?: number } | undefined;
    const mode = d?.mode === "rounds" ? "rounds" : "daily";
    const days = typeof d?.days === "number" ? d.days : mode === "daily" ? 365 : 30;
    return { mode, days: Math.max(1, Math.min(mode === "rounds" ? 90 : 365, days)) };
  })
  .handler(async ({ data }): Promise<{ csv: string; filename: string }> => {
    const mode = data.mode;
    const limit = data.days;
    const sql = await getSql();
    const now = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");

    if (mode === "daily") {
      const rows = await sql<DailyRow>`
        select date, total_rounds, avg_multiplier, median_multiplier,
               highest_multiplier, lowest_multiplier,
               low_count, high_count, moon_count, updated_at
        from crash_daily
        order by date desc
        limit ${limit}
      `;

      let csv = csvRow([
        "Date", "Total Rounds", "Avg Multiplier", "Median Multiplier",
        "Highest Multiplier", "Lowest Multiplier",
        "Low Count (<2x)", "High Count (2-10x)", "Moon Count (10x+)", "Updated At",
      ]);

      for (const r of rows) {
        csv += csvRow([
          r.date,
          r.total_rounds,
          r.avg_multiplier,
          r.median_multiplier,
          r.highest_multiplier,
          r.lowest_multiplier,
          r.low_count,
          r.high_count,
          r.moon_count,
          r.updated_at,
        ]);
      }

      return { csv, filename: `crash-daily-${now}.csv` };
    }

    // rounds mode
    const rows = await sql<RoundRow>`
      select game_id, multiplier, hash, salt, began_at, crashed_at
      from crash_rounds
      order by crashed_at desc
      limit ${limit * 50}
    `;

    let csv = csvRow([
      "Game ID", "Multiplier", "Hash", "Salt", "Began At", "Crashed At",
    ]);

    for (const r of rows) {
      csv += csvRow([
        r.game_id,
        r.multiplier,
        r.hash,
        r.salt,
        r.began_at == null ? "" : toIso(r.began_at),
        toIso(r.crashed_at),
      ]);
    }

    return { csv, filename: `crash-rounds-${now}.csv` };
  });
