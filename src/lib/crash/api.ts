import { createServerFn } from "@tanstack/react-start";
import { getSql } from "@/lib/db";
import { fetchCrashHistory, fetchCrashHistoryDeep, fetchOnlinePlayers, type FetchedRound } from "./fetch-bc";
import { computeRanges, computeStats, computeStreaks } from "./stats";
import {
  generateAndQueuePrediction,
  validateAgainstNewRounds,
} from "@/lib/prediction/service";
import {
  HISTORY_LIMIT,
  STATS_LIMIT,
  type CrashRound,
  type CrashDaily,
  type DailyPayload,
  type DashboardPayload,
} from "./types";

type RoundRow = {
  game_id: string;
  multiplier: string | number;
  hash: string | null;
  salt: string | null;
  began_at: string | Date | null;
  crashed_at: string | Date;
};

function toIso(value: string | Date | null | undefined): string | null {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString();
  const asDate = new Date(value);
  if (!Number.isNaN(asDate.getTime())) return asDate.toISOString();
  return String(value);
}

function mapRow(row: RoundRow): CrashRound | null {
  const multiplier = Number(row.multiplier);
  if (!Number.isFinite(multiplier)) return null;
  const crashedAt = toIso(row.crashed_at);
  if (!crashedAt) return null;
  return {
    gameId: row.game_id,
    multiplier,
    hash: row.hash,
    salt: row.salt,
    beganAt: toIso(row.began_at),
    crashedAt,
  };
}

async function insertNewRounds(rounds: FetchedRound[]): Promise<{ inserted: number; rounds: CrashRound[] }> {
  if (rounds.length === 0) return { inserted: 0, rounds: [] };
  const sql = await getSql();
  let inserted = 0;
  const affectedDates = new Set<string>();
  const insertedRounds: CrashRound[] = [];

  for (const round of rounds) {
    const rows = await sql<{ game_id: string }>`
      insert into crash_rounds (game_id, multiplier, hash, salt, began_at, crashed_at)
      values (
        ${round.gameId},
        ${round.multiplier},
        ${round.hash},
        ${round.salt},
        ${round.beganAt ? round.beganAt.toISOString() : null},
        ${round.crashedAt.toISOString()}
      )
      on conflict (game_id) do nothing
      returning game_id
    `;
    if (rows.length > 0) {
      inserted += 1;
      affectedDates.add(round.crashedAt.toISOString().slice(0, 10));
      insertedRounds.push({
        gameId: round.gameId,
        multiplier: round.multiplier,
        hash: round.hash,
        salt: round.salt,
        beganAt: round.beganAt ? round.beganAt.toISOString() : null,
        crashedAt: round.crashedAt.toISOString(),
      });
    }
  }

  if (affectedDates.size > 0) {
    await recomputeDaily(Array.from(affectedDates));
  }

  return { inserted, rounds: insertedRounds };
}

async function loadRounds(limit: number): Promise<CrashRound[]> {
  const sql = await getSql();
  const rows = await sql<RoundRow>`
    select game_id, multiplier, hash, salt, began_at, crashed_at
    from crash_rounds
    order by crashed_at desc, game_id desc
    limit ${limit}
  `;
  return rows.map(mapRow).filter((row): row is CrashRound => row !== null);
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

/* ── Daily aggregation ─────────────────────────────────────────────────── */

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

/** Build a [start, end) timestamp range for a calendar date (UTC). */
function dateRangeUtc(dateStr: string): { start: string; end: string } {
  const start = new Date(`${dateStr}T00:00:00.000Z`);
  const end = new Date(start);
  end.setUTCDate(end.getUTCDate() + 1);
  return { start: start.toISOString(), end: end.toISOString() };
}

/**
 * Recompute crash_daily for the given YYYY-MM-DD dates.
 * Uses an upsert so existing rows are refreshed in-place.
 * Query uses a timestamp range so the B-tree index on crashed_at is usable.
 */
async function recomputeDaily(dates: string[]): Promise<void> {
  if (dates.length === 0) return;
  const sql = await getSql();

  for (const date of dates) {
    const { start, end } = dateRangeUtc(date);
    await sql`
      insert into crash_daily (
        date, total_rounds, avg_multiplier, median_multiplier,
        highest_multiplier, lowest_multiplier,
        low_count, high_count, moon_count, sum_multipliers, updated_at
      )
      select
        ${date}::date as date,
        count(*)::int as total_rounds,
        avg(multiplier)::numeric(12,4) as avg_multiplier,
        percentile_cont(0.5) within group (order by multiplier)::numeric(12,4) as median_multiplier,
        max(multiplier)::numeric(12,4) as highest_multiplier,
        min(multiplier)::numeric(12,4) as lowest_multiplier,
        count(*) filter (where multiplier < 2)::int as low_count,
        count(*) filter (where multiplier >= 2 and multiplier < 10)::int as high_count,
        count(*) filter (where multiplier >= 10)::int as moon_count,
        sum(multiplier)::numeric(16,4) as sum_multipliers,
        now() as updated_at
      from crash_rounds
      where crashed_at >= ${start}::timestamptz
        and crashed_at < ${end}::timestamptz
      on conflict (date) do update set
        total_rounds = excluded.total_rounds,
        avg_multiplier = excluded.avg_multiplier,
        median_multiplier = excluded.median_multiplier,
        highest_multiplier = excluded.highest_multiplier,
        lowest_multiplier = excluded.lowest_multiplier,
        low_count = excluded.low_count,
        high_count = excluded.high_count,
        moon_count = excluded.moon_count,
        sum_multipliers = excluded.sum_multipliers,
        updated_at = excluded.updated_at
    `;
  }
}

export const refreshDashboard = createServerFn({ method: "POST" }).handler(
  async (): Promise<DashboardPayload> => {
    let fetched = 0;
    let inserted = 0;
    let error: string | null = null;
    let onlinePlayers: number | null = null;

    // Generate prediction for the NEXT round BEFORE discovering new outcomes.
    // This guarantees no data leakage: the prediction uses only rounds already
    // in the database, not the round whose outcome is about to be fetched.
    await generateAndQueuePrediction().catch(() => undefined);

    try {
      const [history, players] = await Promise.all([
        fetchCrashHistory(),
        fetchOnlinePlayers(),
      ]);
      fetched = history.length;
      onlinePlayers = players;
      const insertResult = await insertNewRounds(history);
      inserted = insertResult.inserted;

      // Validate the prediction queued before this poll against the newest round.
      await validateAgainstNewRounds(insertResult.rounds).catch(() => undefined);
    } catch (err) {
      error = err instanceof Error ? err.message : "Could not reach BC.Game";
    }

    const all = await loadRounds(STATS_LIMIT);
    return buildPayload(all, {
      ok: error === null,
      lastSyncAt: new Date().toISOString(),
      fetched,
      inserted,
      error,
      onlinePlayers,
    });
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

    // Overall stats are computed from crash_daily (not raw rounds) so the query
    // stays fast even when raw rounds have been purged.
    // Weighted average uses sum_multipliers for exact results.
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

/** Deep sync: fetch up to 10,000 rounds from BC.Game to backfill missed history. */
export const deepSync = createServerFn({ method: "POST" }).handler(
  async (): Promise<{ fetched: number; inserted: number }> => {
    const history = await fetchCrashHistoryDeep(200);
    const { inserted } = await insertNewRounds(history);
    return { fetched: history.length, inserted };
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
    // Vacuum reclaims dead tuples and updates planner stats.
    // Use .query() because VACUUM is a utility command.
    await sql.query("vacuum analyze crash_rounds");
    return { deleted: rows[0]?.deleted ?? 0 };
  });

/* ── CSV export ────────────────────────────────────────────────────────── */

/** Escape a CSV cell: wrap in quotes if it contains commas, quotes, or newlines. */
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
