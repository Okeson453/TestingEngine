import { getSql } from "@/lib/db";
import type { Sql } from "@/lib/db";
import type { FetchedRound } from "./fetch-bc";
import type { CrashRound } from "./types";
import { globalRecentRoundCache } from "@/lib/observability/performance/hot-cache";
import { ingestMs } from "@/lib/observability/performance/latency";

export type RoundRow = {
  game_id: string;
  multiplier: string | number;
  hash: string | null;
  salt: string | null;
  began_at: string | Date | null;
  crashed_at: string | Date;
};

export function toIso(value: string | Date | null | undefined): string | null {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString();
  const asDate = new Date(value);
  if (!Number.isNaN(asDate.getTime())) return asDate.toISOString();
  return String(value);
}

export function mapRow(row: RoundRow): CrashRound | null {
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

type Prepared = {
  gameId: string;
  multiplier: number;
  hash: string | null;
  salt: string | null;
  beganParam: Date | null;
  crashedAt: Date;
};

function prepareRound(round: FetchedRound): Prepared | null {
  const beganAt: Date | null =
    round.beganAt instanceof Date
      ? round.beganAt
      : round.beganAt
        ? new Date(round.beganAt)
        : null;
  const crashedAt: Date =
    round.crashedAt instanceof Date ? round.crashedAt : new Date(round.crashedAt);
  if (Number.isNaN(crashedAt.getTime())) return null;
  const beganParam =
    beganAt && !Number.isNaN(beganAt.getTime()) ? beganAt : null;
  return {
    gameId: round.gameId,
    multiplier: round.multiplier,
    hash: round.hash,
    salt: round.salt,
    beganParam,
    crashedAt,
  };
}

/**
 * Insert any brand-new BC.Game rounds into `crash_rounds`.
 * Idempotent by `game_id` (primary key) — ON CONFLICT DO NOTHING.
 *
 * Phase 9: RecentRoundCache filters known IDs before DB work.
 * Phase 10: Prefer a single multi-row INSERT in one transaction;
 * falls back to per-row inserts on binder/type errors (Neon/PgBouncer).
 */
export async function insertNewRounds(
  rounds: FetchedRound[],
): Promise<{ inserted: number; rounds: CrashRound[] }> {
  if (rounds.length === 0) return { inserted: 0, rounds: [] };
  const t0 = performance.now();

  // Phase 9 — skip rounds already in hot cache (still safe: ON CONFLICT below)
  const candidates = globalRecentRoundCache.filterUnknown(rounds);
  if (candidates.length === 0) {
    // Still seed cache from input so consecutive polls stay cheap
    globalRecentRoundCache.addMany(
      rounds.map((r) => ({
        gameId: r.gameId,
        multiplier: r.multiplier,
        crashedAt: r.crashedAt,
      })),
    );
    ingestMs.observe(performance.now() - t0);
    return { inserted: 0, rounds: [] };
  }

  const prepared: Prepared[] = [];
  for (const r of candidates) {
    const p = prepareRound(r);
    if (p) prepared.push(p);
  }
  if (prepared.length === 0) {
    ingestMs.observe(performance.now() - t0);
    return { inserted: 0, rounds: [] };
  }

  const sql = await getSql();
  const affectedDates = new Set<string>();
  const insertedRounds: CrashRound[] = [];

  // Phase 10/15 — single multi-row INSERT via unnest (1 RTT, 1 parse plan).
  // Prior path issued N INSERT statements inside one TX (~N RTTs of parse/
  // bind on Neon). For a 50-round poll page that was the dominant ingest cost.
  let batchOk = false;
  if (prepared.length >= 1) {
    try {
      const gameIds = prepared.map((p) => p.gameId);
      const multipliers = prepared.map((p) => p.multiplier);
      const hashes = prepared.map((p) => p.hash);
      const salts = prepared.map((p) => p.salt);
      const beganAts = prepared.map((p) => p.beganParam);
      const crashedAts = prepared.map((p) => p.crashedAt);
      const result = await sql.query<{ game_id: string }>(
        `INSERT INTO crash_rounds (game_id, multiplier, hash, salt, began_at, crashed_at)
         SELECT * FROM unnest(
           $1::text[],
           $2::numeric[],
           $3::text[],
           $4::text[],
           $5::timestamptz[],
           $6::timestamptz[]
         ) AS t(game_id, multiplier, hash, salt, began_at, crashed_at)
         ON CONFLICT (game_id) DO NOTHING
         RETURNING game_id`,
        [gameIds, multipliers, hashes, salts, beganAts, crashedAts],
      );
      const returned = new Set(result.map((r) => r.game_id));
      for (const p of prepared) {
        if (returned.has(p.gameId)) {
          affectedDates.add(p.crashedAt.toISOString().slice(0, 10));
          insertedRounds.push({
            gameId: p.gameId,
            multiplier: p.multiplier,
            hash: p.hash,
            salt: p.salt,
            beganAt: p.beganParam ? p.beganParam.toISOString() : null,
            crashedAt: p.crashedAt.toISOString(),
          });
        }
      }
      batchOk = true;
    } catch (batchErr) {
      console.error(
        `[ingest] multi-row unnest insert failed, falling back per-row: ${String(batchErr)}`,
      );
      insertedRounds.length = 0;
      affectedDates.clear();
    }
  }

  if (!batchOk) {
    for (const p of prepared) {
      try {
        const result = await sql<{ game_id: string }>`
          insert into crash_rounds (game_id, multiplier, hash, salt, began_at, crashed_at)
          values (
            ${p.gameId},
            ${p.multiplier},
            ${p.hash},
            ${p.salt},
            ${p.beganParam},
            ${p.crashedAt}
          )
          on conflict (game_id) do nothing
          returning game_id
        `;
        if (result.length > 0) {
          affectedDates.add(p.crashedAt.toISOString().slice(0, 10));
          insertedRounds.push({
            gameId: p.gameId,
            multiplier: p.multiplier,
            hash: p.hash,
            salt: p.salt,
            beganAt: p.beganParam ? p.beganParam.toISOString() : null,
            crashedAt: p.crashedAt.toISOString(),
          });
        }
      } catch (rowErr) {
        console.error(
          `[ingest] insert game_id=${p.gameId} failed: ${String(rowErr)}`,
        );
      }
    }
  }

  // Update hot cache with everything we saw (inserted or already present)
  globalRecentRoundCache.addMany(
    prepared.map((p) => ({
      gameId: p.gameId,
      multiplier: p.multiplier,
      crashedAt: p.crashedAt,
    })),
  );

  if (affectedDates.size > 0) {
    // Daily aggregates are BACKGROUND — never hold the poll/recovery tick.
    // Failure is soft; next tick or cleanup job can recompute.
    void recomputeDaily(Array.from(affectedDates)).catch((e) => {
      console.error(`[ingest] recomputeDaily soft-failed: ${String(e)}`);
    });
  }

  ingestMs.observe(performance.now() - t0);
  return { inserted: insertedRounds.length, rounds: insertedRounds };
}

export function dateRangeUtc(dateStr: string): { start: string; end: string } {
  const start = new Date(`${dateStr}T00:00:00.000Z`);
  const end = new Date(start);
  end.setUTCDate(end.getUTCDate() + 1);
  return { start: start.toISOString(), end: end.toISOString() };
}

/**
 * Recompute crash_daily for the given YYYY-MM-DD dates (upsert, idempotent).
 * Query uses a timestamp range so the B-tree index on crashed_at is usable.
 */
export async function recomputeDaily(dates: string[]): Promise<void> {
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

export async function loadRounds(limit: number): Promise<CrashRound[]> {
  const sql = await getSql();
  const rows = await sql<RoundRow>`
    select game_id, multiplier, hash, salt, began_at, crashed_at
    from crash_rounds
    order by crashed_at desc, game_id desc
    limit ${limit}
  `;
  return rows.map(mapRow).filter((row): row is CrashRound => row !== null);
}

/** Latest (most recent) ingested round, or null when none exist yet. */
export async function loadLatestRound(sql?: Sql): Promise<CrashRound | null> {
  const s = sql ?? (await getSql());
  const rows = await s<RoundRow>`
    select game_id, multiplier, hash, salt, began_at, crashed_at
    from crash_rounds
    order by crashed_at desc, game_id desc
    limit 1
  `;
  return rows.map(mapRow).find((row): row is CrashRound => row !== null) ?? null;
}
