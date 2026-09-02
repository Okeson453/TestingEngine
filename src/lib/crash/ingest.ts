import { getSql } from "@/lib/db";
import type { Sql } from "@/lib/db";
import type { FetchedRound } from "./fetch-bc";
import type { CrashRound } from "./types";

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

/**
 * Insert any brand-new BC.Game rounds into `crash_rounds`.
 * Idiosyncratic by `game_id` (primary key) — re-running with already-known
 * rounds is a complete no-op, so the worker may poll freely without risk of
 * duplicating data. Recomputes `crash_daily` aggregates for touched dates.
 * Returns only the rows that were genuinely new (the rest were deduped).
 */
export async function insertNewRounds(
  rounds: FetchedRound[],
): Promise<{ inserted: number; rounds: CrashRound[] }> {
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
      // FIX: beganAt is the round's start time, NOT the crash time. Previously
      // this used crashedAt as beganAt, which leaked the round's outcome into
      // history rows and made temporal-integrity checks meaningless.
      const beganIso = round.beganAt ? round.beganAt.toISOString() : null;
      insertedRounds.push({
        gameId: round.gameId,
        multiplier: round.multiplier,
        hash: round.hash,
        salt: round.salt,
        beganAt: beganIso,
        crashedAt: round.crashedAt.toISOString(),
      });
    }
  }

  if (affectedDates.size > 0) {
    await recomputeDaily(Array.from(affectedDates));
  }

  return { inserted, rounds: insertedRounds };
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
