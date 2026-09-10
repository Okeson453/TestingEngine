/**
 * RoundRepository — durable access to the canonical historical rounds table
 * (`crash_rounds`). PostgreSQL (Neon) or PGLite via `getSql()`.
 *
 * Canonical mapping (crash_rounds is the authoritative historical store):
 *   game_id    → id / externalRoundId (unique external round identity)
 *   multiplier → crashPoint (single confirmed crash point; the schema has no
 *                observed/confirmed split — the ED upsert writes one value)
 *   crashed_at → crashedAt
 *   began_at   → startedAt (nullable: ED can arrive before BG)
 *   ingested_at → createdAt
 *
 * There is no durable session/observation-source/data-quality dimension in
 * this schema — the domain layer models those as null, it does not fabricate
 * them.
 */

import { getSql } from '../../db.ts';

export interface RoundRecord {
  id: string;
  externalRoundId: string;
  crashPoint: number;
  crashedAt: string;
  startedAt: string | null;
  createdAt: string;
}

interface CrashRoundRow {
  game_id: string;
  multiplier: string | number;
  crashed_at: string | Date | null;
  began_at: string | Date | null;
  ingested_at: string | Date | null;
}

function toIso(value: string | Date | null): string | null {
  if (value == null) return null;
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function toRecord(row: CrashRoundRow): RoundRecord {
  const crashedAt = toIso(row.crashed_at) ?? new Date(0).toISOString();
  return {
    id: String(row.game_id),
    externalRoundId: String(row.game_id),
    crashPoint: Number(row.multiplier),
    crashedAt,
    startedAt: toIso(row.began_at),
    createdAt: toIso(row.ingested_at) ?? crashedAt,
  };
}

export class RoundRepository {
  /** Most recent completed rounds, newest first. */
  async findRecentCompleted(limit = 200): Promise<RoundRecord[]> {
    const sql = await getSql();
    const rows = await sql<CrashRoundRow>`
      select game_id, multiplier, crashed_at, began_at, ingested_at
      from crash_rounds
      where crashed_at is not null
      order by crashed_at desc
      limit ${limit}
    `;
    return rows.map(toRecord);
  }

  /** Completed rounds with crashed_at in [fromIso, toIso], oldest first. */
  async findCompletedInRange(
    fromIso: string,
    toIso: string,
    limit = 5000,
  ): Promise<RoundRecord[]> {
    const sql = await getSql();
    const rows = await sql<CrashRoundRow>`
      select game_id, multiplier, crashed_at, began_at, ingested_at
      from crash_rounds
      where crashed_at is not null
        and crashed_at >= ${fromIso}
        and crashed_at <= ${toIso}
      order by crashed_at asc
      limit ${limit}
    `;
    return rows.map(toRecord);
  }
}
