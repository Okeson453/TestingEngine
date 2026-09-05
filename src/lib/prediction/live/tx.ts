/**
 * Transaction helper for the live prediction pipeline.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §5.2
 * Production hardening: pin a single connection on Neon so BEGIN/COMMIT
 * (and FOR UPDATE SKIP LOCKED) are actually atomic.
 *
 * On Neon (`pg.Pool`): acquire a dedicated client via pool.connect(), run
 * BEGIN → body → COMMIT/ROLLBACK on that client, then release.
 * On PGLite (tests/dev): fall back to sequential BEGIN/COMMIT via the
 * shared Sql surface (single underlying connection).
 */

import type { Sql } from "@/lib/db";
import { dbSource, getPgPool } from "@/lib/db";

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

/** Build a Sql-compatible tagged-template + .query surface over a runner. */
function makeTxSql(
  queryFn: <T>(text: string, params?: unknown[]) => Promise<T[]>,
): Sql {
  const txSql = (async (
    strings: TemplateStringsArray,
    ...values: unknown[]
  ): Promise<Record<string, unknown>[]> => {
    let text = strings[0]!;
    for (let i = 0; i < values.length; i += 1) {
      text += `$${i + 1}${strings[i + 1]!}`;
    }
    return queryFn<Record<string, unknown>>(text, values);
  }) as unknown as Sql;
  txSql.query = <U = Record<string, unknown>>(
    text: string,
    params: unknown[] = [],
  ) => queryFn<U>(text, params);
  return txSql;
}

/**
 * Run the callback inside a real transaction.
 * On Neon the entire BEGIN…COMMIT runs on one pinned pool client.
 * The callback receives a Sql-compatible `tx` bound to that connection.
 */
export async function runInTransaction<T>(
  sql: Sql,
  fn: (tx: Sql) => Promise<T>,
): Promise<T> {
  // Prefer pinned pool client when available (Neon production path).
  const pool = getPgPool();
  if (pool && dbSource === "neon") {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const txSql = makeTxSql(async <U>(text: string, params: unknown[] = []) => {
        const res = await client.query(text, params);
        return res.rows as U[];
      });
      const result = await fn(txSql);
      await client.query("COMMIT");
      return result;
    } catch (err) {
      try {
        await client.query("ROLLBACK");
      } catch {
        /* swallow — rollback after failed begin is harmless */
      }
      throw err;
    } finally {
      client.release();
    }
  }

  // PGLite / fallback path: sequential BEGIN/COMMIT on the shared Sql.
  // PGLite is single-connection so affinity is not an issue.
  let done = false;
  await sql.query("BEGIN");
  try {
    const txSql = makeTxSql(async <U>(text: string, params: unknown[] = []) =>
      sql.query<U>(text, params),
    );
    const result = await fn(txSql);
    done = true;
    await sql.query("COMMIT");
    return result;
  } catch (err) {
    if (!done) {
      try {
        await sql.query("ROLLBACK");
      } catch {
        /* swallow */
      }
    }
    throw err;
  }
}

/** Escape hatch: detect if a query runner exposes a native transaction. */
export function hasNativeTransaction(
  sql: Sql,
): sql is Sql & { transaction: <T>(fn: (tx: Sql) => Promise<T>) => Promise<T> } {
  return (
    isObject(sql) &&
    typeof (sql as unknown as Record<string, unknown>).transaction === "function"
  );
}
