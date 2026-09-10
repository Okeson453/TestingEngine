/**
 * Transaction helper — pins a single Neon pool client for BEGIN…COMMIT.
 * Always releases the client in `finally` so pool slots cannot leak.
 */
import type { Sql } from "@/lib/db";
import { dbSource, getPgPool, getTaggedPool } from "@/lib/db";

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

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

export async function runInTransaction<T>(
  sql: Sql,
  fn: (tx: Sql) => Promise<T>,
): Promise<T> {
  // POOL-ROUTING FIX: this used to call getPgPool() unconditionally, which
  // returns the GENERAL pool regardless of which Sql the caller passed in.
  // Every transactional write in the app — prediction persist
  // (predictor.ts), outbox claim (notification-worker.ts), validator
  // persist, crash ingest — went through this function, so the dual
  // critical/general pool split (db.ts) never actually applied to any BEGIN
  // … COMMIT transaction: they all silently shared the general pool with
  // dashboard/analytics/forensics traffic, regardless of whether the caller
  // obtained `sql` via getCriticalSql() or getSql(). Pin a client from the
  // pool the caller's `sql` was actually built from (tagged in db.ts);
  // fall back to the general pool only when the tag is absent (e.g. a
  // pre-dual-pool test double).
  const pool = getTaggedPool(sql) ?? getPgPool();
  if (pool && dbSource === "neon") {
    let client: import("pg").PoolClient | null = null;
    try {
      client = await pool.connect();
      await client.query("BEGIN");
      const held = client;
      const txSql = makeTxSql(async <U>(text: string, params: unknown[] = []) => {
        const res = await held.query(text, params);
        return res.rows as U[];
      });
      const result = await fn(txSql);
      await held.query("COMMIT");
      return result;
    } catch (err) {
      if (client) {
        try {
          await client.query("ROLLBACK");
        } catch {
          /* swallow */
        }
      }
      throw err;
    } finally {
      client?.release();
    }
  }

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

export function hasNativeTransaction(
  sql: Sql,
): sql is Sql & { transaction: <T>(fn: (tx: Sql) => Promise<T>) => Promise<T> } {
  return (
    isObject(sql) &&
    typeof (sql as unknown as Record<string, unknown>).transaction === "function"
  );
}
