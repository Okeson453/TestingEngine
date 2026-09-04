/**
 * Minimal transaction helper for the live prediction pipeline.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §5.2
 *
 * The shared `Sql` interface in `db.ts` deliberately does not expose a
 * `transaction` method (the production Neon path uses `pg.Pool`, which has
 * a `connect()` API instead). For tests and the PGLite path we expose a
 * helper that runs the supplied callback inside `BEGIN; ... COMMIT` with
 * `ROLLBACK` on error.
 *
 * Each `tx` parameter is a `Sql` instance bound to the active transaction,
 * so callers can keep using the familiar tagged-template surface.
 */

import type { Sql } from "@/lib/db";

interface TxRunner {
  begin(): Promise<void>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
  exec(text: string): Promise<void>;
  query<T>(text: string, params?: unknown[]): Promise<T[]>;
}

const noop = async (): Promise<void> => undefined;

class SimpleTxRunner implements TxRunner {
  private done = false;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private sql: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(sql: any) {
    this.sql = sql;
  }
  async begin(): Promise<void> {
    await this.sql.query("BEGIN");
  }
  async commit(): Promise<void> {
    if (this.done) return;
    this.done = true;
    await this.sql.query("COMMIT");
  }
  async rollback(): Promise<void> {
    if (this.done) return;
    this.done = true;
    try {
      await this.sql.query("ROLLBACK");
    } catch {
      /* swallow — rollback after commit/rollback is harmless */
    }
  }
  async exec(text: string): Promise<void> {
    await this.sql.query(text);
  }
  async query<T>(text: string, params: unknown[] = []): Promise<T[]> {
    return (this.sql as { query: <U>(t: string, p?: unknown[]) => Promise<U[]> }).query<T>(text, params);
  }
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

/**
 * Run the callback inside a transaction. The callback receives a `tx`
 * Sql-compatible interface (tagged-template + .query) that is bound to
 * the same connection's active transaction.
 */
export async function runInTransaction<T>(
  sql: Sql,
  fn: (tx: Sql) => Promise<T>,
): Promise<T> {
  // Use the underlying connection when possible (PGLite has its own
  // `transaction`, neon pg has `connect`/`BEGIN`). The shared `Sql`
  // interface only exposes `query` and the tagged template; we use
  // raw BEGIN/COMMIT/ROLLBACK for portability.
  const runner = new SimpleTxRunner(sql as unknown);
  await runner.begin();
  let result: T;
  try {
    // Wrap `query` so the callback's tagged-template calls go through
    // the same connection. The wrapped surface looks identical to `Sql`.
    const txSql: Sql = (async (
      strings: TemplateStringsArray,
      ...values: unknown[]
    ): Promise<Record<string, unknown>[]> => {
      let text = strings[0]!;
      for (let i = 0; i < values.length; i += 1) text += `$${i + 1}${strings[i + 1]!}`;
      return runner.query<Record<string, unknown>>(text, values);
    }) as unknown as Sql;
    txSql.query = <U = Record<string, unknown>>(text: string, params: unknown[] = []) =>
      runner.query<U>(text, params);
    result = await fn(txSql);
    await runner.commit();
    return result;
  } catch (err) {
    await runner.rollback();
    throw err;
  }
}

/** Escape hatch: detect if a query runner exposes a native transaction. */
export function hasNativeTransaction(
  sql: Sql,
): sql is Sql & { transaction: <T>(fn: (tx: Sql) => Promise<T>) => Promise<T> } {
  return isObject(sql) && typeof (sql as unknown as Record<string, unknown>).transaction === "function";
}
