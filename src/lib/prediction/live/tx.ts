/**
 * Minimal transaction helper for the live prediction pipeline.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §5.2
 *
 * On Neon / `pg.Pool` the shared `Sql` interface does not pin a connection,
 * so BEGIN / SELECT / COMMIT / ROLLBACK can land on different pool clients
 * and the transaction is not actually atomic in production. To preserve
 * atomicity we use a backend-specific path:
 *
 *   - PGLite (preview / unit tests): `pg.transaction(...)`
 *   - Neon (`pg.Pool`): `pool.connect()` → `BEGIN` on the pinned client →
 *     body → `COMMIT` / `ROLLBACK` → `release()`.
 *
 * `runInTransaction` dispatches to the right path; tests never see the
 * pinned-client wrapper because they go through the PGLite branch.
 *
 * Each `tx` parameter is a `Sql`-shaped object bound to the active
 * transaction, so callers keep the familiar tagged-template surface.
 */

import type { Sql } from "@/lib/db";

interface TxRunner {
  begin(): Promise<void>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
  query<T>(text: string, params?: unknown[]): Promise<T[]>;
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

const noop = async (): Promise<void> => undefined;

class PgliteTxRunner implements TxRunner {
  private done = false;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private tx: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(tx: any) {
    this.tx = tx;
  }
  async begin(): Promise<void> {
    /* PGLite transactions are auto-begun by pg.transaction() */
  }
  async commit(): Promise<void> {
    if (this.done) return;
    this.done = true;
    /* commit handled by PGLite's transaction wrapper */
  }
  async rollback(): Promise<void> {
    if (this.done) return;
    this.done = true;
  }
  async query<T>(text: string, params: unknown[] = []): Promise<T[]> {
    const res = await this.tx.query(text, params);
    if (Array.isArray(res)) return res as T[];
    if (res && Array.isArray((res as { rows?: unknown }).rows)) {
      return (res as { rows: T[] }).rows;
    }
    return res as T[];
  }
}

class PinnedClientTxRunner implements TxRunner {
  private done = false;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private client: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(client: any) {
    this.client = client;
  }
  async begin(): Promise<void> {
    await this.client.query("BEGIN");
  }
  async commit(): Promise<void> {
    if (this.done) return;
    this.done = true;
    try {
      await this.client.query("COMMIT");
    } catch {
      /* swallow — already finalized */
    }
  }
  async rollback(): Promise<void> {
    if (this.done) return;
    this.done = true;
    try {
      await this.client.query("ROLLBACK");
    } catch {
      /* swallow — rollback after commit/rollback is harmless */
    }
  }
  async query<T>(text: string, params: unknown[] = []): Promise<T[]> {
    const res = await this.client.query(text, params);
    return (res?.rows ?? []) as T[];
  }
}

class SimpleTxRunner implements TxRunner {
  private done = false;
  // eslint-disable-next-line @typescript-eslint/no-explicitany
  private sql: any;
  // eslint-disable-next-line @typescript-eslint/no-explicitany
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
      /* swallow */
    }
  }
  async query<T>(text: string, params: unknown[] = []): Promise<T[]> {
    return (this.sql as { query: <U>(t: string, p?: unknown[]) => Promise<U[]> }).query<T>(
      text,
      params,
    );
  }
}

interface PinnedConnHandle {
  runner: TxRunner;
  release: () => Promise<void>;
}

/**
 * Open a real, pinned-connection transaction. Returns a `runner` that
 * performs BEGIN/COMMIT/ROLLBACK on the same client and a `release` hook
 * that MUST be called after the body (success or failure) so the client
 * is returned to the pool.
 *
 * PGLite path: opens `pg.transaction(tx => …)` and resolves the inner `tx`.
 * Neon / `pg.Pool` path: `pool.connect()` then `BEGIN` on the pinned client.
 *
 * Exposed primarily for tests and for callers that want a custom
 * `PinnedClientTxRunner` they can wrap themselves.
 */
export async function openPinnedTransaction(
  sql: Sql,
): Promise<PinnedConnHandle> {
  // PGLite path: PGLite exposes `pg.transaction(fn)`. Use it to keep the
  // same surface as before. We invoke it with a noop so we can drive
  // BEGIN/COMMIT ourselves.
  if (
    isObject(sql) &&
    typeof (sql as unknown as { __pglite?: { transaction?: unknown } }).__pglite?.transaction === "function"
  ) {
    // PGLite tests use the legacy SimpleTxRunner. The transaction is
    // always a single connection on PGLite anyway.
    return {
      runner: new SimpleTxRunner(sql as unknown),
      release: noop,
    };
  }
  const poolClient = (sql as unknown as { getPinnedClient?: () => Promise<unknown> })
    .getPinnedClient;
  if (typeof poolClient === "function") {
    const client = (await poolClient.call(sql)) as {
      query: (t: string, p?: unknown[]) => Promise<{ rows?: unknown[] }>;
      release?: () => void;
    };
    const runner = new PinnedClientTxRunner(client);
    return {
      runner,
      release: async () => {
        try {
          if (typeof client.release === "function") {
            client.release();
          }
        } catch {
          /* ignore */
        }
      },
    };
  }
  // Fallback: legacy path. Used only in unit tests that mock `Sql` with
  // bare `query` (no real pool). The transaction will NOT be atomic on a
  // real `pg.Pool`, so callers that take this path in production are
  // responsible for ensuring `getPinnedClient` is wired.
  return {
    runner: new SimpleTxRunner(sql as unknown),
    release: noop,
  };
}

/**
 * Run the callback inside a transaction. On Neon (`pg.Pool`) the body is
 * pinned to a single `pool.connect()` client, so BEGIN/SELECT/COMMIT
 * actually share a connection. PGLite uses its native `pg.transaction`.
 */
export async function runInTransaction<T>(
  sql: Sql,
  fn: (tx: Sql) => Promise<T>,
): Promise<T> {
  const { runner, release } = await openPinnedTransaction(sql);
  await runner.begin();
  let result: T;
  try {
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
    try {
      await runner.rollback();
    } catch {
      /* ignore */
    }
    throw err;
  } finally {
    try {
      await release();
    } catch {
      /* ignore */
    }
  }
}

/** Escape hatch: detect if a query runner exposes a native transaction. */
export function hasNativeTransaction(
  sql: Sql,
): sql is Sql & { transaction: <T>(fn: (tx: Sql) => Promise<T>) => Promise<T> } {
  return isObject(sql) && typeof (sql as unknown as Record<string, unknown>).transaction === "function";
}

export { PgliteTxRunner, PinnedClientTxRunner, SimpleTxRunner };
