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

/** Per-stage durations of ONE transaction, measured at the transaction itself.
 * Remediation plan §4: acquisition must never be derived from a global
 * "last acquisition" variable — the measurement belongs to the specific
 * transaction being measured. */
export interface TxStageTimings {
  /** Pool/client acquisition for THIS transaction. */
  acquireMs: number;
  /** BEGIN round trip. */
  beginMs: number;
  /** Duration of the caller's statement(s) inside the transaction. */
  bodyMs: number;
  /** COMMIT round trip. */
  commitMs: number;
  /** Total pin-to-release wall time (acquire + begin + body + commit). */
  totalMs: number;
}

export type TxStageReporter = (t: TxStageTimings) => void;

/**
 * FORENSIC INSTRUMENTATION GAP (closed here): TxStageTimings has always been
 * computed per-transaction, but `runInTransaction`'s `onStage` parameter was
 * never supplied by any caller (predictor.ts persist, notification-worker.ts
 * outbox claim, validator.ts persist) — every one of those transactions
 * logged, at best, an undifferentiated total via the caller's own ad-hoc
 * timing, with no way to tell whether a slow transaction was acquire-bound
 * (cold/contended connection), begin-bound, body-bound (the caller's actual
 * statement), or commit-bound. That ambiguity is exactly what makes a slow
 * transaction get reflexively blamed on "pool contention" — it may not be.
 * `logSlowTxStages` gives every `runInTransaction` caller a one-line opt-in
 * to get a stage breakdown logged whenever the transaction is slow, without
 * having to duplicate this logic at each call site.
 */
export function logSlowTxStages(label: string, thresholdMs = 300): TxStageReporter {
  return (t: TxStageTimings) => {
    if (t.totalMs < thresholdMs) return;
    // Identify the dominant stage so a slow-transaction log line points
    // straight at "acquire" (connection/pool), "begin"/"commit" (round-trip
    // overhead), or "body" (the caller's own statement) instead of forcing
    // a re-read of raw numbers to figure out which one actually mattered.
    const stages: Array<[string, number]> = [
      ["acquire", t.acquireMs],
      ["begin", t.beginMs],
      ["body", t.bodyMs],
      ["commit", t.commitMs],
    ];
    const dominant = stages.reduce((a, b) => (b[1] > a[1] ? b : a));
    console.warn(
      `[tx] ${label} slow_tx_ms=${t.totalMs} dominant=${dominant[0]} ` +
        `acquire_ms=${t.acquireMs} begin_ms=${t.beginMs} body_ms=${t.bodyMs} commit_ms=${t.commitMs}`,
    );
  };
}

export async function runInTransaction<T>(
  sql: Sql,
  fn: (tx: Sql) => Promise<T>,
  onStage?: TxStageReporter,
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
  const t0 = Date.now();
  if (pool && dbSource === "neon") {
    let client: import("pg").PoolClient | null = null;
    let acquireMs = 0;
    let beginMs = 0;
    let bodyMs = 0;
    let commitMs = 0;
    try {
      const acquireT0 = Date.now();
      client = await pool.connect();
      acquireMs = Date.now() - acquireT0;
      const beginT0 = Date.now();
      await client.query("BEGIN");
      beginMs = Date.now() - beginT0;
      const held = client;
      const txSql = makeTxSql(async <U>(text: string, params: unknown[] = []) => {
        const res = await held.query(text, params);
        return res.rows as U[];
      });
      const bodyT0 = Date.now();
      const result = await fn(txSql);
      bodyMs = Date.now() - bodyT0;
      const commitT0 = Date.now();
      await held.query("COMMIT");
      commitMs = Date.now() - commitT0;
      onStage?.({
        acquireMs,
        beginMs,
        bodyMs,
        commitMs,
        totalMs: Date.now() - t0,
      });
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
  const acquireT0 = Date.now();
  await sql.query("BEGIN");
  const beginMs = Date.now() - acquireT0;
  try {
    const txSql = makeTxSql(async <U>(text: string, params: unknown[] = []) =>
      sql.query<U>(text, params),
    );
    const bodyT0 = Date.now();
    const result = await fn(txSql);
    const bodyMs = Date.now() - bodyT0;
    done = true;
    const commitT0 = Date.now();
    await sql.query("COMMIT");
    const commitMs = Date.now() - commitT0;
    onStage?.({
      acquireMs: 0, // mono-pool fallback: no pooled client acquisition to time
      beginMs,
      bodyMs,
      commitMs,
      totalMs: Date.now() - t0,
    });
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
