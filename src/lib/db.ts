/**
 * Database client — Neon Postgres (production) / PGLite (local).
 *
 * Dual pool isolation (P0):
 *   - criticalPool: prediction persist + outbox dispatch (reserved capacity)
 *   - generalPool:  dashboard, analytics, feedback, background
 * Shared Neon connection budget is split; critical path never waits behind
 * dashboard fan-out.
 */
import { mkdirSync } from "node:fs";
import { join } from "node:path";

export type DbSource = "neon" | "pglite";

const rawDatabaseUrl =
  typeof process !== "undefined" ? process.env.DATABASE_URL : undefined;
const databaseUrl =
  rawDatabaseUrl && rawDatabaseUrl.trim() ? rawDatabaseUrl : undefined;

export const pgliteDataPath =
  (typeof process !== "undefined" && process.env.PG_DATA_PATH) ||
  join(process.cwd(), "data", "crashwave");

export const dbSource: DbSource = databaseUrl ? "neon" : "pglite";

export interface Sql {
  <T = Record<string, unknown>>(
    strings: TemplateStringsArray,
    ...values: unknown[]
  ): Promise<T[]>;
  query<T = Record<string, unknown>>(
    text: string,
    params?: unknown[],
  ): Promise<T[]>;
}

const globalRef = globalThis as typeof globalThis & {
  __pgSqlPromise__?: Promise<Sql>;
  __pgCriticalSqlPromise__?: Promise<Sql>;
  __pgPool__?: import("pg").Pool;
  __pgCriticalPool__?: import("pg").Pool;
  __pgliteInstance__?: Promise<import("@electric-sql/pglite").PGlite>;
  __pgPoolEnding__?: Promise<void>;
  __poolExhaustionAlerted__?: boolean;
  __lastPoolAcquireMs__?: number;
};

const OID_INT8 = 20;
const OID_DATE = 1082;
const identity = (v: string) => v;

type Run = <T>(text: string, params: unknown[]) => Promise<T[]>;

/**
 * Sql wrappers are tagged with the `pg.Pool` they were built from so that
 * `runInTransaction` (tx.ts) can pin a client from the SAME pool the caller
 * asked for (critical vs general) instead of always reaching for the
 * general/dashboard pool. See tx.ts for the bug this closes.
 */
type TaggedSql = Sql & { __pgPool?: import("pg").Pool };

function toSql(run: Run, pool?: import("pg").Pool): Sql {
  const sql = (async <T = Record<string, unknown>>(
    strings: TemplateStringsArray,
    ...values: unknown[]
  ): Promise<T[]> => {
    let text = strings[0] ?? "";
    const params: unknown[] = [];
    for (let i = 0; i < values.length; i++) {
      params.push(values[i]);
      text += `$${i + 1}` + (strings[i + 1] ?? "");
    }
    return run<T>(text, params);
  }) as TaggedSql;
  sql.query = async <T = Record<string, unknown>>(text: string, params?: unknown[]) =>
    run<T>(text, params ?? []);
  if (pool) sql.__pgPool = pool;
  return sql;
}

/** Read back the pool a Sql wrapper was tagged with (if any). Used by tx.ts. */
export function getTaggedPool(sql: Sql): import("pg").Pool | undefined {
  return (sql as TaggedSql).__pgPool;
}

function readTotalMax(): number {
  const raw = Number(process.env.PG_POOL_MAX ?? 8);
  return Math.max(2, Math.min(Number.isFinite(raw) ? raw : 8, 12));
}

function readCriticalMax(): number {
  const total = readTotalMax();
  const raw = Number(process.env.PG_CRITICAL_POOL_MAX ?? 3);
  const crit = Math.max(1, Math.min(Number.isFinite(raw) ? raw : 3, total - 1));
  return crit;
}

function readGeneralMax(): number {
  return Math.max(1, readTotalMax() - readCriticalMax());
}

export interface PoolStats {
  totalCount: number;
  idleCount: number;
  waitingCount: number;
  max: number;
  label: string;
}

export function getPoolStats(): PoolStats | null {
  const pool = globalRef.__pgPool__;
  if (!pool) return null;
  return {
    totalCount: pool.totalCount,
    idleCount: pool.idleCount,
    waitingCount: pool.waitingCount,
    max: readGeneralMax(),
    label: "general",
  };
}

export function getCriticalPoolStats(): PoolStats | null {
  const pool = globalRef.__pgCriticalPool__;
  if (!pool) return null;
  return {
    totalCount: pool.totalCount,
    idleCount: pool.idleCount,
    waitingCount: pool.waitingCount,
    max: readCriticalMax(),
    label: "critical",
  };
}

/** Last pool acquire latency (ms) observed by any run() — for PERSIST_PROFILE. */
export function getLastPoolAcquireMs(): number | null {
  return globalRef.__lastPoolAcquireMs__ ?? null;
}

function makeRun(
  pool: import("pg").Pool,
  label: string,
): Run {
  return async <T>(text: string, params: unknown[]) => {
    const t0 = Date.now();
    let client: import("pg").PoolClient;
    try {
      client = await pool.connect();
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(
        `[db] ${label} POOL ACQUIRE FAILED: ${msg} | total=${pool.totalCount} idle=${pool.idleCount} waiting=${pool.waitingCount} max=${pool.options.max}`,
      );
      throw err;
    }
    const acquireMs = Date.now() - t0;
    globalRef.__lastPoolAcquireMs__ = acquireMs;
    if (acquireMs > 100) {
      console.warn(
        `[db] ${label} pool_acquire_ms=${acquireMs} total=${pool.totalCount} idle=${pool.idleCount} waiting=${pool.waitingCount}`,
      );
    }
    try {
      const q0 = Date.now();
      const res = await client.query(text, params);
      const queryMs = Date.now() - q0;
      if (queryMs > 500) {
        console.warn(`[db] ${label} slow_query_ms=${queryMs} text=${text.slice(0, 80)}`);
      }
      return res.rows as T[];
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (
        msg.includes("timeout exceeded when trying to connect") ||
        msg.includes("remaining connection slots")
      ) {
        console.error(
          `[db] ${label} POOL EXHAUSTION: ${msg} | total=${pool.totalCount} idle=${pool.idleCount} waiting=${pool.waitingCount}`,
        );
      }
      throw err;
    } finally {
      client!.release();
    }
  };
}

async function createNeonPools(): Promise<{ general: Sql; critical: Sql }> {
  const { Pool, types } = await import("pg");
  types.setTypeParser(OID_INT8, identity);
  types.setTypeParser(OID_DATE, identity);

  const criticalMax = readCriticalMax();
  const generalMax = readGeneralMax();
  const idleTimeoutMillis = (Number(process.env.PG_IDLE_TIMEOUT_MS ?? 15_000) || 15_000);
  // Critical path: short acquire timeout (do not sit 30s behind dashboard)
  const criticalConnTimeout =
    (Number(process.env.PG_CRITICAL_CONN_TIMEOUT_MS ?? 5_000) || 5_000);
  // General / dashboard: bounded, still shorter than legacy 30s default
  const generalConnTimeout =
    (Number(process.env.PG_CONN_TIMEOUT_MS ?? 8_000) || 8_000);
  const dashboardConnTimeout =
    (Number(process.env.PG_DASHBOARD_CONN_TIMEOUT_MS ?? 3_000) || 3_000);

  console.log(
    `[db] Dual pool: critical max=${criticalMax} connTimeoutMs=${criticalConnTimeout}; general max=${generalMax} connTimeoutMs=${generalConnTimeout}; dashboard acquire budget=${dashboardConnTimeout}`,
  );

  const criticalPool = new Pool({
    connectionString: databaseUrl,
    max: criticalMax,
    // WARM-POOL FIX (production trace 18:35-18:39): with min=0 and a 15s
    // idle timeout, both pools went cold between rounds (crash rounds are
    // spaced 10-70s). Every cold acquire paid ~1s of TLS+auth to Neon —
    // the direct cause of the intermittent ~700-1000ms prediction
    // persistence leg and ~1.3-2.0s dispatch leg (warm rounds: ~1ms).
    // min keeps one connection alive; MAX is unchanged — this is connection
    // warming, not pool-size growth.
    min: Math.min(Math.max(0, Number(process.env.PG_POOL_MIN_IDLE ?? 1) || 1), criticalMax),
    idleTimeoutMillis,
    connectionTimeoutMillis: criticalConnTimeout,
    ssl: process.env.PG_SSL === "0" ? false : { rejectUnauthorized: false },
  });
  const generalPool = new Pool({
    connectionString: databaseUrl,
    max: generalMax,
    min: Math.min(Math.max(0, Number(process.env.PG_POOL_MIN_IDLE ?? 1) || 1), generalMax),
    idleTimeoutMillis,
    connectionTimeoutMillis: generalConnTimeout,
    ssl: process.env.PG_SSL === "0" ? false : { rejectUnauthorized: false },
  });

  globalRef.__pgCriticalPool__ = criticalPool;
  globalRef.__pgPool__ = generalPool; // getPgPool / dashboard pin = general

  const monitor = setInterval(() => {
    for (const [label, pool, max] of [
      ["critical", criticalPool, criticalMax],
      ["general", generalPool, generalMax],
    ] as const) {
      if (pool.totalCount >= max && pool.idleCount === 0 && pool.waitingCount > 0) {
        console.error(
          `[db] ${label} POOL PRESSURE: total=${pool.totalCount} idle=${pool.idleCount} waiting=${pool.waitingCount} max=${max}`,
        );
      }
    }
  }, 5_000);
  monitor.unref?.();

  criticalPool.on("error", (err) => console.error("[db] critical pool error:", err.message));
  generalPool.on("error", (err) => console.error("[db] general pool error:", err.message));

  return {
    critical: toSql(makeRun(criticalPool, "critical"), criticalPool),
    general: toSql(makeRun(generalPool, "general"), generalPool),
  };
}

async function createPgliteSql(): Promise<Sql> {
  mkdirSync(pgliteDataPath, { recursive: true });
  const { PGlite } = await import("@electric-sql/pglite");
  const db = new PGlite(pgliteDataPath);
  await db.waitReady;
  const run: Run = async <T>(text: string, params: unknown[]) => {
    const res = await db.query(text, params);
    return (res.rows ?? []) as T[];
  };
  return toSql(run);
}

export async function getSql(): Promise<Sql> {
  if (!globalRef.__pgSqlPromise__) {
    if (databaseUrl) {
      globalRef.__pgSqlPromise__ = createNeonPools().then((p) => {
        globalRef.__pgCriticalSqlPromise__ = Promise.resolve(p.critical);
        return p.general;
      });
    } else {
      globalRef.__pgSqlPromise__ = createPgliteSql();
      globalRef.__pgCriticalSqlPromise__ = globalRef.__pgSqlPromise__;
    }
  }
  return globalRef.__pgSqlPromise__;
}

/** Latency-critical path: prediction persist + outbox (reserved pool). */
export async function getCriticalSql(): Promise<Sql> {
  await getSql(); // ensure pools initialized
  if (globalRef.__pgCriticalSqlPromise__) {
    return globalRef.__pgCriticalSqlPromise__;
  }
  return getSql();
}

export async function endPgPool(): Promise<void> {
  if (globalRef.__pgPoolEnding__) return globalRef.__pgPoolEnding__;
  globalRef.__pgPoolEnding__ = (async () => {
    const g = globalRef.__pgPool__;
    const c = globalRef.__pgCriticalPool__;
    globalRef.__pgPool__ = undefined;
    globalRef.__pgCriticalPool__ = undefined;
    globalRef.__pgSqlPromise__ = undefined;
    globalRef.__pgCriticalSqlPromise__ = undefined;
    await Promise.all([
      g ? g.end().catch(() => undefined) : Promise.resolve(),
      c ? c.end().catch(() => undefined) : Promise.resolve(),
    ]);
  })();
  return globalRef.__pgPoolEnding__;
}

export function getPgPool(): import("pg").Pool | null {
  return globalRef.__pgPool__ ?? null;
}

export function getCriticalPool(): import("pg").Pool | null {
  return globalRef.__pgCriticalPool__ ?? null;
}

export async function withPinnedClient<T>(
  fn: (client: import("pg").PoolClient) => Promise<T>,
): Promise<T> {
  const pool = getPgPool();
  if (!pool) {
    throw new Error("withPinnedClient requires Postgres pool (not PGLite)");
  }
  const t0 = Date.now();
  const client = await pool.connect();
  globalRef.__lastPoolAcquireMs__ = Date.now() - t0;
  try {
    return await fn(client);
  } finally {
    client.release();
  }
}

/** Dashboard-only: fail fast if general pool is busy (do not block 30s). */
export async function withDashboardClient<T>(
  fn: (client: import("pg").PoolClient) => Promise<T>,
  timeoutMs = (Number(process.env.PG_DASHBOARD_CONN_TIMEOUT_MS ?? 3_000) || 3_000),
): Promise<T> {
  const pool = getPgPool();
  if (!pool) {
    throw new Error("withDashboardClient requires Postgres pool");
  }
  const t0 = Date.now();
  const client = await Promise.race([
    pool.connect(),
    new Promise<never>((_, rej) =>
      setTimeout(
        () =>
          rej(
            new Error(
              `dashboard DB acquire timeout after ${timeoutMs}ms (pool busy)`,
            ),
          ),
        timeoutMs,
      ),
    ),
  ]);
  globalRef.__lastPoolAcquireMs__ = Date.now() - t0;
  try {
    return await fn(client);
  } finally {
    client.release();
  }
}


/** Auth bootstrap: ensure general SQL (and PGLite when no DATABASE_URL) is ready. */
export async function ensureDbReady(): Promise<void> {
  await getSql();
}

/**
 * PGLite handle for Better Auth when running without DATABASE_URL.
 * Production Neon path should not call this.
 */
export async function getPglite(): Promise<import("@electric-sql/pglite").PGlite> {
  if (databaseUrl) {
    throw new Error("getPglite() is only available without DATABASE_URL (local PGLite mode)");
  }
  const g = globalThis as typeof globalThis & {
    __pgliteAuth__?: Promise<import("@electric-sql/pglite").PGlite>;
  };
  if (!g.__pgliteAuth__) {
    g.__pgliteAuth__ = (async () => {
      mkdirSync(pgliteDataPath, { recursive: true });
      const { PGlite } = await import("@electric-sql/pglite");
      const db = new PGlite(pgliteDataPath);
      await db.waitReady;
      return db;
    })();
  }
  return g.__pgliteAuth__;
}
