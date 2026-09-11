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
import {
  maxLoopLagBetween,
  startEventLoopLagSampler,
} from "@/lib/observability/event-loop-lag";

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

/** Pool sizing readers — exported for regression tests (pure env math). */
export function readTotalMax(): number {
  // sep 11 18:20 pass: 10 -> 12. Production pinned the GENERAL pool at its
  // max (total=7) with ~1.0-1.16s acquires — real concurrent demand from
  // sweeps + heartbeats + event-log writes. 12 keeps critical at 4 and
  // gives the general pool 8 (burst headroom without Neon pressure).
  const raw = Number(process.env.PG_POOL_MAX ?? 12);
  return Math.max(2, Math.min(Number.isFinite(raw) ? raw : 12, 12));
}

export function readCriticalMax(): number {
  const total = readTotalMax();
  // sep 11 pass: 3 -> 4. Critical consumers per round boundary: BG reconcile
  // CTE (now routed here) + N+1 persist + dispatcher claim/auth/finalize can
  // legally overlap 3-wide; a 4th slot removes the tail wait (one 1065ms
  // critical blip observed at 14:50:29 during SIGNAL_READY persist).
  const raw = Number(process.env.PG_CRITICAL_POOL_MAX ?? 4);
  const crit = Math.max(1, Math.min(Number.isFinite(raw) ? raw : 4, total - 1));
  return crit;
}

export function readGeneralMax(): number {
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
      const q0Perf = performance.now();
      const res = await client.query(text, params);
      const queryMs = Date.now() - q0;
      if (queryMs > 500) {
        // FORENSIC ATTRIBUTION (sep 11 19:05-19:12 logs): queryMs alone
        // cannot distinguish a Neon/network RTT spike from an event-loop
        // stall — both inflate every query without touching pool acquires
        // (the window's signature: SELECT 1 at 595ms, zero acquire warns).
        // loop_lag_ms = max event-loop lag sampled during this query's
        // wall-clock window. loop_lag ≈ queryMs ⇒ application-side stall;
        // loop_lag ≈ 0 ⇒ network/Neon execution.
        const loopLag = Math.round(maxLoopLagBetween(q0Perf, performance.now()));
        console.warn(`[db] ${label} slow_query_ms=${queryMs} loop_lag_ms=${loopLag} text=${text.slice(0, 80)}`);
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
  // LATENCY FIX: crash rounds are spaced 10–70s. A 15s idle timeout drops the
  // critical Neon connection between rounds; the next prediction/dispatch then
  // pays ~700–1500ms TLS+auth. Keep critical clients warm across inter-round gaps.
  // CHURN FIX (sep 11 15:58 logs): the GENERAL pool still churned — min=1 with
  // a 60s idle timeout closed connections between bursts, and every burst above
  // min re-paid ~1.0-1.1s of Neon TLS+auth on the hot path (observed general
  // pool_acquire_ms=1059/1128 at boot and ~1159ms mid-stream). min=2 + 300s
  // idle keeps enough clients warm that steady-state acquires are local (~0ms).
  // This is churn elimination, not pool-size increase: max stays 5.
  const generalIdleTimeoutMillis =
    Number(process.env.PG_IDLE_TIMEOUT_MS ?? 300_000) || 300_000;
  const criticalIdleTimeoutMillis =
    Number(process.env.PG_CRITICAL_IDLE_TIMEOUT_MS ?? 180_000) || 180_000;
  // Critical path: short acquire timeout (do not sit 30s behind dashboard)
  const criticalConnTimeout =
    (Number(process.env.PG_CRITICAL_CONN_TIMEOUT_MS ?? 5_000) || 5_000);
  // General / dashboard: bounded, still shorter than legacy 30s default
  const generalConnTimeout =
    (Number(process.env.PG_CONN_TIMEOUT_MS ?? 8_000) || 8_000);
  const dashboardConnTimeout =
    (Number(process.env.PG_DASHBOARD_CONN_TIMEOUT_MS ?? 3_000) || 3_000);
  // Keep ≥2 critical clients: persist + dispatch can overlap on ED.
  const criticalMin = Math.min(
    Math.max(1, Number(process.env.PG_CRITICAL_POOL_MIN ?? 2) || 2),
    criticalMax,
  );
  const generalMin = Math.min(
    // sep 11 pass: 2 -> 3. Steady-state general concurrency is heartbeat (1)
    // + at least one periodic sweep; a 3rd warm client avoids re-paying the
    // ~1s Neon TLS+auth on the first burst above min.
    Math.max(0, Number(process.env.PG_POOL_MIN_IDLE ?? 3) || 3),
    generalMax,
  );

  console.log(
    `[db] Dual pool: critical max=${criticalMax} min=${criticalMin} idleTimeoutMs=${criticalIdleTimeoutMillis} connTimeoutMs=${criticalConnTimeout}; general max=${generalMax} min=${generalMin} idleTimeoutMs=${generalIdleTimeoutMillis} connTimeoutMs=${generalConnTimeout}; dashboard acquire budget=${dashboardConnTimeout}`,
  );

  const criticalPool = new Pool({
    connectionString: databaseUrl,
    max: criticalMax,
    // WARM-POOL FIX: min≥2 + long idle timeout so Neon TLS is not re-paid on
    // every ED. Cold acquire was the dominant 0.7–1.5s leg on both persist
    // and outbox dispatch (warm path is ~1–5ms acquire).
    min: criticalMin,
    idleTimeoutMillis: criticalIdleTimeoutMillis,
    connectionTimeoutMillis: criticalConnTimeout,
    allowExitOnIdle: false,
    keepAlive: true,
    keepAliveInitialDelayMillis: 10_000,
    ssl: process.env.PG_SSL === "0" ? false : { rejectUnauthorized: false },
  });
  const generalPool = new Pool({
    connectionString: databaseUrl,
    max: generalMax,
    min: generalMin,
    idleTimeoutMillis: generalIdleTimeoutMillis,
    connectionTimeoutMillis: generalConnTimeout,
    // Match critical: never let node-pg drop idle clients while Neon is still
    // reachable — mid-stream pool_acquire_ms≈1.1s was TLS+auth after idle exit.
    allowExitOnIdle: false,
    keepAlive: true,
    keepAliveInitialDelayMillis: 10_000,
    ssl: process.env.PG_SSL === "0" ? false : { rejectUnauthorized: false },
  });

  globalRef.__pgCriticalPool__ = criticalPool;
  globalRef.__pgPool__ = generalPool; // getPgPool / dashboard pin = general

  // PREWARM + KEEPALIVE (sep 11 latency): node-pg min creates sockets, but
  // Neon still closes idle TCP from the server side. Observed: idleCount≥1
  // yet pool_acquire_ms≈1059–1159 because the "idle" client was half-open and
  // connect() rebuilt TLS. Force min clients through SELECT 1 at boot, then
  // ping both pools every 25s so server-side idle kill never lands on the
  // prediction/dispatch hot path.
  void (async () => {
    const warm = async (pool: import("pg").Pool, label: string, n: number) => {
      const clients: import("pg").PoolClient[] = [];
      try {
        for (let i = 0; i < n; i++) {
          const c = await pool.connect();
          await c.query("select 1");
          clients.push(c);
        }
        console.log(`[db] ${label} prewarmed clients=${clients.length}`);
      } catch (e) {
        console.warn(`[db] ${label} prewarm failed: ${e instanceof Error ? e.message : String(e)}`);
      } finally {
        for (const c of clients) {
          try { c.release(); } catch { /* soft */ }
        }
      }
    };
    await Promise.all([
      warm(criticalPool, "critical", criticalMin),
      warm(generalPool, "general", Math.max(1, generalMin)),
    ]);
  })();

  const keepAlive = setInterval(() => {
    for (const [label, pool] of [
      ["critical", criticalPool],
      ["general", generalPool],
    ] as const) {
      void pool
        .query("select 1")
        .catch((e) =>
          console.warn(
            `[db] ${label} keepalive failed: ${e instanceof Error ? e.message : String(e)}`,
          ),
        );
    }
  }, 25_000);
  keepAlive.unref?.();

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

  // Forensic attribution (sep 11 19:05-19:12): slow-query lines carry
  // loop_lag_ms, which requires the sampler to be running.
  startEventLoopLagSampler();

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

/** Pin a critical-pool client for multi-statement prediction dispatch (claim + auth + finalize)
 * so one acquire covers the whole SIGNAL_READY→Telegram leg instead of 3× cold connects. */
export async function withCriticalPinnedClient<T>(
  fn: (client: import("pg").PoolClient) => Promise<T>,
): Promise<T> {
  await getSql(); // ensure pools
  const pool = getCriticalPool();
  if (!pool) {
    // PGLite / tests: fall through to general pin
    return withPinnedClient(fn);
  }
  const t0 = Date.now();
  const client = await pool.connect();
  globalRef.__lastPoolAcquireMs__ = Date.now() - t0;
  if (globalRef.__lastPoolAcquireMs__ > 100) {
    console.warn(
      `[db] critical pin acquire_ms=${globalRef.__lastPoolAcquireMs__} total=${pool.totalCount} idle=${pool.idleCount} waiting=${pool.waitingCount}`,
    );
  }
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
