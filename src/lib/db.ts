/**
 * Database client — Neon Postgres (production) / PGLite (local).
 * Restored Neon/PGLite database client.
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
  __pgPool__?: import("pg").Pool;
  __pgliteInstance__?: Promise<import("@electric-sql/pglite").PGlite>;
  __pgPoolEnding__?: Promise<void>;
  __poolExhaustionAlerted__?: boolean;
};

const OID_INT8 = 20;
const OID_DATE = 1082;
const identity = (v: string) => v;

type Run = <T>(text: string, params: unknown[]) => Promise<T[]>;

function toSql(run: Run): Sql {
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
  }) as Sql;
  sql.query = async <T = Record<string, unknown>>(text: string, params?: unknown[]) =>
    run<T>(text, params ?? []);
  return sql;
}

function readPoolMax(): number {
  const raw = Number(process.env.PG_POOL_MAX ?? 8);
  return Math.max(1, Math.min(Number.isFinite(raw) ? raw : 8, 12));
}

function readPoolMin(): number {
  const raw = Number(process.env.PG_POOL_MIN ?? 1);
  return Math.max(0, Math.min(Number.isFinite(raw) ? raw : 1, readPoolMax()));
}

async function createNeonSql(): Promise<Sql> {
  const { Pool, types } = await import("pg");
  types.setTypeParser(OID_INT8, identity);
  types.setTypeParser(OID_DATE, identity);

  const poolMax = readPoolMax();
  const poolMin = readPoolMin();
  const idleTimeoutMillis = Number(process.env.PG_IDLE_TIMEOUT_MS ?? 15_000) || 15_000;
  const connectionTimeoutMillis =
    Number(process.env.PG_POOL_CONN_TIMEOUT_MS ?? process.env.PG_CONN_TIMEOUT_MS ?? 30_000) ||
    30_000;

  console.log(
    `[db] Pool configured max=${poolMax} min=${poolMin} idleMs=${idleTimeoutMillis} connTimeoutMs=${connectionTimeoutMillis}`,
  );

  const pool = new Pool({
    connectionString: databaseUrl,
    max: poolMax,
    min: poolMin,
    idleTimeoutMillis,
    connectionTimeoutMillis,
    ssl:
      process.env.PG_SSL === "0"
        ? false
        : {
            rejectUnauthorized:
              process.env.PG_SSL_REJECT_UNAUTHORIZED === "1" ||
              process.env.PG_SSL_REJECT_UNAUTHORIZED === "true",
          },
    family: process.env.PG_FAMILY === "0" ? undefined : Number(process.env.PG_FAMILY ?? 4) || 4,
  });
  globalRef.__pgPool__ = pool;

  pool.on("error", (err) => {
    console.error("[db] pool error:", err.message);
  });

  const monitor = setInterval(() => {
    if (pool.totalCount >= poolMax && pool.idleCount === 0) {
      if (!globalRef.__poolExhaustionAlerted__) {
        globalRef.__poolExhaustionAlerted__ = true;
        console.error(
          `[db] POOL EXHAUSTION ALERT: utilization=1.00 total=${pool.totalCount} idle=${pool.idleCount} waiting=${pool.waitingCount} max=${poolMax}`,
        );
      }
    } else {
      globalRef.__poolExhaustionAlerted__ = false;
    }
  }, 5_000);
  monitor.unref?.();

  const run: Run = async <T>(text: string, params: unknown[]) => {
    const client = await pool.connect();
    try {
      const res = await client.query(text, params);
      return res.rows as T[];
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes("timeout exceeded when trying to connect") || msg.includes("remaining connection slots")) {
        console.error(
          `[db] POOL EXHAUSTION: ${msg} | pool total=${pool.totalCount} idle=${pool.idleCount} waiting=${pool.waitingCount}`,
        );
      }
      throw err;
    } finally {
      client.release();
    }
  };

  return toSql(run);
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
    globalRef.__pgSqlPromise__ = databaseUrl ? createNeonSql() : createPgliteSql();
  }
  return globalRef.__pgSqlPromise__;
}

export async function endPgPool(): Promise<void> {
  if (globalRef.__pgPoolEnding__) return globalRef.__pgPoolEnding__;
  globalRef.__pgPoolEnding__ = (async () => {
    const pool = globalRef.__pgPool__;
    globalRef.__pgPool__ = undefined;
    globalRef.__pgSqlPromise__ = undefined;
    if (pool) {
      try {
        await pool.end();
      } catch {
        /* soft */
      }
    }
  })();
  return globalRef.__pgPoolEnding__;
}

export function getPgPool(): import("pg").Pool | null {
  return globalRef.__pgPool__ ?? null;
}

export async function withPinnedClient<T>(
  fn: (client: import("pg").PoolClient) => Promise<T>,
): Promise<T> {
  const pool = getPgPool();
  if (!pool) {
    throw new Error("withPinnedClient requires Postgres pool (not PGLite)");
  }
  const client = await pool.connect();
  try {
    return await fn(client);
  } finally {
    client.release();
  }
}

export interface PoolStats {
  totalCount: number;
  idleCount: number;
  waitingCount: number;
  max: number;
}

export function getPoolStats(): PoolStats | null {
  const pool = globalRef.__pgPool__;
  if (!pool) return null;
  return {
    totalCount: pool.totalCount,
    idleCount: pool.idleCount,
    waitingCount: pool.waitingCount,
    max: readPoolMax(),
  };
}
