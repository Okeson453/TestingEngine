import { mkdirSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { pendingMigrations } from "../../scripts/migration-plan.mjs";

/** Which database backend is active. */
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
  __pgliteInstance__?: Promise<import("@electric-sql/pglite").PGlite>;
  __pgliteMigrateChain__?: Promise<void>;
};

const OID_INT8 = 20;
const OID_DATE = 1082;
const OID_INTERVAL = 1186;
const identity = (v: string) => v;

type Run = <T>(text: string, params: unknown[]) => Promise<T[]>;

function toSql(run: Run): Sql {
  const sql = (async <T = Record<string, unknown>>(
    strings: TemplateStringsArray,
    ...values: unknown[]
  ): Promise<T[]> => {
    let text = strings[0];
    for (let i = 0; i < values.length; i += 1) text += `$${i + 1}${strings[i + 1]}`;
    return run<T>(text, values);
  }) as unknown as Sql;
  sql.query = <T = Record<string, unknown>>(text: string, params: unknown[] = []) =>
    run<T>(text, params);
  return sql;
}

function createNeonSql(): Promise<Sql> {
  globalRef.__pgSqlPromise__ ??= (async () => {
    const { Pool, types } = await import("pg");
    types.setTypeParser(OID_INT8, Number);
    types.setTypeParser(OID_DATE, identity);
    types.setTypeParser(OID_INTERVAL, identity);
    // Pool size is env-configurable. Default 8 for production headroom;
    // set PG_POOL_MAX=3 on tight poolers (e.g. small pgBouncer / Layerbase).
    const poolMax = Math.max(1, Number(process.env.PG_POOL_MAX ?? 8) || 8);
    const poolMin = Math.max(0, Number(process.env.PG_POOL_MIN ?? 1) || 1);
    const idleTimeoutMillis = Number(process.env.PG_POOL_IDLE_MS ?? 10000) || 10000;
    // Neon / managed Postgres often needs >5s on cold start from Railway.
    const connectionTimeoutMillis =
      Number(process.env.PG_POOL_CONN_TIMEOUT_MS ?? 30_000) || 30_000;
    const pool = new Pool({
      connectionString: databaseUrl,
      max: poolMax,
      min: Math.min(poolMin, poolMax),
      idleTimeoutMillis,
      connectionTimeoutMillis,
      allowExitOnIdle: true,
      keepAlive: true,
      keepAliveInitialDelayMillis: 10_000,
    });
    // eslint-disable-next-line no-console
    console.log(`[db] Pool configured max=${poolMax} min=${Math.min(poolMin, poolMax)} idleMs=${idleTimeoutMillis}`);

    pool.on("error", (err: Error) => {
      // eslint-disable-next-line no-console
      console.error("[db] Pool connection error (non-fatal):", err.message);
    });

    return toSql(async <T>(text: string, params: unknown[]) => {
      const res = await pool.query(text, params);
      return res.rows as T[];
    });
  })().catch((err) => {
    globalRef.__pgSqlPromise__ = undefined;
    throw err;
  });
  return globalRef.__pgSqlPromise__;
}

async function createPgliteSql(): Promise<Sql> {
  globalRef.__pgliteInstance__ ??= (async () => {
    const { PGlite } = await import("@electric-sql/pglite");
    mkdirSync(pgliteDataPath, { recursive: true });
    const pg = new PGlite({
      dataDir: pgliteDataPath,
      parsers: {
        [OID_INT8]: Number,
        [OID_DATE]: identity,
        [OID_INTERVAL]: identity,
      },
    });
    await pg.waitReady;
    await pg.exec(
      "create table if not exists _migrations (name text primary key, applied_at timestamptz not null default now())",
    );
    return pg;
  })().catch((err) => {
    globalRef.__pgliteInstance__ = undefined;
    throw err;
  });
  const pg = await globalRef.__pgliteInstance__;

  const migrate = async (): Promise<void> => {
    let migrations: Record<string, string>;
    try {
      migrations = import.meta.glob("/migrations/*.sql", {
        query: "?raw",
        import: "default",
        eager: true,
      }) as Record<string, string>;
    } catch {
      const { pendingMigrations: pending } = await import(
        "../../scripts/migration-plan.mjs",
      );
      const dirs = [join(process.cwd(), "migrations")];
      const entries: string[] = [];
      for (const dir of dirs) {
        try {
          for (const name of readdirSync(dir)) {
            if (name.endsWith(".sql")) entries.push(name);
          }
        } catch {
          /* no migrations dir */
        }
      }
      migrations = {};
      for (const name of entries) {
        migrations[name] = readFileSync(join(dirs[0], name), "utf8");
      }
    }
    const doneRows = await pg.query<{ name: string }>(
      "select name from _migrations",
    );
    const done = doneRows.rows.map((r) => r.name);
    for (const { name, path } of pendingMigrations(Object.keys(migrations), done)) {
      await pg.transaction(async (tx) => {
        await tx.exec(migrations[path]);
        await tx.query("insert into _migrations (name) values ($1)", [name]);
      });
    }
  };
  const pass = (globalRef.__pgliteMigrateChain__ ?? Promise.resolve())
    .catch(() => undefined)
    .then(migrate);
  globalRef.__pgliteMigrateChain__ = pass;
  await pass;

  return toSql(async <T>(text: string, params: unknown[]) => {
    const result = await pg.query<T>(text, params);
    return result.rows;
  });
}

let sqlPromise: Promise<Sql> | null = null;

async function createSql(): Promise<Sql> {
  if (typeof window !== "undefined") {
    throw new Error(
      "@/lib/db is server-only — call getSql() from a createServerFn handler " +
        "or a server route loader, never from client code.",
    );
  }
  return dbSource === "neon" ? createNeonSql() : createPgliteSql();
}

export function getSql(): Promise<Sql> {
  sqlPromise ??= createSql().catch((err) => {
    sqlPromise = null;
    throw err;
  });
  return sqlPromise;
}

export async function getPglite(): Promise<import("@electric-sql/pglite").PGlite> {
  if (dbSource !== "pglite") {
    throw new Error("getPglite() is only available on the PGLite fallback (no DATABASE_URL)");
  }
  await getSql();
  const pg = await globalRef.__pgliteInstance__;
  if (!pg) throw new Error("PGLite instance failed to initialize");
  return pg;
}

export function ensureDbReady(): Promise<void> {
  if (dbSource !== "pglite") return Promise.resolve();
  return getSql().then(() => undefined);
}

const globalBoot = globalThis as typeof globalThis & {
  __pgBootstrapPromise__?: Promise<void>;
};
if (typeof window === "undefined" && dbSource === "pglite") {
  globalBoot.__pgBootstrapPromise__ ??= ensureDbReady().catch((err) => {
    globalBoot.__pgBootstrapPromise__ = undefined;
    console.error("[db] PGLite bootstrap failed:", err);
    throw err;
  });
}
