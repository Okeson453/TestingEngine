#!/usr/bin/env node
/**
 * Standalone background worker process entry point.
 *
 * Production: DATABASE_URL must be set (Neon / Postgres).
 * Ensures:
 *   - Single live boot
 *   - Graceful SIGTERM/SIGINT with pool.end() so PgBouncer slots free
 *   - Unrecoverable process-level exceptions terminate the worker (Phase 13)
 *     so the runtime restarts a clean process rather than leaving a
 *     potentially corrupted worker alive.
 */
import { pathToFileURL } from "node:url";
import vm from "node:vm";
import { registerProcessFailureHandlers } from "./worker-fatal.mjs";

// Fix plan Phase 3: the wr_utils sandbox is a STARTUP REQUIREMENT in
// production. Without --experimental-vm-modules the sign loader would fall
// back to a privileged dynamic import of downloaded third-party code —
// refused elsewhere, so fail the boot clearly here instead of starting a
// worker that cannot sign.
if (
  process.env.NODE_ENV === "production" &&
  typeof vm.SourceTextModule !== "function"
) {
  console.error(
    "[worker] FATAL: NODE_ENV=production requires node:vm.SourceTextModule (the wr_utils sandbox).",
  );
  console.error(
    "[worker] Start the worker with NODE_OPTIONS=--experimental-vm-modules — production fails closed rather than executing downloaded code with full Node privileges.",
  );
  process.exit(1);
}

if (!process.env.DATABASE_URL) {
  console.error("[worker] DATABASE_URL is not set (local PGLite mode).");
  console.error(
    "[worker] In this mode the background worker runs in-process within the dev server",
  );
  console.error("[worker] (`npm run dev`) — no standalone process is needed.");
  console.error("[worker] To run a separate worker process, set DATABASE_URL (Neon).");
  process.exit(0);
}

// Phase 13 (fix applied): process failure handlers MUST be installed before
// ANY boot step — the wr_utils crash fired from a setTimeout during boot,
// before the previous post-boot registration existed, producing a raw crash
// loop. endPool resolves lazily: the db module is only imported further down.
let endPgPoolLazy = null;
// Disposer kept out only for symmetry with worker-fatal.mjs's test hook; the
// worker process lives until SIGTERM, so the handlers are never removed.
const _offFailureHandlers = registerProcessFailureHandlers({
  endPool: () => endPgPoolLazy?.(),
});

// The live pipeline runs validation, prediction, feedback, outbox and polling
// concurrently. Match src/lib/db.ts's measured default (general 7 / critical 3
// at PG_POOL_MAX=10); an explicit operator value still wins.
if (!process.env.PG_POOL_MAX) {
  process.env.PG_POOL_MAX = "10";
}
if (!process.env.PG_APP_NAME) {
  process.env.PG_APP_NAME = "testingengine-worker";
}


/** Apply pending SQL migrations on a fresh DATABASE_URL (e.g. new Neon). */
async function ensureMigrations() {
  const { readdir, readFile } = await import("node:fs/promises");
  const { dirname, join } = await import("node:path");
  const { fileURLToPath } = await import("node:url");
  const pg = (await import("pg")).default;
  const { pendingMigrations } = await import("./migration-plan.mjs");

  const migrationsDir = join(dirname(fileURLToPath(import.meta.url)), "..", "migrations");
  let entries;
  try {
    entries = await readdir(migrationsDir);
  } catch {
    console.warn("[worker] no migrations/ directory — skip migrate");
    return;
  }

  const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL,
    max: 1,
    connectionTimeoutMillis: Number(process.env.PG_POOL_CONN_TIMEOUT_MS ?? 30_000) || 30_000,
    ssl:
      process.env.PG_SSL === "0"
        ? undefined
        : {
            rejectUnauthorized:
              process.env.PG_SSL_REJECT_UNAUTHORIZED === "1" ||
              process.env.PG_SSL_REJECT_UNAUTHORIZED === "true",
          },
    family: process.env.PG_FAMILY === "0" ? undefined : Number(process.env.PG_FAMILY ?? 4) || 4,
  });
  let client;
  let lastConnErr;
  for (let i = 1; i <= 5; i += 1) {
    try {
      client = await pool.connect();
      lastConnErr = null;
      break;
    } catch (e) {
      lastConnErr = e;
      const msg = String(e?.message ?? e);
      console.warn(`[worker] DB connect attempt ${i}/5 failed: ${msg}`);
      if (i < 5) await new Promise((r) => setTimeout(r, 2000 * i));
    }
  }
  if (!client) {
    await pool.end().catch(() => undefined);
    throw lastConnErr ?? new Error("DB connect failed");
  }
  try {
    await client.query(
      "CREATE TABLE IF NOT EXISTS _migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    );
    const applied = (await client.query("SELECT name FROM _migrations")).rows.map((r) => r.name);
    let count = 0;
    for (const { name } of pendingMigrations(entries, applied)) {
      const text = await readFile(join(migrationsDir, name), "utf8");
      try {
        await client.query("BEGIN");
        await client.query(text);
        await client.query("INSERT INTO _migrations (name) VALUES ($1)", [name]);
        await client.query("COMMIT");
      } catch (err) {
        try {
          await client.query("ROLLBACK");
        } catch {
          /* soft */
        }
        console.error(`[worker] migrate failed on ${name}:`, err?.message ?? err);
        throw err;
      }
      console.log(`[worker] applied migration ${name}`);
      count += 1;
    }
    console.log(
      count
        ? `[worker] migrations done — ${count} applied`
        : "[worker] migrations up to date",
    );
  } finally {
    client.release();
    await pool.end();
  }
}

const liveBoot = await import("@/lib/prediction/live/boot");
const events = await import("@/lib/prediction/events/game-event-handlers");
const db = await import("@/lib/db");
endPgPoolLazy = db.endPgPool;
const edgeHttp = await import("@/lib/prediction/live/edge-http");

let shuttingDown = false;

async function bootWithRetry(maxAttempts = 8) {
  let lastErr;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const result = await liveBoot.startLiveBoot({
        startSubscriber: async () => {
          await events.startEventDrivenPipeline();
        },
      });
      return result;
    } catch (err) {
      lastErr = err;
      const msg = String(err?.message ?? err);
      console.error(
        `[worker] boot attempt ${attempt}/${maxAttempts} failed: ${msg}`,
      );
      if (
        msg.includes("max_client_conn") ||
        msg.includes("too many clients") ||
        msg.includes("remaining connection slots")
      ) {
        console.error(
          "[worker] connection pool saturated — waiting before retry (release stale clients)",
        );
        try {
          await db.endPgPool();
        } catch {
          /* ignore */
        }
        await new Promise((r) => setTimeout(r, Math.min(5_000 * attempt, 20_000)));
        continue;
      }
      // Lock held — wait for previous instance TTL then retry (rolling deploy).
      if (msg.includes("Worker lock not acquired")) {
        const wait = Math.min(15_000 * attempt, 45_000);
        console.error(
          `[worker] lock held by another instance — waiting ${wait}ms before retry`,
        );
        await new Promise((r) => setTimeout(r, wait));
        continue;
      }
      await new Promise((r) => setTimeout(r, Math.min(2_000 * attempt, 10_000)));
    }
  }
  throw lastErr;
}

// New / empty Neon: create schema before schema validation in LiveBoot.
try {
  await ensureMigrations();
} catch (e) {
  console.error("[worker] ensureMigrations failed:", e?.message ?? e);
  process.exit(1);
}

const result = await bootWithRetry();

try {
  await edgeHttp.startEdgeHttpServer();
} catch (e) {
  console.error("[worker] edge HTTP server failed to start:", e?.message ?? e);
}

console.log(
  JSON.stringify({
    level: "info",
    time: new Date().toISOString(),
    component: "worker-entry",
    msg: "live prediction pipeline started",
    seed: result.seed,
  }),
);

/**
 * Phase 13 — Correct worker failure handling.
 * Handlers now live in worker-fatal.mjs and are registered at the VERY TOP
 * of this file (before migrations / boot), because the wr_utils crash fired
 * during boot before the previous post-boot registration existed.
 * See the `registerProcessFailureHandlers` call above the env setup.
 */

const shutdown = async (signal) => {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[worker] ${signal} received — graceful shutdown`);
  try {
    await edgeHttp.stopEdgeHttpServer();
  } catch (e) {
    console.error("[worker] stopEdgeHttpServer:", e?.message ?? e);
  }
  try {
    await events.stopEventDrivenPipeline();
  } catch (e) {
    console.error("[worker] stopEventDrivenPipeline:", e?.message ?? e);
  }
  try {
    await liveBoot.stopLiveBoot();
  } catch (e) {
    console.error("[worker] stopLiveBoot:", e?.message ?? e);
  }
  try {
    await db.endPgPool();
    console.log("[worker] pg pool closed");
  } catch (e) {
    console.error("[worker] endPgPool:", e?.message ?? e);
  }
  process.exit(0);
};
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

void pathToFileURL(import.meta.url);
console.log(
  "[worker] background prediction worker running (DATABASE_URL) — live pipeline booted",
);
