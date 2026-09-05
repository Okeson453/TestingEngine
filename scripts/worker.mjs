#!/usr/bin/env node
/**
 * Standalone background worker process entry point.
 *
 * Production: DATABASE_URL must be set (Neon / Postgres).
 * Ensures:
 *   - Single live boot
 *   - Graceful SIGTERM/SIGINT with pool.end() so PgBouncer slots free
 *   - Uncaught errors are logged but do not leave abandoned DB clients
 */
import { pathToFileURL } from "node:url";

if (!process.env.DATABASE_URL) {
  console.error("[worker] DATABASE_URL is not set (local PGLite mode).");
  console.error(
    "[worker] In this mode the background worker runs in-process within the dev server",
  );
  console.error("[worker] (`npm run dev`) — no standalone process is needed.");
  console.error("[worker] To run a separate worker process, set DATABASE_URL (Neon).");
  process.exit(0);
}

// Prefer a small pool on the worker process unless operator overrides.
if (!process.env.PG_POOL_MAX) {
  process.env.PG_POOL_MAX = "3";
}
if (!process.env.PG_POOL_MIN) {
  process.env.PG_POOL_MIN = "1";
}
if (!process.env.PG_APP_NAME) {
  process.env.PG_APP_NAME = "testingengine-worker";
}

const liveBoot = await import("@/lib/prediction/live/boot");
const events = await import("@/lib/prediction/events/game-event-handlers");
const db = await import("@/lib/db");

let shuttingDown = false;

async function bootWithRetry(maxAttempts = 5) {
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
      // Lock held by another instance — exit so Railway does not thrash.
      if (msg.includes("Worker lock not acquired")) {
        throw err;
      }
      await new Promise((r) => setTimeout(r, Math.min(2_000 * attempt, 10_000)));
    }
  }
  throw lastErr;
}

const result = await bootWithRetry();

console.log(
  JSON.stringify({
    level: "info",
    time: new Date().toISOString(),
    component: "worker-entry",
    msg: "live prediction pipeline started",
    seed: result.seed,
  }),
);

process.on("uncaughtException", (err) => {
  console.error("[worker] uncaughtException:", err?.stack ?? err);
  // Do not exit immediately — let Railway restart policy decide after SIGTERM
  // from the platform; exiting here can race with in-flight pool clients.
});
process.on("unhandledRejection", (reason) => {
  console.error("[worker] unhandledRejection:", reason);
});

const shutdown = async (signal) => {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[worker] ${signal} received — graceful shutdown`);
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
