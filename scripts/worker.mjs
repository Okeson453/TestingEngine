#!/usr/bin/env node
/**
 * Standalone background worker process for the prediction/validation engine.
 *
 * Run in production with a remote database (Neon / DATABASE_URL set):
 *
 *   npm run worker
 *
 * The worker owns the full lifecycle server-side:
 *   BC.Game polling -> new-round detection -> prediction generation ->
 *   WIN/LOSS validation -> permanent DB persistence -> daily entry counting.
 *
 * It is fully independent of the dashboard/browser (no React effects, no
 * client timers, no refreshDashboard dependency). A DB-backed distributed lock
 * guarantees only one instance ever generates/validates at a time, and all
 * mutations are idempotent so a restart never duplicates records.
 *
 * Local/preview mode (no DATABASE_URL) uses PGLite in-process inside the dev
 * server — the dev server already launches the worker at boot, so this CLI is
 * intentionally a no-op there (running two PGLite instances on one data dir
 * is not safe).
 *
 * Requires running via the alias loader so `@/` resolves:
 *   node --experimental-strip-types --import ./scripts/paths-loader.mjs scripts/worker.mjs
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

const mod = await import("@/lib/prediction/worker");
const { startWorker, stopWorker } = mod;

startWorker();

const shutdown = async () => {
  try {
    await stopWorker();
  } catch {
    /* best effort */
  }
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// Flush the import so relative path errors surface immediately.
void pathToFileURL(import.meta.url);
console.log("[worker] background prediction worker running (DATABASE_URL)");
