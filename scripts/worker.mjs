#!/usr/bin/env node
/**
 * Standalone background worker process entry point.
 *
 * Spec: TestingEngine_Deep_Diagnosis.md §3.1
 *       UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.7
 *
 * Run in production with a remote database (Neon / DATABASE_URL set):
 *
 *   npm run worker
 *
 * Local/preview mode (no DATABASE_URL) uses PGLite in-process inside the dev
 * server — the dev server already launches the live boot at boot, so this
 * CLI is intentionally a no-op there (running two PGLite instances on one
 * data dir is not safe).
 *
 * Requires running via the alias loader so `@/` resolves:
 *   node --experimental-strip-types --import ./scripts/paths-loader.mjs scripts/worker.mjs
 *
 * The single production entry point is `startLiveBoot` from
 * `src/lib/prediction/live/boot.ts`, which wires, in order:
 *   1. ColdStartSeeder — backfill crash_rounds if empty
 *   2. OutboxDispatcher — drain queued Telegram notifications
 *   3. EventDrivenPipeline — subscribe to BC.Game's Socket.IO bg/ed events
 *   4. PollWorker — REST safety net
 *   5. ClockSkewMonitor — periodic skew measurement
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

const liveBoot = await import("@/lib/prediction/live/boot");
const events = await import("@/lib/prediction/events/game-event-handlers");

const result = await liveBoot.startLiveBoot({
  startSubscriber: async () => {
    await events.startEventDrivenPipeline();
  },
});

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
});
process.on("unhandledRejection", (reason) => {
  console.error("[worker] unhandledRejection:", reason);
});

// Surface silent deaths — without these, an unhandled rejection after the
// boot log can exit the process with no further Railway output.
process.on("uncaughtException", (err) => {
  console.error("[worker] uncaughtException:", err?.stack ?? err);
});
process.on("unhandledRejection", (reason) => {
  console.error("[worker] unhandledRejection:", reason);
});

const shutdown = async () => {
  try {
    await events.stopEventDrivenPipeline();
  } catch {
    /* best effort */
  }
  try {
    await liveBoot.stopLiveBoot();
  } catch {
    /* best effort */
  }
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

void pathToFileURL(import.meta.url);
console.log("[worker] background prediction worker running (DATABASE_URL) — live pipeline booted");
