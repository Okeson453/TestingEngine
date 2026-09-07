#!/usr/bin/env node
/**
 * TestingEngine — live prediction latency benchmark.
 *
 * Reproduces the numbers in LATENCY_REPORT.md §5 against a local PGLite
 * instance (no DATABASE_URL required). Measures two critical-path metrics
 * after the Phase 1-3 + fire-and-forget fixes:
 *
 *   1. onGameEnd sync path   — time from ed event to `onGameEnd` resolving.
 *                               The N+1 prediction is now backgrounded, so
 *                               this is just validate + match + outbox.
 *   2. signal release        — time from ed event to the N+1 prediction row
 *                               AND its notification_outbox row being durable.
 *
 * Run:
 *   PATH=/opt/node22/bin:$PATH node --experimental-strip-types \
 *     --import ./scripts/paths-loader.mjs scripts/benchmark-latency.mjs
 */
import { performance } from "node:perf_hooks";

import { getSql } from "@/lib/db";
import { insertNewRounds } from "@/lib/crash/ingest";
import { onGameStart } from "@/lib/prediction/live/predictor";
import {
  onGameEnd,
  waitForInFlightPredictions,
} from "@/lib/prediction/live/validator";

const ITERATIONS = Number(process.env.BENCH_ITER ?? 20);
const REPEATS = Number(process.env.BENCH_REPEATS ?? 3);

async function ts(sql, offsetMs) {
  const rows = await sql`select (now() + (${offsetMs}::int * interval '1 millisecond'))::text as n`;
  return String(rows[0].n);
}

async function seedHistory(sql, refMs, n = 60) {
  const seeds = [];
  for (let i = 0; i < n; i += 1) {
    const crashedAt = new Date(refMs - (i + 1) * 4_000);
    seeds.push({
      gameId: String(800_000 + i),
      multiplier: 1 + (i % 9) * 0.21,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  await insertNewRounds(seeds);
}

async function resetDb(sql) {
  await sql`truncate pending_predictions, prediction_validations, notification_outbox, live_event_log, crash_rounds restart identity cascade`;
}

function percentile(sorted, p) {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[idx];
}

async function main() {
  const sql = await getSql();

  for (let repeat = 1; repeat <= REPEATS; repeat += 1) {
    const syncTimes = [];
    const signalTimes = [];

    for (let i = 0; i < ITERATIONS; i += 1) {
      await resetDb(sql);
      const dbNow = await sql`select now()::text as n`;
      const refMs = new Date(String(dbNow[0].n)).getTime();
      await seedHistory(sql, refMs);

      const targetGameId = String(900_000 + i);
      const targetBeganAt = await ts(sql, -100);
      const bg = await onGameStart({
        gameId: targetGameId,
        beginTime: targetBeganAt,
        hash: null,
        salt: null,
        sourceRoundGameId: String(899_999),
        receivedAt: await ts(sql, 0),
      });
      if (bg.kind !== "predicted") continue;

      const edStart = performance.now();
      const ed = await onGameEnd({
        gameId: targetGameId,
        endTime: await ts(sql, 2_900),
        multiplier: 2.5,
        receivedAt: await ts(sql, 3_000),
      });
      const syncMs = performance.now() - edStart;
      syncTimes.push(syncMs);

      // Signal release = validate committed + background N+1 prediction
      // (pending_predictions + notification_outbox) durable.
      await waitForInFlightPredictions();
      const signalMs = performance.now() - edStart;
      signalTimes.push(signalMs);

      if (ed.kind !== "resolved") {
        console.error(`iter ${i}: unexpected ed kind ${ed.kind}`);
      }
    }

    syncTimes.sort((a, b) => a - b);
    signalTimes.sort((a, b) => a - b);
    console.log(
      `Run ${repeat}: sync ${percentile(syncTimes, 50).toFixed(1)}ms p50 / ` +
        `${percentile(syncTimes, 95).toFixed(1)}ms p95 | ` +
        `signal ${percentile(signalTimes, 50).toFixed(1)}ms p50 / ` +
        `${percentile(signalTimes, 95).toFixed(1)}ms p95 ` +
        `(n=${syncTimes.length})`,
    );
  }

  process.exit(0);
}

main().catch((e) => {
  console.error("benchmark failed:", e);
  process.exit(1);
});
