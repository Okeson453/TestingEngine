/**
 * Retention sweep — bounded cleanup for append-only observability tables.
 *
 * Scope (deliberate):
 *   - live_event_log ONLY. It is pure observability (migration 0011: "append-
 *     only observability"), receives ~4 rows per round (BG + PR + ED + PREDICT
 *     lifecycle rows, ~93 rounds/hour → ~10k rows/day) and had NO retention
 *     before this module. Unbounded jsonb-bearing growth degrades the
 *     received_at DESC index and every dashboard query over time.
 *
 *   - NEVER crash_rounds, pending_predictions, prediction_validations,
 *     notification_outbox, live_round_state: those are prediction/result
 *     history and the delivery audit trail. Deleting them is forbidden
 *     (spec: never delete required prediction/result history).
 *
 * Design:
 *   - Batched bounded DELETE (id IN (SELECT ... LIMIT batch)) — each batch is
 *     a single short autocommit statement on the GENERAL pool. The critical
 *     pool is never touched. A sweep between rounds costs one or two ~ms
 *     batches; the 6h cadence means sweeps are effectively always no-ops
 *     after the first backfill.
 *   - Hard per-sweep batch cap so even the initial backfill of a large table
 *     cannot hold a connection for long or starve the general pool.
 *   - Runs only on the authoritative worker (fencing-gated at boot wiring);
 *     the interval timer is unref'd so it never holds the process open.
 */
import { getSql, type Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("live-retention");

const DEFAULT_RETENTION_DAYS = 14;
const DEFAULT_BATCH_SIZE = 2_000;
const DEFAULT_MAX_BATCHES = 50; // 50 × 2000 = 100k rows max per sweep
const DEFAULT_INTERVAL_MS = 6 * 60 * 60_000;

export function getLiveEventLogRetentionDays(): number {
  const raw = Number(process.env.LIVE_EVENT_LOG_RETENTION_DAYS ?? DEFAULT_RETENTION_DAYS);
  return Number.isFinite(raw) && raw >= 1 ? Math.floor(raw) : DEFAULT_RETENTION_DAYS;
}

export interface RetentionSweepResult {
  table: string;
  deleted: number;
  batches: number;
  batchCapHit: boolean;
  durationMs: number;
  retentionDays: number;
}

/**
 * One sweep: delete rows older than the retention window in bounded batches.
 * Exported for tests (PGlite) — production callers use startRetentionSweep().
 */
export async function runLiveEventLogRetentionSweep(
  sql: Sql,
  opts: {
    retentionDays?: number;
    batchSize?: number;
    maxBatches?: number;
  } = {},
): Promise<RetentionSweepResult> {
  const retentionDays = opts.retentionDays ?? getLiveEventLogRetentionDays();
  const batchSize = Math.max(1, Math.min(10_000, opts.batchSize ?? DEFAULT_BATCH_SIZE));
  const maxBatches = Math.max(1, opts.maxBatches ?? DEFAULT_MAX_BATCHES);
  const t0 = performance.now();
  let deleted = 0;
  let batches = 0;
  let batchCapHit = false;

  while (batches < maxBatches) {
    const rows = await sql<{ id: number }>`
      DELETE FROM live_event_log
      WHERE id IN (
        SELECT id FROM live_event_log
        WHERE received_at < now() - (${retentionDays}::int * interval '1 day')
        LIMIT ${batchSize}
      )
      RETURNING id
    `;
    batches += 1;
    deleted += rows.length;
    if (rows.length < batchSize) break;
    if (batches === maxBatches) batchCapHit = true;
  }

  const durationMs = Math.round((performance.now() - t0) * 100) / 100;
  if (deleted > 0 || batchCapHit) {
    logger.info(
      {
        component: "live-retention",
        table: "live_event_log",
        deleted,
        batches,
        batchCapHit,
        durationMs,
        retentionDays,
      },
      batchCapHit
        ? "retention sweep hit batch cap — more rows remain for next sweep"
        : "retention sweep complete",
    );
  }
  return {
    table: "live_event_log",
    deleted,
    batches,
    batchCapHit,
    durationMs,
    retentionDays,
  };
}

let sweepTimer: ReturnType<typeof setInterval> | null = null;
let sweepInFlight = false;

function sweepIntervalMs(): number {
  const raw = Number(process.env.RETENTION_SWEEP_INTERVAL_MS ?? DEFAULT_INTERVAL_MS);
  return Number.isFinite(raw) && raw >= 60_000 ? raw : DEFAULT_INTERVAL_MS;
}

/**
 * Fire one sweep if none is running (never overlap; the general pool must
 * stay free between rounds). Errors are logged and swallowed — retention is
 * never allowed to affect the live pipeline.
 */
async function sweepOnceSafe(): Promise<void> {
  if (sweepInFlight) return;
  sweepInFlight = true;
  try {
    const sql = await getSql();
    await runLiveEventLogRetentionSweep(sql);
  } catch (e) {
    logger.warn(
      { component: "live-retention", error: String(e) },
      "retention sweep failed (soft) — will retry next interval",
    );
  } finally {
    sweepInFlight = false;
  }
}

/** Start the periodic sweep. First run is deferred so boot never waits on it. */
export function startRetentionSweep(): void {
  if (sweepTimer) return;
  const interval = sweepIntervalMs();
  sweepTimer = setInterval(() => void sweepOnceSafe(), interval);
  sweepTimer.unref?.();
  // Deferred initial pass: 10 minutes after boot, off the boot critical path.
  setTimeout(() => void sweepOnceSafe(), 10 * 60_000).unref?.();
  logger.info(
    {
      component: "live-retention",
      intervalMs: interval,
      retentionDays: getLiveEventLogRetentionDays(),
      table: "live_event_log",
    },
    "retention sweep scheduled",
  );
}

/** Stop the periodic sweep (authority lost / shutdown). */
export function stopRetentionSweep(): void {
  if (sweepTimer) {
    clearInterval(sweepTimer);
    sweepTimer = null;
  }
}

/** Test hook. */
export function resetRetentionSweepForTests(): void {
  stopRetentionSweep();
  sweepInFlight = false;
}
