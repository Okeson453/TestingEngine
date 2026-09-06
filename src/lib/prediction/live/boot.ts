/**
 * Boot orchestration for the live prediction pipeline.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.7
 *
 * Wires, in order:
 *   1. `ColdStartSeeder.runColdStartSeeder` — backfill crash_rounds if empty
 *   2. `OutboxDispatcher.start()` — drain queued Telegram notifications
 *   3. `LiveEventSubscriber` (via `events/game-event-handlers.startEventDrivenPipeline`)
 *      — subscribe to BC.Game's Socket.IO `bg`/`ed` events
 *   4. `PollWorker.start()` — REST safety net
 *   5. `ClockSkewMonitor.start()` — periodic skew measurement
 *
 * The boot is a single process. All four logical roles are independent
 * timers, so a failure in one does not take down the others.
 */
import { runColdStartSeeder, type SeedResult } from "./cold-start-seeder";
import { OutboxDispatcher } from "./notification-worker";
import { PollWorker } from "./poll-worker";
import { ClockSkewMonitor } from "./clock-skew-monitor";
import { getLogger } from "@/lib/observability/logger";
import { getSql, type Sql } from "@/lib/db";
import { loadAcieStateFromDb } from "@/lib/prediction/acie/state-persistence";

const logger = getLogger("live-boot");

/** Event-loop lag probe (timing diagnosis 3.8 / 7.6). */
let eventLoopProbeTimer: ReturnType<typeof setInterval> | null = null;
function startEventLoopLagMonitor(): void {
  if (eventLoopProbeTimer) return;
  eventLoopProbeTimer = setInterval(() => {
    const start = process.hrtime.bigint();
    setImmediate(() => {
      const lagMs = Number(process.hrtime.bigint() - start) / 1_000_000;
      if (lagMs > 50) {
        logger.warn({ eventLoopLagMs: Math.round(lagMs) }, "Event loop lag detected");
      }
      try {
        (globalThis as { __eventLoopLagMs__?: number }).__eventLoopLagMs__ = lagMs;
      } catch { /* ignore */ }
    });
  }, 2_000);
  if (typeof eventLoopProbeTimer.unref === "function") eventLoopProbeTimer.unref();
}

/** Keep one connection warm so first critical-path query avoids cold connect. */
let connectionWarmerTimer: ReturnType<typeof setInterval> | null = null;
function startConnectionWarmer(getSqlFn: () => Promise<Sql>): void {
  if (connectionWarmerTimer) return;
  connectionWarmerTimer = setInterval(() => {
    void getSqlFn()
      .then((sql) => sql`SELECT 1`)
      .catch(() => undefined);
  }, 3_000);
  if (typeof connectionWarmerTimer.unref === "function") connectionWarmerTimer.unref();
}

function stopConnectionWarmer(): void {
  if (connectionWarmerTimer) {
    clearInterval(connectionWarmerTimer);
    connectionWarmerTimer = null;
  }
}


/** Distributed single-writer lock (P0). Uses worker_locks table from 0006. */
const WORKER_ID =
  process.env.RAILWAY_REPLICA_ID ||
  process.env.WORKER_ID ||
  `worker-${process.pid}-${Math.random().toString(36).slice(2, 10)}`;
const LOCK_KEY = "prediction_worker";
const LOCK_TTL_SECONDS = 30;
let lockHeartbeatTimer: ReturnType<typeof setInterval> | null = null;

async function acquireWorkerLock(sql: Sql): Promise<boolean> {
  const rows = await sql<{ owner_id: string }>`
    INSERT INTO worker_locks (lock_key, owner_id, acquired_at, expires_at, heartbeat_at)
    VALUES (
      ${LOCK_KEY},
      ${WORKER_ID},
      now(),
      now() + (${LOCK_TTL_SECONDS}::int * interval '1 second'),
      now()
    )
    ON CONFLICT (lock_key) DO UPDATE
    SET owner_id = EXCLUDED.owner_id,
        acquired_at = EXCLUDED.acquired_at,
        expires_at = EXCLUDED.expires_at,
        heartbeat_at = EXCLUDED.heartbeat_at
    WHERE worker_locks.expires_at < now()
       OR worker_locks.owner_id = ${WORKER_ID}
    RETURNING owner_id
  `;
  return rows[0]?.owner_id === WORKER_ID;
}

async function heartbeatWorkerLock(sql: Sql): Promise<void> {
  await sql`
    UPDATE worker_locks
    SET heartbeat_at = now(),
        expires_at = now() + (${LOCK_TTL_SECONDS}::int * interval '1 second')
    WHERE lock_key = ${LOCK_KEY} AND owner_id = ${WORKER_ID}
  `;
}

// P2.11: Persist Incremental State
// Save incremental state alongside worker health
async function persistIncrementalState(sql: Sql): Promise<void> {
  try {
    const { globalIncrementalState } = await import(
      "@/lib/prediction/state/incremental-state-engine"
    );
    const snap = globalIncrementalState.snapshot();
    const stateJson = JSON.stringify({
      count: snap.count,
      ewma: snap.ewma,
      ewmaHit13: snap.ewmaHit13,
      welford: snap.welford,
      runs: snap.runs,
      timestamp: new Date().toISOString(),
    });
    await sql`
      INSERT INTO worker_state (key, value, updated_at)
      VALUES ('incremental_state', ${stateJson}, now())
      ON CONFLICT (key) DO UPDATE
      SET value = EXCLUDED.value, updated_at = now()
    `;
  } catch (e) {
    logger.debug(
      { component: "live-boot", error: String(e) },
      "incremental state persistence failed (soft)",
    );
  }
}

/**
 * Phase 4 / 12 — Restore adaptive incremental state from real Crash observations.
 * Never fabricate crash points (no Array(n).fill(1.5)).
 * Prefer recent multipliers from crash_rounds; fall back to cold-start if insufficient.
 */
async function restoreIncrementalState(sql: Sql): Promise<void> {
  try {
    const { globalIncrementalState } = await import(
      "@/lib/prediction/state/incremental-state-engine"
    );
    // Authoritative history: last N real crash multipliers (chronological)
    const history = await sql<{ multiplier: string | number }>`
      SELECT multiplier
      FROM crash_rounds
      WHERE crashed_at IS NOT NULL
        AND multiplier IS NOT NULL
      ORDER BY crashed_at DESC, game_id DESC
      LIMIT 500
    `;
    const points = history
      .map((r) => Number(r.multiplier))
      .filter((n) => Number.isFinite(n) && n > 0)
      .reverse();

    if (points.length >= 5) {
      globalIncrementalState.seed(points);
      logger.info(
        {
          component: "live-boot",
          count: points.length,
          source: "crash_rounds",
        },
        "Incremental state restored from real crash_rounds history",
      );
      return;
    }

    // Insufficient history — controlled cold start (do not fabricate values)
    logger.warn(
      {
        component: "live-boot",
        available: points.length,
        reason: "insufficient_history",
      },
      "Incremental state cold-start: insufficient crash_rounds history (no fabricated points)",
    );
  } catch (e) {
    logger.debug(
      { component: "live-boot", error: String(e) },
      "incremental state restore failed (soft) — continuing cold",
    );
  }
}

async function writeWorkerHealth(sql: Sql, cycle: number): Promise<void> {
  let poolInfo = "";
  try {
    const { getPoolStats } = await import("@/lib/db");
    const s = getPoolStats();
    if (s) {
      poolInfo = `total=${s.totalCount} idle=${s.idleCount} waiting=${s.waitingCount} max=${s.max}`;
    }
  } catch {
    /* optional */
  }
  const payload = JSON.stringify({
    workerId: WORKER_ID,
    cycle,
    at: new Date().toISOString(),
    pool: poolInfo,
    pid: process.pid,
  });
  await sql`
    INSERT INTO worker_state (key, value, updated_at)
    VALUES ('worker_heartbeat', ${payload}, now())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = now()
  `;
  await sql`
    INSERT INTO worker_state (key, value, updated_at)
    VALUES ('worker_status', 'online', now())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = now()
  `;

  // P2.11: Persist incremental state on each heartbeat
  await persistIncrementalState(sql);

  // Phase 17 — sample production invariants on heartbeat (non-blocking soft)
  try {
    const { sampleProductionInvariants } = await import(
      "@/lib/prediction/live/invariants"
    );
    await sampleProductionInvariants(sql);
  } catch {
    /* soft */
  }

  // Phase 14 — sample pool pressure; log if PG_POOL_MAX should rise
  try {
    const { logPoolSizingAdvice } = await import("@/lib/db/pool-sizing");
    logPoolSizingAdvice();
  } catch {
    /* soft */
  }

  // Phase 18 — log lifecycle metrics snapshot periodically
  if (cycle % 6 === 0) {
    try {
      const { getLifecycleMetricsSnapshot } = await import(
        "@/lib/observability/metrics/lifecycle-metrics"
      );
      logger.info(
        { component: "live-boot", metrics: getLifecycleMetricsSnapshot() },
        "lifecycle metrics snapshot",
      );
    } catch {
      /* soft */
    }
  }
}

async function releaseWorkerLock(sql: Sql): Promise<void> {
  await sql`
    DELETE FROM worker_locks
    WHERE lock_key = ${LOCK_KEY} AND owner_id = ${WORKER_ID}
  `;
  await sql`
    INSERT INTO worker_state (key, value, updated_at)
    VALUES ('worker_status', 'stopped', now())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = now()
  `;
}

function startLockHeartbeat(getSqlFn: () => Promise<Sql>): void {
  if (lockHeartbeatTimer) return;
  let cycle = 0;
  lockHeartbeatTimer = setInterval(() => {
    cycle += 1;
    void getSqlFn()
      .then(async (sql) => {
        await heartbeatWorkerLock(sql);
        await writeWorkerHealth(sql, cycle);
      })
      .catch((e) => {
        logger.warn(
          { component: "live-boot", error: String(e), cycle },
          "worker lock/health heartbeat failed — will retry next interval",
        );
      });
  }, 10_000);
  lockHeartbeatTimer.unref?.();
}

function stopLockHeartbeat(): void {
  if (lockHeartbeatTimer) {
    clearInterval(lockHeartbeatTimer);
    lockHeartbeatTimer = null;
  }
}


/** Spec §3.10 — startup schema validation. Verifies every required table
 *  exists before any worker role is started. A missing table is a hard
 *  failure: the worker would otherwise fail in an arbitrary place. */
const REQUIRED_TABLES = [
  "crash_rounds",
  "pending_predictions",
  "prediction_validations",
  "notification_outbox",
  "live_event_log",
  "worker_locks",
  "worker_state",
  "acie_online_state",
  "live_round_state",
] as const;

export async function validateSchema(sql: Sql): Promise<void> {
  for (const table of REQUIRED_TABLES) {
    const rows = await sql<{ exists: boolean }>`
      select exists (
        select 1 from information_schema.tables
        where table_schema = 'public' and table_name = ${table}
      ) as exists
    `;
    if (!rows[0]?.exists) {
      throw new Error(
        `Required table missing: ${table}. Run migrations before starting the worker.`,
      );
    }
  }
}

export interface BootResult {
  seed: SeedResult;
  bootStartedAt: string;
}

export interface BootDeps {
  seeder?: () => Promise<SeedResult>;
  dispatcher?: OutboxDispatcher;
  pollWorker?: PollWorker;
  clockMonitor?: ClockSkewMonitor;
  startSubscriber?: () => Promise<void>;
}

class LiveBoot {
  private dispatcher: OutboxDispatcher | null = null;
  private pollWorker: PollWorker | null = null;
  private clockMonitor: ClockSkewMonitor | null = null;
  private started = false;
  private lastResult: BootResult | null = null;

  async start(deps: BootDeps = {}): Promise<BootResult> {
    if (this.started) {
      return this.lastResult!;
    }
    // Do NOT set started=true until all components initialize (P0 startup state machine)
    const bootStartedAt = new Date().toISOString();

    try {
    const seeder = deps.seeder ?? (() => runColdStartSeeder());
    const dispatcher = deps.dispatcher ?? new OutboxDispatcher();
    const pollWorker = deps.pollWorker ?? new PollWorker();
    const clockMonitor = deps.clockMonitor ?? new ClockSkewMonitor();
    this.dispatcher = dispatcher;
    this.pollWorker = pollWorker;
    this.clockMonitor = clockMonitor;

    // Cold-start must not crash the worker process on a transient DB timeout.
    // Railway will otherwise restart-loop while Neon is waking.
    let seed: SeedResult;
    try {
      seed = await seeder();
    } catch (e) {
      logger.error(
        { component: "live-boot", error: String(e) },
        "cold-start seeder threw; continuing with empty seed result",
      );
      seed = {
        alreadySeeded: false,
        initialCount: 0,
        finalCount: 0,
        insertedTotal: 0,
        pagesFetched: 0,
        elapsedMs: 0,
        timedOut: true,
      };
    }

    let sql;
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      try {
        sql = await getSql();
        await sql`select 1`;
        break;
      } catch (e) {
        logger.warn(
          { component: "live-boot", attempt, error: String(e) },
          "DB not ready after cold-start; retrying",
        );
        await new Promise((r) => setTimeout(r, Math.min(2000 * attempt, 8000)));
      }
    }
    if (!sql) {
      throw new Error("Database unreachable after cold-start retries — aborting boot");
    }

    logger.info(
      { component: "live-boot", seed, bootStartedAt },
      "cold-start seeder complete; starting dispatcher / subscriber / poll / monitor",
    );

    // Spec §3.10 — schema validation. Run after the seeder so the seeder
    // has a chance to populate the table list; run before dispatcher /
    // poll so a missing migration fails the boot fast.
    try {
      await validateSchema(sql);
      logger.info(
        { component: "live-boot" },
        "schema validation passed; all required tables present",
      );
      // §5.1 Restore ACIE online state so warm-up is not required after restart.
      try {
        const { ACIEEngine } = await import("@/lib/prediction/acie/engine");
        const eng = new ACIEEngine();
        const result = await loadAcieStateFromDb(eng);
        if (result.restored) {
          logger.info(
            {
              component: "live-boot",
              reason: result.reason,
              observationCount: result.observationCount,
              crashPoints: result.crashPoints,
            },
            "ACIE online state restored (warm)",
          );
        } else {
          logger.warn(
            {
              component: "live-boot",
              reason: result.reason,
              error: result.error ?? null,
            },
            `ACIE starting cold — restore reason: ${result.reason}`,
          );
        }
        (globalThis as { __acieEngine__?: typeof eng }).__acieEngine__ = eng;
      } catch (e) {
        logger.warn(
          {
            component: "live-boot",
            reason: "db_error",
            error: String(e),
            stack: e instanceof Error ? e.stack : undefined,
          },
          "ACIE state restore threw — continuing cold",
        );
        // Still register a cold engine so feedback/live path can observeRound
        try {
          const { ACIEEngine } = await import("@/lib/prediction/acie/engine");
          const cold = new ACIEEngine();
          (globalThis as { __acieEngine__?: typeof cold }).__acieEngine__ = cold;
        } catch {
          /* ACIE optional */
        }
      }

      // P2.11: Restore incremental state after schema validation
      await restoreIncrementalState(sql);
    } catch (e) {
      logger.error(
        { component: "live-boot", error: String(e) },
        "schema validation failed; aborting boot",
      );
      throw e;
    }

    // P0: distributed single-writer lock — wait for expire/steal instead of
    // crash-looping during rolling deploys (previous holder may still be draining).
    let hasLock = await acquireWorkerLock(sql);
    if (!hasLock) {
      const waitMs = Number(process.env.WORKER_LOCK_WAIT_MS ?? 45_000);
      const stepMs = 3_000;
      const deadline = Date.now() + waitMs;
      logger.warn(
        { component: "live-boot", workerId: WORKER_ID, waitMs },
        "Lock held by another worker — waiting for TTL/steal before aborting",
      );
      while (!hasLock && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, stepMs));
        hasLock = await acquireWorkerLock(sql);
      }
    }
    if (!hasLock) {
      logger.error(
        { component: "live-boot", workerId: WORKER_ID },
        "Another worker holds the distributed lock. Refusing to start mutation roles.",
      );
      throw new Error(
        `Worker lock not acquired (another instance holds '${LOCK_KEY}')`,
      );
    }
    logger.info(
      { component: "live-boot", workerId: WORKER_ID },
      "distributed worker lock acquired",
    );
    startLockHeartbeat(getSql);

    await dispatcher.start();
    if (deps.startSubscriber) {
      try {
        await deps.startSubscriber();
      } catch (e) {
        logger.warn(
          { component: "live-boot", error: String(e) },
          "subscriber start failed; continuing with REST fallback",
        );
      }
    }
    startEventLoopLagMonitor();
    startConnectionWarmer(getSql);
    await pollWorker.start();
    await clockMonitor.start();

    this.started = true;
    this.lastResult = { seed, bootStartedAt };
    return this.lastResult;
    } catch (err) {
      // Cleanup partial init so retry can rebuild cleanly
      this.started = false;
      try { await this.dispatcher?.stop(); } catch { /* */ }
      try { await this.pollWorker?.stop(); } catch { /* */ }
      try { await this.clockMonitor?.stop(); } catch { /* */ }
      try {
        const s = await getSql();
        await releaseWorkerLock(s);
      } catch { /* */ }
      this.dispatcher = null;
      this.pollWorker = null;
      this.clockMonitor = null;
      throw err;
    }
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    stopLockHeartbeat();
    if (this.dispatcher) {
      try { await this.dispatcher.stop(); } catch { /* best effort */ }
    }
    if (this.pollWorker) {
      try { await this.pollWorker.stop(); } catch { /* best effort */ }
    }
    if (this.clockMonitor) {
      try { await this.clockMonitor.stop(); } catch { /* best effort */ }
    }
    try {
      const sql = await getSql();
      await releaseWorkerLock(sql);
      // P2.11: Persist incremental state on shutdown
      await persistIncrementalState(sql);
    } catch (e) {
      logger.warn(
        { component: "live-boot", error: String(e) },
        "failed to release worker lock or persist state on stop",
      );
    }
  }
}

const globalRef = globalThis as typeof globalThis & {
  __liveBoot__?: LiveBoot;
};

export function getLiveBoot(): LiveBoot {
  globalRef.__liveBoot__ ??= new LiveBoot();
  return globalRef.__liveBoot__;
}

export async function startLiveBoot(deps?: BootDeps): Promise<BootResult> {
  return getLiveBoot().start(deps);
}

export async function stopLiveBoot(): Promise<void> {
  await getLiveBoot().stop();
}
