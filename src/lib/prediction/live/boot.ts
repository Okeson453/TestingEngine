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
    this.started = true;
    const bootStartedAt = new Date().toISOString();

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
      }
    } catch (e) {
      logger.error(
        { component: "live-boot", error: String(e) },
        "schema validation failed; aborting boot",
      );
      throw e;
    }

    // P0: distributed single-writer lock
    const hasLock = await acquireWorkerLock(sql);
    if (!hasLock) {
      logger.error(
        { component: "live-boot", workerId: WORKER_ID },
        "Another worker holds the distributed lock. Refusing to start mutation roles.",
      );
      // Still allow read-only / outbox recovery? Spec says shut down.
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
    await pollWorker.start();
    await clockMonitor.start();

    this.lastResult = { seed, bootStartedAt };
    return this.lastResult;
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
    } catch (e) {
      logger.warn(
        { component: "live-boot", error: String(e) },
        "failed to release worker lock on stop",
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