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
        const restored = await loadAcieStateFromDb(eng);
        logger.info(
          { component: "live-boot", restored },
          restored ? "ACIE online state restored" : "ACIE starting fresh (no prior snapshot)",
        );
        (globalThis as { __acieEngine__?: typeof eng }).__acieEngine__ = eng;
      } catch (e) {
        // Log full error so operators can distinguish import vs DB vs schema issues
        logger.warn(
          {
            component: "live-boot",
            error: String(e),
            stack: e instanceof Error ? e.stack : undefined,
          },
          "ACIE state restore skipped — continuing without warm state",
        );
      }
    } catch (e) {
      logger.error(
        { component: "live-boot", error: String(e) },
        "schema validation failed; aborting boot",
      );
      throw e;
    }

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
    if (this.dispatcher) await this.dispatcher.stop();
    if (this.pollWorker) await this.pollWorker.stop();
    if (this.clockMonitor) await this.clockMonitor.stop();
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