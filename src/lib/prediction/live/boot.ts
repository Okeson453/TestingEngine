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

const logger = getLogger("live-boot");

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

    const seed = await seeder();
    logger.info(
      { component: "live-boot", seed, bootStartedAt },
      "cold-start seeder complete; starting dispatcher / subscriber / poll / monitor",
    );

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
