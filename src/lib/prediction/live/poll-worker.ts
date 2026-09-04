/**
 * Poll worker — REST safety net.
 *
 * Spec: TestingEngine_Deep_Diagnosis.md §3.5 / §4.2
 *
 * The poll worker does NOT generate predictions itself. It:
 *   1. Fetches history and inserts missed crash_rounds
 *   2. Calls live/validator.onGameEnd for each newly discovered round
 *      (which validates any pending prediction and triggers N+1 prediction)
 *   3. Logs stuck / orphaned predictions
 */
import { getSql, type Sql } from "@/lib/db";
import { fetchCrashHistory, type FetchedRound } from "@/lib/crash/fetch-bc";
import { insertNewRounds } from "@/lib/crash/ingest";
import { onGameEnd } from "@/lib/prediction/live/validator";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("poll-worker");

export const POLL_INTERVAL_MS = Number(process.env.POLL_WORKER_MS ?? 60_000);
export const STALE_PREDICTED_MS = 15 * 60 * 1_000;

export interface PollTickResult {
  fetched: number;
  inserted: number;
  validated: number;
  missedRounds: number;
  stuckPredicted: number;
  error: string | null;
}

export class PollWorker {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private running = false;
  private getSqlFn: () => Promise<Sql> = getSql;
  private fetchImpl: (pages: number) => Promise<FetchedRound[]> = fetchCrashHistory;
  private pages = 2;

  constructor(opts?: {
    getSqlFn?: () => Promise<Sql>;
    fetchImpl?: (pages: number) => Promise<FetchedRound[]>;
    pages?: number;
  }) {
    if (opts?.getSqlFn) this.getSqlFn = opts.getSqlFn;
    if (opts?.fetchImpl) this.fetchImpl = opts.fetchImpl;
    if (opts?.pages != null) this.pages = opts.pages;
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    this.scheduleNext(0);
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  async tickOnce(): Promise<PollTickResult> {
    const sql = await this.getSqlFn();
    const result: PollTickResult = {
      fetched: 0,
      inserted: 0,
      validated: 0,
      missedRounds: 0,
      stuckPredicted: 0,
      error: null,
    };
    try {
      const rounds = await this.fetchImpl(this.pages);
      result.fetched = rounds.length;
      if (rounds.length > 0) {
        const ins = await insertNewRounds(rounds);
        result.inserted = ins.inserted;

        for (const r of ins.rounds) {
          const crashedAt =
            r.crashedAt instanceof Date
              ? r.crashedAt.toISOString()
              : String(r.crashedAt ?? new Date().toISOString());
          try {
            const vr = await onGameEnd({
              gameId: r.gameId,
              endTime: crashedAt,
              multiplier: Number(r.multiplier),
              receivedAt: new Date().toISOString(),
            });
            if (vr.kind === "resolved") result.validated += 1;
            if (vr.kind === "orphaned" || vr.kind === "bg_arrived_late") {
              result.missedRounds += 1;
            }
          } catch (ve) {
            logger.warn(
              { component: "poll-worker", gameId: r.gameId, error: String(ve) },
              "validation during poll failed",
            );
          }
        }
      }

      const stuck = await sql<{ count: number }>`
        select count(*)::int as count from pending_predictions
        where matched = false
          and target_round_started_at is not null
          and target_round_started_at < now() - (${STALE_PREDICTED_MS}::int * interval '1 millisecond')
      `;
      result.stuckPredicted = stuck[0]?.count ?? 0;
      if (result.stuckPredicted > 0) {
        logger.warn(
          { component: "poll-worker", stuckPredicted: result.stuckPredicted },
          "stuck PREDICTED rows older than 15 minutes",
        );
      }
    } catch (e) {
      result.error = String(e);
      logger.error(
        { component: "poll-worker", error: String(e) },
        "poll tick failed",
      );
    }
    return result;
  }

  private scheduleNext(delayMs: number): void {
    if (!this.running) return;
    this.timer = setTimeout(() => {
      void this.runOneTick();
    }, delayMs);
    this.timer.unref?.();
  }

  private async runOneTick(): Promise<void> {
    if (!this.running) return;
    await this.tickOnce();
    this.scheduleNext(POLL_INTERVAL_MS);
  }
}
