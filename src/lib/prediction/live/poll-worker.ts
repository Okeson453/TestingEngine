/**
 * Poll worker — REST safety net.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.5
 *
 * The poll worker NEVER initiates a prediction. It is the safety net that
 * fills in `crash_rounds` rows that the Socket.IO subscription missed
 * during a disconnect, and that marks any rounds whose `bg` was missed
 * (i.e. they exist in `crash_rounds` but have no `pending_predictions`
 * row) as observability warnings — without ever back-generating
 * predictions.
 */
import { getSql, type Sql } from "@/lib/db";
import { fetchCrashHistory, type FetchedRound } from "@/lib/crash/fetch-bc";
import { insertNewRounds } from "@/lib/crash/ingest";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("poll-worker");

export const POLL_INTERVAL_MS = Number(process.env.POLL_WORKER_MS ?? 60_000);
/** Stale threshold: a PREDICTED row older than this is observable as stuck. */
export const STALE_PREDICTED_MS = 15 * 60 * 1_000;

export interface PollTickResult {
  fetched: number;
  inserted: number;
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

  /** Run a single reconciliation pass. Returns the result for testability. */
  async tickOnce(): Promise<PollTickResult> {
    const sql = await this.getSqlFn();
    const result: PollTickResult = {
      fetched: 0,
      inserted: 0,
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
          const pending = await sql<{ count: number }>`
            select count(*)::int as count
            from pending_predictions
            where target_game_id = ${r.gameId}
          `;
          if ((pending[0]?.count ?? 0) === 0) {
            result.missedRounds += 1;
            logger.warn(
              {
                component: "poll-worker",
                gameId: r.gameId,
                missedRound: true,
              },
              "reconciled round has no prediction; socket was likely offline during bg",
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
