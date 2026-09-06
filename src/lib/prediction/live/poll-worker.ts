/**
 * Poll worker — REST safety net / recovery only.
 *
 * Spec: TestingEngine_Comprehensive_Diagnosis_and_Solution.md §2–3, §9
 *
 * Rules:
 *   - NEVER predict from every historical poll result (eliminates cascade).
 *   - At most ONE prediction attempt per tick, and only for the newest
 *     causally eligible round that has no pending N+1 yet and has not
 *     already started/crashed as a live target.
 *   - Primary work: insert missed crash_rounds, validate outcomes,
 *     recover stuck PENDING predictions, detect stream health.
 *   - Adaptive interval; schedule-after-completion (no overlapping requests).
 *
 * Phase 3: prediction/latency deps loaded at module init, not per tick.
 */
import { randomUUID } from "node:crypto";
import { getSql, type Sql } from "@/lib/db";
import { fetchCrashHistory, type FetchedRound } from "@/lib/crash/fetch-bc";
import { insertNewRounds } from "@/lib/crash/ingest";
import { onGameEnd } from "@/lib/prediction/live/validator";
import { onGameEndPredict } from "@/lib/prediction/live/predictor";
import { getLogger } from "@/lib/observability/logger";
import { runColdStartSeeder } from "@/lib/prediction/live/cold-start-seeder";
import {
  reconcileStuckPredictions,
  type StuckRecoveryResult,
} from "@/lib/prediction/live/stuck-recovery";
import {
  upsertLiveRoundFromHistory,
  markLiveRoundEnded,
} from "@/lib/prediction/live/live-round-state";
import { bcGameSocket } from "@/lib/crash/socket-client";
import {
  interRoundGapMs,
  httpFetchMs,
  pollTickMs,
  validateBatchMs,
  predictionHandoffMs,
} from "@/lib/observability/performance/latency";

const logger = getLogger("poll-worker");

/** Optimized polling interval. Canonical env: POLL_WORKER_MS (default 500).
 *  Lowered 1500→500 so recovery can catch a missed ED within ~1 inter-round
 *  gap. README previously documented PREDICTION_POLL_MS — that name is unused. */
export const POLL_INTERVAL_MS = Number(
  process.env.POLL_WORKER_MS ?? process.env.PREDICTION_POLL_MS ?? 500,
);
export const STALE_PREDICTED_MS = Number(process.env.STUCK_STALE_MS ?? 5 * 60 * 1_000);

export interface PollTickResult {
  fetched: number;
  inserted: number;
  validated: number;
  /** Only the newest eligible round may trigger a prediction attempt. */
  predictionAttempts: number;
  missedRounds: number;
  stuckRecovered: number;
  stuckPredicted: number;
  error: string | null;
}

export class PollWorker {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private running = false;
  private getSqlFn: () => Promise<Sql> = getSql;
  private fetchImpl: (pages: number) => Promise<FetchedRound[]> = fetchCrashHistory;
  /** Default 1 page (50 rounds) — enough for newest-round recovery; was 2. */
  private pages = Math.max(
    1,
    Math.min(5, Number(process.env.PREDICTION_FETCH_PAGES ?? 1) || 1),
  );
  /** When true, poll may call onGameEnd for the single newest eligible round. */
  private allowNewestPredict = true;
  private tickCount = 0;
  /** Consecutive failed ticks — drives exponential backoff to avoid pool thrash. */
  private consecutiveFailures = 0;
  private lastError: string | null = null;
  /** Guard: at most one BC.Game request in flight. */
  private fetchInFlight = false;

  constructor(opts?: {
    getSqlFn?: () => Promise<Sql>;
    fetchImpl?: (pages: number) => Promise<FetchedRound[]>;
    pages?: number;
    allowNewestPredict?: boolean;
  }) {
    if (opts?.getSqlFn) this.getSqlFn = opts.getSqlFn;
    if (opts?.fetchImpl) this.fetchImpl = opts.fetchImpl;
    if (opts?.pages != null) this.pages = opts.pages;
    if (opts?.allowNewestPredict != null) this.allowNewestPredict = opts.allowNewestPredict;
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
    const tickT0 = performance.now();
    const sql = await this.getSqlFn();
    const result: PollTickResult = {
      fetched: 0,
      inserted: 0,
      validated: 0,
      predictionAttempts: 0,
      missedRounds: 0,
      stuckRecovered: 0,
      stuckPredicted: 0,
      error: null,
    };

    try {
      // Enforce single active BC.Game request
      if (this.fetchInFlight) {
        logger.warn({ component: "poll-worker" }, "skip tick — fetch already in flight");
        return result;
      }
      this.fetchInFlight = true;
      const fetchT0 = performance.now();
      let rounds: FetchedRound[];
      try {
        rounds = await this.fetchImpl(this.pages);
      } finally {
        this.fetchInFlight = false;
        httpFetchMs.observe(performance.now() - fetchT0);
      }
      result.fetched = rounds.length;

      if (rounds.length > 0) {
        // Sort ascending by gameId for stable processing
        const sorted = [...rounds].sort((a, b) => {
          try {
            return Number(BigInt(a.gameId) - BigInt(b.gameId));
          } catch {
            return String(a.gameId).localeCompare(String(b.gameId));
          }
        });

        const ins = await insertNewRounds(sorted);
        result.inserted = ins.inserted;

        // Adaptive poll: observe inter-round gaps from history timestamps
        try {
          const withCrash = sorted.filter((r) => r.crashedAt);
          for (let i = 1; i < withCrash.length; i += 1) {
            const a = new Date(withCrash[i - 1]!.crashedAt as string | Date).getTime();
            const b = new Date(withCrash[i]!.crashedAt as string | Date).getTime();
            const gap = Math.abs(b - a);
            this.recordInterRoundGap(gap);
            interRoundGapMs.observe(gap);
          }
          const med = this.medianGapMs();
          if (med != null) {
            try {
              await sql`
                INSERT INTO worker_state (key, value)
                VALUES ('median_inter_round_gap_ms', ${String(med)})
                ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()
              `;
            } catch { /* soft */ }
          }
        } catch { /* ignore */ }

        // Update live-round lifecycle from history (does NOT start predictions)
        for (const r of ins.rounds) {
          try {
            await upsertLiveRoundFromHistory(r, sql);
            if (r.crashedAt) {
              await markLiveRoundEnded(
                r.gameId,
                r.crashedAt instanceof Date
                  ? r.crashedAt.toISOString()
                  : String(r.crashedAt),
                Number(r.multiplier),
                sql,
              );
            }
          } catch (le) {
            logger.debug(
              { component: "poll-worker", gameId: r.gameId, error: String(le) },
              "live-round state update skipped",
            );
          }
        }

        // Validation for newly inserted rounds — bounded parallelism
        const VALIDATE_CONCURRENCY = Math.max(
          1,
          Math.min(4, Number(process.env.POLL_VALIDATE_CONCURRENCY ?? 2) || 2),
        );
        {
          const valT0 = performance.now();
          let cursor = 0;
          const workers = Array.from(
            { length: Math.min(VALIDATE_CONCURRENCY, ins.rounds.length) },
            async () => {
              while (cursor < ins.rounds.length) {
                const idx = cursor;
                cursor += 1;
                const r = ins.rounds[idx]!;
                const rawCrash = r.crashedAt as Date | string | null | undefined;
                const crashedAt =
                  rawCrash instanceof Date
                    ? rawCrash.toISOString()
                    : String(rawCrash ?? new Date().toISOString());
                try {
                  const vr = await onGameEnd({
                    gameId: r.gameId,
                    endTime: crashedAt,
                    multiplier: Number(r.multiplier),
                    receivedAt: new Date().toISOString(),
                    skipPredict: true,
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
            },
          );
          await Promise.all(workers);
          validateBatchMs.observe(performance.now() - valT0);
        }

        // At most one prediction attempt: newest round only, if still eligible
        if (this.allowNewestPredict && sorted.length > 0) {
          const newest = sorted[sorted.length - 1]!;
          const handoffT0 = performance.now();
          const attempted = await this.maybePredictNewest(newest, sql);
          predictionHandoffMs.observe(performance.now() - handoffT0);
          if (attempted) result.predictionAttempts = 1;
        }
      }

      // Stuck prediction recovery (safe reconcile, not blind cancel)
      try {
        const recovery: StuckRecoveryResult = await reconcileStuckPredictions(sql);
        result.stuckRecovered = recovery.reconciled + recovery.cancelled;

        // Periodic thin-history reseed (D10 / 6.8): every 20 ticks if history < 100
        this.tickCount += 1;
        if (this.tickCount % 20 === 0) {
          try {
            const seed = await runColdStartSeeder({ maxPages: 2, timeoutMs: 8_000 });
            if (!seed.alreadySeeded && seed.insertedTotal > 0) {
              logger.info(
                { component: "poll-worker", inserted: seed.insertedTotal, finalCount: seed.finalCount },
                "thin-history reseed inserted rounds",
              );
            }
          } catch (seedErr) {
            logger.debug(
              { component: "poll-worker", error: String(seedErr) },
              "thin-history reseed soft-failed",
            );
          }
        }
        result.stuckPredicted = recovery.stillLive;
      } catch (re) {
        logger.warn(
          { component: "poll-worker", error: String(re) },
          "stuck recovery failed",
        );
      }
    } catch (e) {
      result.error = String(e);
      this.consecutiveFailures += 1;
      this.lastError = String(e);
      logger.error(
        {
          component: "poll-worker",
          error: String(e),
          consecutiveFailures: this.consecutiveFailures,
          pages: this.pages,
        },
        `poll tick failed: ${String(e)} (failures=${this.consecutiveFailures})`,
      );
    }
    if (result.error == null) {
      this.consecutiveFailures = 0;
      this.lastError = null;
    }
    pollTickMs.observe(performance.now() - tickT0);
    return result;
  }

  /**
   * Newest-round-only prediction gate.
   * Predict only when:
   *   - no PENDING prediction for target = newest+1
   *   - target has not already started/crashed in live state or crash_rounds
   *   - newest itself is fully ended (has crash time)
   */
  private async maybePredictNewest(
    newest: FetchedRound,
    sql: Sql,
  ): Promise<boolean> {
    if (!newest.crashedAt) return false;

    let targetGameId: string;
    try {
      if (typeof newest.gameId !== "string" || !/^\d+$/.test(newest.gameId)) {
        logger.warn(
          { component: "poll-worker", sourceGameId: newest.gameId },
          "skip poll prediction: source gameId not safe numeric sequence",
        );
        return false;
      }
      const next = BigInt(newest.gameId) + 1n;
      if (next <= 0n) return false;
      targetGameId = next.toString();
    } catch {
      return false;
    }

    const existing = await sql<{ c: number }>`
      SELECT count(*)::int AS c FROM pending_predictions
      WHERE target_game_id = ${targetGameId} AND status = 'PENDING'
    `;
    if ((existing[0]?.c ?? 0) > 0) return false;

    const live = await sql<{ lifecycle: string }>`
      SELECT lifecycle FROM live_round_state WHERE game_id = ${targetGameId} LIMIT 1
    `.catch(() => [] as { lifecycle: string }[]);
    if (live.length > 0) {
      const lc = live[0]!.lifecycle;
      if (lc === "STARTED" || lc === "RUNNING" || lc === "ENDED" || lc === "RECONCILED") {
        logger.info(
          { component: "poll-worker", targetGameId, lifecycle: lc },
          "newest target already live — skip poll prediction",
        );
        return false;
      }
    }

    const already = await sql<{ game_id: string }>`
      SELECT game_id FROM crash_rounds WHERE game_id = ${targetGameId} LIMIT 1
    `;
    if (already.length > 0) {
      logger.info(
        { component: "poll-worker", targetGameId },
        "newest target already in crash_rounds — skip poll prediction",
      );
      return false;
    }

    // Stream health: only defer if ED path is generating current predictions
    try {
      const st = bcGameSocket.getState();
      const lastCrashEd =
        (typeof bcGameSocket.getLastEdAtForGame === "function"
          ? bcGameSocket.getLastEdAtForGame("crash")
          : null) || st.lastEdAt;

      if (st.status === "connected" && lastCrashEd) {
        const lag = Date.now() - new Date(lastCrashEd).getTime();
        if (lag < 10_000) {
          const recentPending = await sql<{ target_game_id: string }>`
            SELECT target_game_id FROM pending_predictions
            WHERE status = 'PENDING'
            ORDER BY requested_at DESC
            LIMIT 1
          `.catch(() => [] as { target_game_id: string }[]);

          let shouldDefer = false;
          if (recentPending.length > 0) {
            try {
              const pendingId = BigInt(recentPending[0]!.target_game_id);
              const newestId = BigInt(newest.gameId);
              if (pendingId >= newestId || newestId - pendingId <= 2n) {
                shouldDefer = true;
              }
            } catch {
              shouldDefer = true;
            }
          } else {
            shouldDefer = false;
          }

          if (shouldDefer) {
            logger.debug(
              { component: "poll-worker", lagMs: lag },
              "socket healthy and predictions current — defer prediction to ED path",
            );
            return false;
          }

          logger.info(
            {
              component: "poll-worker",
              lagMs: lag,
              newestGameId: newest.gameId,
              recentPendingId: recentPending[0]?.target_game_id ?? null,
            },
            "socket appears healthy but predictions are stale — forcing poll recovery",
          );
        }
      }
    } catch {
      /* socket optional in pure unit tests */
    }

    const crashedAt =
      newest.crashedAt instanceof Date
        ? newest.crashedAt.toISOString()
        : String(newest.crashedAt);

    logger.info(
      {
        component: "poll-worker",
        sourceGameId: newest.gameId,
        targetGameId,
      },
      "poll recovery: single newest-round prediction attempt",
    );

    try {
      await onGameEndPredict(
        newest.gameId,
        crashedAt,
        Number(newest.multiplier),
        randomUUID(),
        { recoveryMode: true },
      );
      return true;
    } catch (e) {
      logger.warn(
        { component: "poll-worker", error: String(e) },
        "newest prediction attempt failed",
      );
      return false;
    }
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
    try {
      await this.tickOnce();
    } catch (e) {
      logger.error(
        { component: "poll-worker", error: String(e) },
        "tick error — rescheduling (worker stays online)",
      );
    }
    // Always reschedule so a single DB/network failure cannot stop the loop.
    this.scheduleNext(this.nextIntervalMs());
  }

  private recentGapMs: number[] = [];

  recordInterRoundGap(gapMs: number): void {
    if (!Number.isFinite(gapMs) || gapMs <= 0 || gapMs > 120_000) return;
    this.recentGapMs.push(gapMs);
    if (this.recentGapMs.length > 30) this.recentGapMs.shift();
  }

  private medianGapMs(): number | null {
    if (this.recentGapMs.length < 3) return null;
    const sorted = [...this.recentGapMs].sort((a, b) => a - b);
    return sorted[Math.floor(sorted.length / 2)]!;
  }

  private nextIntervalMs(): number {
    const base = POLL_INTERVAL_MS;
    if (this.consecutiveFailures > 0) {
      const backoff = Math.min(
        30_000,
        Math.round(base * Math.pow(2, Math.min(this.consecutiveFailures, 6))),
      );
      logger.warn(
        {
          component: "poll-worker",
          consecutiveFailures: this.consecutiveFailures,
          backoffMs: backoff,
          lastError: this.lastError,
        },
        `poll backoff ${backoff}ms after ${this.consecutiveFailures} failures`,
      );
      return backoff;
    }
    try {
      const st = bcGameSocket.getState().status;
      const med = this.medianGapMs();
      if (med != null) {
        const adaptive = Math.round(med * 0.25);
        const minMs =
          st === "waf_blocked" || st === "degraded" ? 400 : 200;
        return Math.max(minMs, Math.min(1_000, adaptive));
      }
      return Math.max(200, Math.min(1_000, base));
    } catch {
      /* socket optional in pure unit tests */
    }
    return Math.max(200, Math.min(1_000, base));
  }
}
