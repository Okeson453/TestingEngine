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
 *   - 3 s interval is an emergency fallback, not a hard temporal guarantee.
 */
import { getSql, type Sql } from "@/lib/db";
import { fetchCrashHistory, type FetchedRound } from "@/lib/crash/fetch-bc";
import { insertNewRounds } from "@/lib/crash/ingest";
import { onGameEnd } from "@/lib/prediction/live/validator";
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

const logger = getLogger("poll-worker");

/** Emergency fallback interval. Canonical env: POLL_WORKER_MS (default 3000).
 *  README previously documented PREDICTION_POLL_MS — that name is unused. */
export const POLL_INTERVAL_MS = Number(
  process.env.POLL_WORKER_MS ?? process.env.PREDICTION_POLL_MS ?? 3_000,
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
  private pages = 2;
  /** When true, poll may call onGameEnd for the single newest eligible round. */
  private allowNewestPredict = true;
  private tickCount = 0;

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
      const rounds = await this.fetchImpl(this.pages);
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
            try {
              const { interRoundGapMs } = await import(
                "@/lib/observability/performance/latency"
              );
              interRoundGapMs.observe(gap);
            } catch { /* optional */ }
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

        // Validation only for all newly inserted rounds (no N+1 cascade)
        for (const r of ins.rounds) {
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
              // Poll path: suppress automatic N+1 from validator; we decide below
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

        // At most one prediction attempt: newest round only, if still eligible
        if (this.allowNewestPredict && sorted.length > 0) {
          const newest = sorted[sorted.length - 1]!;
          const attempted = await this.maybePredictNewest(newest, sql);
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
      logger.error(
        { component: "poll-worker", error: String(e) },
        "poll tick failed",
      );
    }
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
      targetGameId = (BigInt(newest.gameId) + 1n).toString();
    } catch {
      return false;
    }

    const existing = await sql<{ c: number }>`
      SELECT count(*)::int AS c FROM pending_predictions
      WHERE target_game_id = ${targetGameId} AND status = 'PENDING'
    `;
    if ((existing[0]?.c ?? 0) > 0) return false;

    // Prefer live state; fall back to crash_rounds existence
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

    // Stream health: if socket is healthy, prefer letting ED drive prediction
    // and only use poll when the stream appears degraded (optional soft gate)
    try {
      const { bcGameSocket } = await import("@/lib/crash/socket-client");
      const st = bcGameSocket.getState();
      // Use Crash-specific last ED so Dice/Limbo activity cannot suppress
      // the poll recovery path when the Crash socket is silent.
      const lastCrashEd =
        (typeof bcGameSocket.getLastEdAtForGame === "function"
          ? bcGameSocket.getLastEdAtForGame("crash")
          : null) || st.lastEdAt;
      if (st.status === "connected" && lastCrashEd) {
        const lag = Date.now() - new Date(lastCrashEd).getTime();
        if (lag < 30_000) {
          logger.debug(
            { component: "poll-worker", lagMs: lag },
            "socket healthy — defer prediction to ED path",
          );
          return false;
        }
      }
    } catch {
      /* socket module optional in pure unit tests */
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
      // Import lazily to avoid circular deps in tests
      const { onGameEndPredict } = await import("@/lib/prediction/live/predictor");
      const { randomUUID } = await import("node:crypto");
      await onGameEndPredict(
        newest.gameId,
        crashedAt,
        Number(newest.multiplier),
        randomUUID(),
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

  /**
   * Adaptive poll interval: when Socket.IO is blocked/degraded (Cloudflare),
   * poll more aggressively so N+1 predictions stay ahead of the round.
   * Cap at 2s minimum to avoid hammering BC.Game REST.
   */
  private recentGapMs: number[] = [];

  /** Record an observed inter-round gap for adaptive polling (P3 #12). */
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
    try {
      const st = bcGameSocket.getState().status;
      if (
        st === "waf_blocked" ||
        st === "degraded" ||
        st === "reconnecting" ||
        st === "stopped" ||
        st === "connecting"
      ) {
        // Adaptive: ~30% of median inter-round gap, clamped 1–1.5s when unhealthy
        const med = this.medianGapMs();
        if (med != null) {
          const adaptive = Math.round(med * 0.3);
          return Math.max(1_000, Math.min(1_500, adaptive));
        }
        return Math.max(1_000, Math.min(base, 1_500));
      }
    } catch {
      /* socket optional in pure unit tests */
    }
    // Healthy socket: still adapt slightly if we know the gap
    const med = this.medianGapMs();
    if (med != null) {
      return Math.max(2_000, Math.min(base, Math.round(med * 0.5)));
    }
    return base;
  }
}
