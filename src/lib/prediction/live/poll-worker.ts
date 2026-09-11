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
import { attemptNPlusOnePrediction } from "@/lib/prediction/live/prediction-attempt";
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
import { nativeBcGameSocket } from "@/lib/crash/native-socket-client";
import {
  interRoundGapMs,
  httpFetchMs,
  pollTickMs,
  validateBatchMs,
  predictionHandoffMs,
  pollDeferMs,
  crashEdLagMs,
  socketHealthCheckMs,
} from "@/lib/observability/performance/latency";
import { isEdgeFresh } from "@/lib/prediction/live/edge-ingest";
import { hasActiveOrCompletedClaim, peekClaim } from "@/lib/prediction/live/target-coordinator";
import {
  isTargetPastBettingWindow,
  noteRoundStarted,
  getMedianBettingWindowMs,
} from "@/lib/prediction/live/live-round-registry";
import { globalRecentRoundCache } from "@/lib/observability/performance/hot-cache";
import { isAuthoritative } from "@/lib/prediction/live/fencing";

/** Prefer native WS health (workspace breakthrough) over socket.io-client state. */
function liveSocketSnapshot(): { status: string; lastEdAt: number | null } {
  const nativeStatus = nativeBcGameSocket.getStatus();
  const nativeEd = nativeBcGameSocket.getLastEdAt();
  // Fix 4/7: "socket_open" (transport open, /g/cm not joined) is NOT healthy.
  if (nativeStatus === "connected" || nativeStatus === "socket_open") {
    if (nativeStatus === "connected" && nativeEd && Date.now() - nativeEd < 90_000) {
      return { status: "connected", lastEdAt: nativeEd };
    }
    return { status: "degraded", lastEdAt: nativeEd };
  }
  if (nativeStatus === "degraded") {
    return { status: nativeStatus, lastEdAt: nativeEd };
  }
  const st = bcGameSocket.getState();
  const lastEdMs =
    (typeof bcGameSocket.getLastEdAtForGame === "function"
      ? bcGameSocket.getLastEdAtForGame("crash")
      : null) ||
    (st.lastEdAt ? Date.parse(st.lastEdAt) : null);
  return {
    status: st.status,
    lastEdAt: Number.isFinite(lastEdMs as number) ? (lastEdMs as number) : nativeEd,
  };
}

/**
 * Fix 7: poll cadence is a function of native WS health.
 *   healthy   → 2–5s (dormant light verification, WS owns prediction)
 *   degraded  → 500–1000ms (recovery mode)
 *   dead/waf  → 500ms (active recovery)
 */
function wsStreamState(): "healthy" | "degraded" | "dead" {
  const snap = liveSocketSnapshot();
  if (snap.status === "connected" && snap.lastEdAt != null) {
    const age = Date.now() - snap.lastEdAt;
    if (age < 15_000) return "healthy";
    if (age < 90_000) return "degraded";
    return "dead";
  }
  if (snap.status === "degraded") return "degraded";
  return "dead";
}


const logger = getLogger("poll-worker");

// Second-opinion audit rec 1: make the effective poll-defer window visible at
// boot and flag stale env overrides. Code default is 800ms; a leftover
// POLL_HEALTHY_DEFER_MS=2500 in the deploy env (e.g. from the historical
// LATENCY_FIX_AUDIT value) silently widens poll-recovery deferral.
const POLL_DEFER_DEFAULT_MS = 800;
const pollDeferEnvRaw = process.env.POLL_HEALTHY_DEFER_MS;
const HEALTHY_DEFER_MS =
  pollDeferEnvRaw != null && pollDeferEnvRaw !== ""
    ? Number(pollDeferEnvRaw)
    : POLL_DEFER_DEFAULT_MS;
if (pollDeferEnvRaw != null && pollDeferEnvRaw !== "") {
  const parsed = Number(pollDeferEnvRaw);
  if (!Number.isFinite(parsed) || parsed < 0) {
    logger.warn(
      {
        component: "poll-worker",
        envValue: pollDeferEnvRaw,
        effectiveDeferMs: HEALTHY_DEFER_MS,
      },
      "POLL_DEFER_CONFIG: POLL_HEALTHY_DEFER_MS is not a valid number — falling back to default",
    );
  } else if (parsed !== POLL_DEFER_DEFAULT_MS) {
    logger.warn(
      {
        component: "poll-worker",
        envValue: pollDeferEnvRaw,
        effectiveDeferMs: HEALTHY_DEFER_MS,
        defaultMs: POLL_DEFER_DEFAULT_MS,
      },
      "POLL_DEFER_CONFIG: env override active — verify this is intentional (stale 2500 from LATENCY_FIX_AUDIT is a known leftover)",
    );
  }
}

/** Optimized polling interval. Canonical env: POLL_WORKER_MS (default 500).
 *  Lowered 1500→500 so recovery can catch a missed ED within ~1 inter-round
 *  gap. README previously documented PREDICTION_POLL_MS — that name is unused. */
export const POLL_INTERVAL_MS = Number(
  process.env.POLL_WORKER_MS ?? process.env.PREDICTION_POLL_MS ?? 500,
);
export const STALE_PREDICTED_MS = Number(process.env.STUCK_STALE_MS ?? 5 * 60 * 1_000);

/** Rate limiter for repetitive skip logs (sep 11 poll noise fix): the same
 * "target already handled" condition fires every tick while WS is healthy —
 * identical payload, zero new information. Log INFO at most once per 30s
 * per key; demote the rest to debug. */
const skipLogLastAt = new Map<string, number>();
const SKIP_LOG_INTERVAL_MS = 30_000;
function logSkipOncePerInterval(
  key: string,
  fields: Record<string, unknown>,
  message: string,
): void {
  const now = Date.now();
  const last = skipLogLastAt.get(key) ?? 0;
  if (now - last < SKIP_LOG_INTERVAL_MS) {
    logger.debug({ component: "poll-worker", ...fields }, message);
    return;
  }
  skipLogLastAt.set(key, now);
  logger.info({ component: "poll-worker", ...fields }, message);
}

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
  private fetchImpl: (pages: number) => Promise<FetchedRound[]> = (pages) =>
    fetchCrashHistory(
      pages,
      Number(process.env.POLL_FETCH_TIMEOUT_MS ?? process.env.BCGAME_HISTORY_TIMEOUT_MS ?? 2_000) || 2_000,
    );
  /** Default 1 page (50 rounds) — enough for newest-round recovery; was 2. */
  private pages = Math.max(
    1,
    Math.min(5, Number(process.env.PREDICTION_FETCH_PAGES ?? 1) || 1),
  );
  /** When true, poll may call onGameEnd for the single newest eligible round. */
  private allowNewestPredict = true;
  private tickCount = 0;
  /**
   * Last median inter-round gap written durably to worker_state. Rate-limits
   * the durable write (memory gate-cache stays authoritative). Declared here:
   * a5c6bd0 referenced this property without declaring it (tsc TS2339 ×4,
   * undefined at runtime).
   */
  private _lastPersistedGapMs: number | null = null;
  /** Consecutive failed ticks — drives exponential backoff to avoid pool thrash. */
  private consecutiveFailures = 0;
  private lastError: string | null = null;
  /** Guard: at most one BC.Game request in flight. */
  private fetchInFlight = false;
  /**
   * AUDIT 2026-09-11 (bandwidth): while the native WS is healthy, every poll
   * tick still fetched a full 50-round page (~10-15KB) from bc.game every
   * 2-5s — ~200-600MB/day of upstream traffic that insertNewRounds then
   * discarded as already-known rounds. When the WS delivered an ED within
   * WS_FRESH_MAX_AGE_MS, the REST page carries no information the live
   * stream didn't already provide. Skip the fetch on those ticks.
   *
   * Guard rails:
   *   - POLL_SKIP_FETCH_WHEN_WS_HEALTHY=0 disables (default on).
   *   - After MAX_CONSECUTIVE_SKIPS consecutive skips, force one verification
   *     fetch so REST parity is re-proven at least once per ~minute.
   *   - Any WS degradation (age >= WS_FRESH_MAX_AGE_MS) resumes fetching
   *     immediately — recovery behavior is untouched.
   *   - Skipped ticks still run stuck-recovery reconcile.
   */
  private consecutiveSkips = 0;
  private skippedFetches = 0;
  /** Injectable for tests — production reads the live socket snapshot. */
  lastEdAgeMsFn: () => number | null = () => {
    const snap = liveSocketSnapshot();
    return snap.lastEdAt == null ? null : Date.now() - snap.lastEdAt;
  };
  /** Injectable for tests — production uses the module-level wsStreamState(). */
  wsHealthFn: () => "healthy" | "degraded" | "dead" = wsStreamState;

  private static readonly WS_FRESH_MAX_AGE_MS = 10_000;
  private static readonly MAX_CONSECUTIVE_SKIPS = 12;

  private skipFetchEnabled(): boolean {
    return process.env.POLL_SKIP_FETCH_WHEN_WS_HEALTHY !== "0";
  }

  /** True when the live WS makes this tick's REST fetch redundant. */
  private shouldSkipFetchForHealthyWs(): boolean {
    if (!this.skipFetchEnabled()) return false;
    if (this.wsHealthFn() !== "healthy") {
      this.consecutiveSkips = 0;
      return false;
    }
    // "healthy" already means last ED < 15s; require the tighter fresh window.
    const edAgeMs = this.lastEdAgeMsFn();
    if (edAgeMs == null || edAgeMs > PollWorker.WS_FRESH_MAX_AGE_MS) {
      this.consecutiveSkips = 0;
      return false;
    }
    // Periodic REST parity verification.
    if (this.consecutiveSkips >= PollWorker.MAX_CONSECUTIVE_SKIPS) {
      this.consecutiveSkips = 0;
      return false;
    }
    this.consecutiveSkips += 1;
    this.skippedFetches += 1;
    if (this.consecutiveSkips === 1 || this.consecutiveSkips % 10 === 0) {
      logger.info(
        {
          component: "poll-worker",
          consecutiveSkips: this.consecutiveSkips,
          totalSkipped: this.skippedFetches,
          lastEdAgeMs: edAgeMs,
        },
        "poll fetch skipped — WS healthy and ED fresh (REST page redundant)",
      );
    }
    return true;
  }

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
    // Fencing gate (fix plan Phase 1): a worker that lost authority must not
    // mutate via poll recovery. No-op before the registry is initialized.
    if (!isAuthoritative()) {
      return {
        fetched: 0,
        inserted: 0,
        validated: 0,
        predictionAttempts: 0,
        missedRounds: 0,
        stuckRecovered: 0,
        stuckPredicted: 0,
        error: null,
      };
    }
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
      // Bandwidth + DB guard (audit 2026-09-11): when the native WS just
      // delivered an ED, the REST page is redundant — skip the fetch AND the
      // stuck-recovery reconcile (a general-pool query) this tick. While WS
      // healthy, the ED path owns prediction and stuck recovery only needs
      // its verification-fetch cadence (~once per MAX_CONSECUTIVE_SKIPS
      // ticks, ~24s at the healthy 2s cadence; SLA is minutes). Degraded/dead
      // WS never skips, so recovery behavior is untouched.
      if (this.shouldSkipFetchForHealthyWs()) {
        // Keep periodic in-tick maintenance cadence honest (thin-history
        // reseed keys off tickCount) even on skipped ticks.
        this.tickCount += 1;
        result.fetched = 0;
        pollTickMs.observe(performance.now() - tickT0);
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
              const { setMedianInterRoundGapMs } = await import(
                "@/lib/prediction/live/gate-cache"
              );
              setMedianInterRoundGapMs(med);
            } catch { /* soft */ }
            // Rate-limit durable write: memory is authoritative for hot path;
            // DB only every 20 ticks or when gap moves >15%.
            const shouldPersistGap =
              this.tickCount % 20 === 0 ||
              this._lastPersistedGapMs == null ||
              Math.abs(med - this._lastPersistedGapMs) / Math.max(1, this._lastPersistedGapMs) > 0.15;
            if (shouldPersistGap) {
              this._lastPersistedGapMs = med;
              void sql`
                INSERT INTO worker_state (key, value)
                VALUES ('median_inter_round_gap_ms', ${String(med)})
                ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()
              `.catch(() => undefined);
            }
          }
        } catch { /* ignore */ }

        // Update live-round lifecycle from history (does NOT start predictions)
        for (const r of ins.rounds) {
          try {
            await upsertLiveRoundFromHistory(r, sql);
            if (r.crashedAt) {
              await markLiveRoundEnded(
                r.gameId,
                r.crashedAt,
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
          Math.min(2, Number(process.env.POLL_VALIDATE_CONCURRENCY ?? 1) || 1),
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
      const msg = String(e).toLowerCase();
      // Timeouts / 403s are often transient; still count but don't explode.
      this.consecutiveFailures += 1;
      this.lastError = String(e);
      logger.error(
        {
          component: "poll-worker",
          error: String(e),
          consecutiveFailures: this.consecutiveFailures,
          pages: this.pages,
          transient: msg.includes("timeout") || msg.includes("403") || msg.includes("abort"),
        },
        `poll tick failed: ${String(e)} (failures=${this.consecutiveFailures})`,
      );
    }
    if (result.error == null) {
      // Decay quickly on success so one good tick clears multi-failure backoff.
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

    // Fix 2: source must be recent enough for a reliable N+1 (poll lag ceiling)
    const crashedAtDate =
      newest.crashedAt instanceof Date
        ? newest.crashedAt
        : new Date(newest.crashedAt as string);
    const sourceAgeMs = Date.now() - crashedAtDate.getTime();
    // When Socket is WAF-blocked, poll is the only path — allow older sources
    // (history RTT + backoff can push newest past 30s). Cap still prevents
    // predicting from minutes-old backlog.
    let maxSourceAgeMs = Number(process.env.POLL_MAX_SOURCE_AGE_MS ?? 60_000) || 60_000;
    try {
      const st = liveSocketSnapshot().status;
      if (st === "waf_blocked" || st === "degraded" || st === "stopped") {
        maxSourceAgeMs = Math.max(
          maxSourceAgeMs,
          Number(process.env.POLL_MAX_SOURCE_AGE_WAF_MS ?? 90_000) || 90_000,
        );
      }
    } catch { /* soft */ }
    if (Number.isFinite(sourceAgeMs) && sourceAgeMs > maxSourceAgeMs) {
      logger.info(
        {
          component: "poll-worker",
          sourceGameId: newest.gameId,
          sourceAgeMs: Math.round(sourceAgeMs),
          maxSourceAgeMs,
        },
        "skip poll prediction: source round too old for reliable N+1",
      );
      return false;
    }

    // Browser-edge is fresher than poll — defer N+1 to avoid duplicate/cascade
    try {
      const edge = await isEdgeFresh(sql);
      if (edge.fresh) {
        logger.debug(
          {
            component: "poll-worker",
            edgeAgeMs: edge.ageMs,
            lastEdgeGameId: edge.lastGameId,
          },
          "edge feed fresh — defer poll prediction",
        );
        return false;
      }
    } catch { /* soft */ }

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

    // ── Memory-first gates (zero DB RTT) ──
    // Same-process ED claim / registry / recent-round cache answer the common
    // "already handled" cases without competing for the general pool.
    if (hasActiveOrCompletedClaim(targetGameId)) {
      const peek = peekClaim(targetGameId);
      logger.debug(
        {
          component: "poll-worker",
          targetGameId,
          owner: peek?.owner,
          completed: peek?.completed,
        },
        "skip poll prediction: in-memory claim already owns target",
      );
      return false;
    }
    if (isTargetPastBettingWindow(targetGameId)) {
      logSkipOncePerInterval(
        `registry:${targetGameId}`,
        { targetGameId },
        "skip poll prediction: registry says target already started/ended",
      );
      return false;
    }
    if (globalRecentRoundCache.has(targetGameId)) {
      logSkipOncePerInterval(
        `cache:${targetGameId}`,
        { targetGameId },
        "skip poll prediction: target already in recent-round cache",
      );
      return false;
    }

    // ── Single combined DB gate (only if memory is inconclusive) ──
    // Collapses prior 3–4 sequential SELECTs into one round-trip.
    const gate = await sql<{
      pending_c: number;
      lifecycle: string | null;
      crash_exists: boolean;
      began_at: string | Date | null;
    }>`
      SELECT
        (SELECT count(*)::int FROM pending_predictions
          WHERE target_game_id = ${targetGameId} AND status = 'PENDING') AS pending_c,
        (SELECT lifecycle FROM live_round_state
          WHERE game_id = ${targetGameId} LIMIT 1) AS lifecycle,
        (SELECT began_at FROM live_round_state
          WHERE game_id = ${targetGameId} LIMIT 1) AS began_at,
        EXISTS (SELECT 1 FROM crash_rounds WHERE game_id = ${targetGameId}) AS crash_exists
    `.catch(() => [] as { pending_c: number; lifecycle: string | null; crash_exists: boolean; began_at: string | Date | null }[]);
    const g = gate[0];
    if (g) {
      if ((g.pending_c ?? 0) > 0) return false;
      if (g.crash_exists) {
        logSkipOncePerInterval(
          `crash_rounds:${targetGameId}`,
          { targetGameId },
          "newest target already in crash_rounds — skip poll prediction",
        );
        return false;
      }
      const lc = g.lifecycle;
      if (lc === "ENDED" || lc === "RECONCILED") {
        logSkipOncePerInterval(
          `lifecycle:${targetGameId}`,
          { targetGameId, lifecycle: lc },
          "newest target already finished — skip poll prediction",
        );
        return false;
      }
      if (lc === "STARTED" || lc === "RUNNING") {
        // BOOT-RACE FIX (sep 11 15:58 logs): after a restart the in-memory
        // registry is empty, so the cheap registry check above misses for a
        // round that started pre-boot; this gate then forced a recovery
        // attempt the predictor's temporal gate declined anyway
        // ("insufficient window") — a wasted attempt plus boot noise on
        // every cold start with a live round. Seed the registry from
        // live_round_state.began_at so the cheap path catches this round
        // from now on, and skip QUIETLY when the betting window has
        // already elapsed: the temporal contract forbids predicting a
        // target that is about to start or already live, and BG-primary
        // owns the NEXT round's N+1 via its BG event (proven at 15:59:08
        // in the same window). A round whose window is still open still
        // gets a genuine recovery attempt below.
        const beganMs = g.began_at ? new Date(g.began_at).getTime() : NaN;
        const startedAgoMs = Number.isFinite(beganMs) ? Date.now() - beganMs : NaN;
        try {
          noteRoundStarted(targetGameId, Number.isFinite(beganMs) ? beganMs : Date.now());
        } catch { /* soft */ }
        if (Number.isFinite(startedAgoMs) && startedAgoMs > getMedianBettingWindowMs()) {
          logSkipOncePerInterval(
            `boot-window:${targetGameId}`,
            { targetGameId, lifecycle: lc, startedAgoMs: Math.round(startedAgoMs) },
            "live target past betting window (post-boot) — no recovery attempt; BG-primary owns next N+1",
          );
          return false;
        }
        // Pending already checked above (pending_c); force recovery only when
        // lifecycle is live but no pending row exists.
        logger.warn(
          { component: "poll-worker", targetGameId, lifecycle: lc },
          "target live but no pending prediction — forcing recovery attempt",
        );
      }
    }

    // Stream health: only defer if ED path is generating current predictions
    // Prefer native WS (workspace) lastEd / status over socket.io-client.
    try {
      const snap = liveSocketSnapshot();
      const st = { status: snap.status, lastEdAt: snap.lastEdAt };
      const lastCrashEd = snap.lastEdAt;

      if (st.status === "connected" && lastCrashEd) {
        const lag = Date.now() - lastCrashEd;
        // Latency fix: defer window 10s → 2.5s so a missed ED is recovered
        // inside one inter-round gap instead of 2–3 rounds later.
        if (lag < HEALTHY_DEFER_MS) {
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
              // Only defer when pending is current (same or +1). A gap of 2+
              // means ED path already lagged — force poll recovery.
              if (pendingId >= newestId || newestId - pendingId <= 1n) {
                shouldDefer = true;
              }
            } catch {
              shouldDefer = true;
            }
          } else {
            shouldDefer = false;
          }

          if (shouldDefer) {
            try {
              pollDeferMs.observe(lag);
              crashEdLagMs.observe(lag);
            } catch { /* soft */ }
            logger.info(
              {
                component: "poll-worker",
                lagMs: Math.round(lag),
                deferThresholdMs: HEALTHY_DEFER_MS,
                socketStatus: st.status,
                lastCrashEd,
              },
              "socket healthy and predictions current — defer prediction to ED path",
            );
            return false;
          }

          try {
            crashEdLagMs.observe(lag);
          } catch { /* soft */ }
          logger.info(
            {
              component: "poll-worker",
              lagMs: Math.round(lag),
              deferThresholdMs: HEALTHY_DEFER_MS,
              newestGameId: newest.gameId,
              recentPendingId: recentPending[0]?.target_game_id ?? null,
              socketStatus: st.status,
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
      // Fix 8: one authoritative attempt path for ED and recovery alike.
      await attemptNPlusOnePrediction({
        sourceRoundId: newest.gameId,
        sourceCrashAt: crashedAt,
        sourceMultiplier: Number(newest.multiplier),
        source: "RECOVERY",
        correlationId: randomUUID(),
      });
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
    const tickStarted = performance.now();
    try {
      await this.tickOnce();
    } catch (e) {
      logger.error(
        { component: "poll-worker", error: String(e) },
        "tick error — rescheduling (worker stays online)",
      );
    }
    // Cadence fix: subtract elapsed so a slow BC.Game fetch (1–3s) does not
    // push the effective poll period to 3–4s (elapsed + full interval).
    const elapsed = performance.now() - tickStarted;
    const target = this.nextIntervalMs();
    const delay = Math.max(50, Math.round(target - elapsed));
    this.scheduleNext(delay);
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
    // Failure backoff: LINEAR + hard cap. Exponential (base*2^n) produced
    // 2.4–4.8s stalls after 3–4 transient BC.Game timeouts — the operator
    // "3–4 second pulling" symptom. Cap at 2s so recovery stays inside one
    // Crash inter-round gap (~3–5s).
    if (this.consecutiveFailures > 0) {
      const n = Math.min(this.consecutiveFailures, 8);
      const backoff = Math.min(2_000, Math.round(base + n * 250));
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
      // Fix 7: cadence driven by WS stream health.
      const wsState = wsStreamState();
      if (wsState === "healthy") {
        // WS owns the prediction path; poll is dormant light verification.
        return Math.max(2_000, Math.min(5_000, base));
      }
      if (wsState === "degraded") {
        // Recovery mode: aggressive but not thrashing.
        return Math.max(500, Math.min(1_000, base));
      }
      // dead / waf / stopped: poll IS the live path — active recovery.
      return Math.max(250, Math.min(500, base));
    } catch {
      /* socket optional in pure unit tests */
    }
    return Math.max(200, Math.min(1_000, base));
  }
}
