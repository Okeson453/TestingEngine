/**
 * LiveSupervisor (Diagnosis fixes 1, 6, 14).
 *
 * Single owner of ALL control-loop timers in the worker process:
 *   - worker lock heartbeat + worker health write (10s)
 *   - production invariant monitor (30s) — started EXACTLY ONCE (fix 1:
 *     the old code created a new permanent setInterval inside the recurring
 *     heartbeat, multiplying invariant timers every 10s → warning storms)
 *   - connection warmer (3s)
 *   - event-loop lag probe (2s)
 *
 * Also derives the authoritative WorkerHealth record (fix 14): ONLINE is a
 * derived state — process alive AND DB healthy AND ws live AND namespace
 * joined AND recent ED — never a bare online/offline string.
 */
import { getSql, getCriticalSql, getPoolStats, type Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";
import { nativeBcGameSocket } from "@/lib/crash/native-socket-client";
import { getLastSignalAt } from "@/lib/prediction/live/latency-trace";
import { markAuthorityLost } from "@/lib/prediction/live/fencing";

const logger = getLogger("live-supervisor");

const HEARTBEAT_INTERVAL_MS = 10_000;
const INVARIANT_INTERVAL_MS = 30_000;
const CONNECTION_WARMER_MS = 3_000;
const EVENT_LOOP_PROBE_MS = 2_000;
/** ED older than this (while joined) means the stream is not live. */
const LIVE_ED_WINDOW_MS = 20_000;

export type HealthState =
  | "healthy"
  | "degraded"
  | "down"
  | "alive"
  | "stopping"
  | "live"
  | string;

export interface WorkerHealth {
  process: "alive" | "stopping";
  ws: "live" | "degraded" | "down";
  namespaceJoined: boolean;
  lastEdAt: number | null;
  lastBgAt: number | null;
  lastSignalAt: number | null;
  lastHeartbeatAt: number;
  db: "healthy" | "degraded" | "down";
  lifecycle: "COLD" | "WARMING" | "WARM" | "PRODUCTION" | "DEGRADED";
  online: boolean;
}

export class LiveSupervisor {
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private invariantTimer: ReturnType<typeof setInterval> | null = null;
  private warmerTimer: ReturnType<typeof setInterval> | null = null;
  private probeTimer: ReturnType<typeof setInterval> | null = null;
  private getSqlFn: () => Promise<Sql> = getSql;
  private cycle = 0;
  private lastDbOkAt: number | null = null;
  /** Consecutive zero-row lock heartbeats — >=2 triggers self-demotion. */
  private lockLostStrikes = 0;
  private lastHeartbeatAt: number | null = null;
  private lifecycle: WorkerHealth["lifecycle"] = "COLD";
  private lastHealth: WorkerHealth | null = null;

  constructor(deps?: { getSqlFn?: () => Promise<Sql> }) {
    if (deps?.getSqlFn) this.getSqlFn = deps.getSqlFn;
  }

  /** Fix 14: derive the single authoritative health record. */
  deriveWorkerHealth(): WorkerHealth {
    const wsStatus = nativeBcGameSocket.getStatus();
    const joined = nativeBcGameSocket.isJoined();
    const lastEdAt = nativeBcGameSocket.getLastEdAt();
    const lastEventAt = nativeBcGameSocket.getLastEventAt();
    const now = Date.now();

    let ws: WorkerHealth["ws"] = "down";
    if (wsStatus === "connected" && joined && lastEdAt != null && now - lastEdAt < LIVE_ED_WINDOW_MS) {
      ws = "live";
    } else if (
      wsStatus === "connected" ||
      wsStatus === "socket_open" ||
      wsStatus === "degraded"
    ) {
      ws = "degraded";
    }

    let db: WorkerHealth["db"] = "down";
    if (this.lastDbOkAt != null && now - this.lastDbOkAt < 30_000) db = "healthy";
    else if (this.lastDbOkAt != null && now - this.lastDbOkAt < 90_000) db = "degraded";

    // ONLINE (fix 14): process alive AND db healthy AND ws live AND joined
    // AND recent ED. No more false online/offline oscillation.
    const online =
      this.lifecycle === "PRODUCTION" && db === "healthy" && ws === "live" && joined;

    return {
      process: this.lifecycle === "COLD" ? "stopping" : "alive",
      ws,
      namespaceJoined: joined,
      lastEdAt,
      lastBgAt: lastEventAt,
      lastSignalAt: getLastSignalAt(),
      lastHeartbeatAt: this.lastHeartbeatAt ?? 0,
      db,
      lifecycle: this.lifecycle,
      online,
    };
  }

  start(): void {
    this.startEventLoopLagMonitor();
    this.startConnectionWarmer();
    this.startInvariantMonitor(); // fix 1: exactly one timer, created once
    this.startHeartbeat();
    this.lifecycle = "WARMING";
  }

  async stop(): Promise<void> {
    this.lifecycle = "COLD";
    for (const t of [this.heartbeatTimer, this.invariantTimer, this.warmerTimer, this.probeTimer]) {
      if (t) clearInterval(t);
    }
    this.heartbeatTimer = null;
    this.invariantTimer = null;
    this.warmerTimer = null;
    this.probeTimer = null;
  }

  /** Fix 1: module-singleton invariant monitor — created ONCE, not per heartbeat. */
  private startInvariantMonitor(): void {
    if (this.invariantTimer) return;
    const run = (): void => {
      void (async () => {
        const sql = await this.getSqlFn();
        const { sampleProductionInvariants } = await import(
          "@/lib/prediction/live/invariants"
        );
        await sampleProductionInvariants(sql);
        // Recovery sweep for genuinely-stuck feedback rows (idempotent via the
        // durable claim; skip-reasoned rows excluded). Same cadence as the
        // invariant sample — self-heals crash windows between validation
        // commit and the deferred feedback call.
        const { sweepStuckFeedback } = await import(
          "@/lib/prediction/live/feedback"
        );
        await sweepStuckFeedback(sql);
        return undefined;
      })()
        .catch((error) => {
          logger.error(
            { component: "live-supervisor", error: String(error) },
            "production invariant check failed",
          );
        });
    };
    run(); // first check immediately
    this.invariantTimer = setInterval(run, INVARIANT_INTERVAL_MS);
    this.invariantTimer.unref?.();
  }

  private startHeartbeat(): void {
    if (this.heartbeatTimer) return;
    // Split-brain guard (P0): if the lock was force-stolen by a newer
    // instance, our heartbeat UPDATE matches zero rows. A single zero-row
    // heartbeat is definitive (the claim UPDATE itself would throw on a
    // transient DB error — false is a real answer, not a network flake), but
    // we require 2 consecutive losses (~20s) so an exotic race can't demote a
    // healthy worker. After demotion this process stops predicting and
    // dispatching; the instance that stole the lock owns the live path.
    this.heartbeatTimer = setInterval(() => {
      this.cycle += 1;
      void this.getSqlFn()
        .then(async (sql) => {
          this.lastDbOkAt = Date.now();
          const ownsLock = await heartbeatWorkerLock(sql);
          if (ownsLock) {
            this.lockLostStrikes = 0;
          } else {
            this.lockLostStrikes += 1;
            logger.error(
              {
                component: "live-supervisor",
                workerId: WORKER_ID,
                lockLostStrikes: this.lockLostStrikes,
                cycle: this.cycle,
              },
              "WORKER_LOCK_LOST: heartbeat matched zero rows — lock held by another instance",
            );
            if (this.lockLostStrikes >= 2) {
              logger.error(
                { component: "live-supervisor", workerId: WORKER_ID },
                "WORKER_LOCK_DEMOTED: stopping predict/dispatch/invariant loops (split-brain guard)",
              );
              // Fix plan Phase 2: fire the cancellation cascade — stops the
              // dispatcher, poll worker, clock monitor; ed/poll/dispatch
              // gates refuse further authoritative mutation.
              markAuthorityLost(`lock lost for ${this.lockLostStrikes} consecutive heartbeats`);
              this.stop();
            }
          }
          await this.writeWorkerHealth(sql, this.cycle);
        })
        .catch((e) => {
          logger.warn(
            { component: "live-supervisor", error: String(e), cycle: this.cycle },
            "worker lock/health heartbeat failed — will retry next interval",
          );
        });
    }, HEARTBEAT_INTERVAL_MS);
    this.heartbeatTimer.unref?.();
  }

  private startConnectionWarmer(): void {
    if (this.warmerTimer) return;
    // LATENCY FIX: the previous warmer only touched getSql() (general pool).
    // Prediction persist + outbox dispatch use getCriticalSql(). Cold critical
    // connections paid ~700–1500ms Neon TLS on every ED; warm both pools.
    this.warmerTimer = setInterval(() => {
      void Promise.all([
        this.getSqlFn().then((sql) => sql`SELECT 1`),
        getCriticalSql().then((sql) => sql`SELECT 1`),
      ])
        .then(() => {
          this.lastDbOkAt = Date.now();
        })
        .catch(() => undefined);
    }, CONNECTION_WARMER_MS);
    this.warmerTimer.unref?.();
  }

  /** Ring buffer of recent event-loop lag samples for p50/p95/p99 (P1). */
  private lagSamples: number[] = [];
  private static readonly LAG_SAMPLE_CAP = 120;

  private startEventLoopLagMonitor(): void {
    if (this.probeTimer) return;
    this.probeTimer = setInterval(() => {
      const start = process.hrtime.bigint();
      setImmediate(() => {
        const lagMs = Number(process.hrtime.bigint() - start) / 1_000_000;
        this.lagSamples.push(lagMs);
        if (this.lagSamples.length > LiveSupervisor.LAG_SAMPLE_CAP) {
          this.lagSamples.shift();
        }
        const stats = this.computeLagPercentiles();
        try {
          const g = globalThis as {
            __eventLoopLagMs__?: number;
            __eventLoopLagP50__?: number;
            __eventLoopLagP95__?: number;
            __eventLoopLagP99__?: number;
          };
          g.__eventLoopLagMs__ = lagMs;
          g.__eventLoopLagP50__ = stats.p50 ?? undefined;
          g.__eventLoopLagP95__ = stats.p95 ?? undefined;
          g.__eventLoopLagP99__ = stats.p99 ?? undefined;
        } catch {
          /* ignore */
        }
        if (lagMs > 50 || (stats.p99 != null && stats.p99 > 100)) {
          logger.warn(
            {
              eventLoopLagMs: Math.round(lagMs),
              event_loop_lag_p50: stats.p50,
              event_loop_lag_p95: stats.p95,
              event_loop_lag_p99: stats.p99,
            },
            "Event loop lag detected",
          );
        }
      });
    }, EVENT_LOOP_PROBE_MS);
    this.probeTimer.unref?.();
  }

  private computeLagPercentiles(): {
    p50: number | null;
    p95: number | null;
    p99: number | null;
  } {
    const n = this.lagSamples.length;
    if (n === 0) return { p50: null, p95: null, p99: null };
    const sorted = [...this.lagSamples].sort((a, b) => a - b);
    const at = (p: number) =>
      Math.round(sorted[Math.min(n - 1, Math.max(0, Math.ceil((p / 100) * n) - 1))]!);
    return { p50: at(50), p95: at(95), p99: at(99) };
  }

  async writeWorkerHealth(sql: Sql, cycle: number): Promise<void> {
    let poolInfo = "";
    try {
      const s = getPoolStats();
      if (s) {
        poolInfo = `total=${s.totalCount} idle=${s.idleCount} waiting=${s.waitingCount} max=${s.max}`;
      }
    } catch {
      /* optional */
    }
    const lag = this.computeLagPercentiles();
    const payload = JSON.stringify({
      workerId: WORKER_ID,
      cycle,
      at: new Date().toISOString(),
      pool: poolInfo,
      pid: process.pid,
      event_loop_lag_p50: lag.p50,
      event_loop_lag_p95: lag.p95,
      event_loop_lag_p99: lag.p99,
    });
    // Fix 14: worker_status is now DERIVED, not a bare online string.
    this.lastHeartbeatAt = Date.now();
    const health = this.deriveWorkerHealth();
    this.lifecycle = "PRODUCTION";
    health.lifecycle = this.lifecycle;
    health.process = "alive";
    this.lastHealth = health;
    // HEALTH STATE: the three worker_state rows (heartbeat/status/health)
    // used to be THREE sequential general-pool upserts every 10s heartbeat —
    // a standing tax on the pool shared with dispatcher normal-lane claims,
    // forensics and feedback sweeps. One data-modifying CTE, one round trip.
    const healthJson = JSON.stringify({
      ...health,
      lastEdAt: health.lastEdAt != null ? new Date(health.lastEdAt).toISOString() : null,
      lastBgAt: health.lastBgAt != null ? new Date(health.lastBgAt).toISOString() : null,
      lastSignalAt: health.lastSignalAt != null ? new Date(health.lastSignalAt).toISOString() : null,
      lastHeartbeatAt: new Date(health.lastHeartbeatAt).toISOString(),
    });
    await sql`
      WITH hb AS (
        INSERT INTO worker_state (key, value, updated_at)
        VALUES ('worker_heartbeat', ${payload}, now())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = now()
        RETURNING 1
      ),
      st AS (
        INSERT INTO worker_state (key, value, updated_at)
        VALUES ('worker_status', ${health.online ? 'online' : 'offline'}, now())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = now()
        RETURNING 1
      ),
      hlth AS (
        INSERT INTO worker_state (key, value, updated_at)
        VALUES ('worker_health', ${healthJson}, now())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = now()
        RETURNING 1
      )
      SELECT
        (SELECT count(*) FROM hb) AS heartbeat_written,
        (SELECT count(*) FROM st) AS status_written,
        (SELECT count(*) FROM hlth) AS health_written
    `.catch((e) => {
      logger.debug(
        { component: "live-supervisor", error: String(e) },
        "worker_state health persistence failed (soft)",
      );
    });

    // P2.11: Persist incremental state — every 6th heartbeat (60s), not
    // every cycle. This is crash-recovery model state (EWMA/Welford/baseline
    // snapshots); 60s staleness is immaterial for restart fidelity and the
    // old every-10s cadence put 3 more general-pool writes on the same pool
    // the dispatcher's normal lane and forensics share (observed general
    // pool 5/0/1 during realtime windows).
    if (cycle % 6 === 0) {
      try {
        await persistIncrementalState(sql);
      } catch {
        /* soft */
      }
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
          { component: "live-supervisor", metrics: getLifecycleMetricsSnapshot() },
          "lifecycle metrics snapshot",
        );
      } catch {
        /* soft */
      }
    }
  }

  getLastHealth(): WorkerHealth | null {
    return this.lastHealth;
  }
}

/** Distributed single-writer lock (P0). Uses worker_locks table from 0006. */
export const WORKER_ID =
  process.env.RAILWAY_REPLICA_ID ||
  process.env.WORKER_ID ||
  `worker-${process.pid}-${Math.random().toString(36).slice(2, 10)}`;
const LOCK_KEY = "prediction_worker";
const LOCK_TTL_SECONDS = 8;

export async function heartbeatWorkerLock(sql: Sql): Promise<boolean> {
  const rows = await sql<{ owner_id: string }>`
    UPDATE worker_locks
    SET heartbeat_at = now(),
        expires_at = now() + (${LOCK_TTL_SECONDS}::int * interval '1 second')
    WHERE lock_key = ${LOCK_KEY} AND owner_id = ${WORKER_ID}
    RETURNING owner_id
  `;
  // Zero rows = we no longer own the lock (force-stolen by a newer instance).
  // Caller (live-supervisor) counts consecutive losses and self-demotes.
  return rows.length > 0;
}

// P2.11: Persist Incremental State
// Save incremental state alongside worker health.
// ROUND-TRIP FIX (sep 11): this used to be THREE sequential worker_state
// upserts (incremental_state, baseline_adaptive_state, safe_baseline_state)
// on every 10s heartbeat — six general-pool round trips per minute of pure
// telemetry. One data-modifying CTE, one round trip, same upsert semantics
// per key. Called every 6th heartbeat (60s) by writeWorkerHealth.
export async function persistIncrementalState(sql: Sql): Promise<void> {
  try {
    const { globalIncrementalState } = await import(
      "@/lib/prediction/state/incremental-state-engine"
    );
    const { globalBaselineModel } = await import(
      "@/lib/prediction/models/baseline-model"
    );
    const { globalSafeBaseline } = await import(
      "@/lib/prediction/lifecycle/safe-baseline-controller"
    );
    const snap = globalIncrementalState.snapshot();
    const incrementalJson = JSON.stringify({
      count: snap.count,
      ewma: snap.ewma,
      ewmaHit13: snap.ewmaHit13,
      welford: snap.welford,
      runs: snap.runs,
      timestamp: new Date().toISOString(),
    });
    const baselineJson = JSON.stringify(globalBaselineModel.exportState());
    const safeJson = JSON.stringify(globalSafeBaseline.exportState());
    await sql`
      WITH inc AS (
        INSERT INTO worker_state (key, value, updated_at)
        VALUES ('incremental_state', ${incrementalJson}, now())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = now()
        RETURNING 1
      ),
      base AS (
        INSERT INTO worker_state (key, value, updated_at)
        VALUES ('baseline_adaptive_state', ${baselineJson}, now())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = now()
        RETURNING 1
      ),
      safe AS (
        INSERT INTO worker_state (key, value, updated_at)
        VALUES ('safe_baseline_state', ${safeJson}, now())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = now()
        RETURNING 1
      )
      SELECT
        (SELECT count(*) FROM inc) AS incremental_written,
        (SELECT count(*) FROM base) AS baseline_written,
        (SELECT count(*) FROM safe) AS safe_written
    `;
  } catch (e) {
    logger.debug(
      { component: "live-supervisor", error: String(e) },
      "incremental state persistence failed (soft)",
    );
  }
}

/** Restore baseline adaptive state from worker_state (soft on miss/corrupt). */
export async function restoreSafeBaselineState(sql: Sql): Promise<boolean> {
  try {
    const rows = await sql<{ value: string }>`
      SELECT value FROM worker_state WHERE key = 'safe_baseline_state' LIMIT 1
    `;
    if (!rows.length || !rows[0]?.value) return false;
    const parsed = JSON.parse(rows[0].value);
    const { globalSafeBaseline } = await import(
      "@/lib/prediction/lifecycle/safe-baseline-controller"
    );
    globalSafeBaseline.importState(parsed);
    logger.info(
      { component: "live-supervisor", mode: globalSafeBaseline.getMode(), n: globalSafeBaseline.snapshot().n },
      "safe baseline state restored",
    );
    return true;
  } catch (e) {
    logger.debug(
      { component: "live-supervisor", error: String(e) },
      "safe baseline state restore failed (soft)",
    );
    return false;
  }
}

export async function restoreBaselineAdaptiveState(sql: Sql): Promise<boolean> {
  try {
    const rows = await sql<{ value: string }>`
      SELECT value FROM worker_state WHERE key = 'baseline_adaptive_state' LIMIT 1
    `;
    if (!rows.length || !rows[0]?.value) return false;
    const parsed = JSON.parse(rows[0].value);
    const { globalBaselineModel } = await import(
      "@/lib/prediction/models/baseline-model"
    );
    globalBaselineModel.importState(parsed);
    logger.info(
      {
        component: "live-supervisor",
        outcomeCount: globalBaselineModel.getAdaptiveState().outcomeCount,
        rollingAbsError: globalBaselineModel.getAdaptiveState().rollingAbsError,
      },
      "baseline adaptive state restored",
    );
    return true;
  } catch (e) {
    logger.debug(
      { component: "live-supervisor", error: String(e) },
      "baseline adaptive state restore failed (soft)",
    );
    return false;
  }
}
