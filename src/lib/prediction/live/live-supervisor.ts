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
import { getSql, getPoolStats, type Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";
import { nativeBcGameSocket } from "@/lib/crash/native-socket-client";
import { getLastSignalAt } from "@/lib/prediction/live/latency-trace";

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
        return sampleProductionInvariants(sql);
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
    this.heartbeatTimer = setInterval(() => {
      this.cycle += 1;
      void this.getSqlFn()
        .then(async (sql) => {
          this.lastDbOkAt = Date.now();
          await heartbeatWorkerLock(sql);
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
    this.warmerTimer = setInterval(() => {
      void this.getSqlFn()
        .then((sql) => sql`SELECT 1`)
        .then(() => {
          this.lastDbOkAt = Date.now();
        })
        .catch(() => undefined);
    }, CONNECTION_WARMER_MS);
    this.warmerTimer.unref?.();
  }

  private startEventLoopLagMonitor(): void {
    if (this.probeTimer) return;
    this.probeTimer = setInterval(() => {
      const start = process.hrtime.bigint();
      setImmediate(() => {
        const lagMs = Number(process.hrtime.bigint() - start) / 1_000_000;
        if (lagMs > 50) {
          logger.warn({ eventLoopLagMs: Math.round(lagMs) }, "Event loop lag detected");
        }
        try {
          (globalThis as { __eventLoopLagMs__?: number }).__eventLoopLagMs__ = lagMs;
        } catch {
          /* ignore */
        }
      });
    }, EVENT_LOOP_PROBE_MS);
    this.probeTimer.unref?.();
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
    // Fix 14: worker_status is now DERIVED, not a bare online string.
    this.lastHeartbeatAt = Date.now();
    const health = this.deriveWorkerHealth();
    this.lifecycle = "PRODUCTION";
    health.lifecycle = this.lifecycle;
    health.process = "alive";
    this.lastHealth = health;
    await sql`
      INSERT INTO worker_state (key, value, updated_at)
      VALUES ('worker_status', ${health.online ? 'online' : 'offline'}, now())
      ON CONFLICT (key) DO UPDATE
      SET value = EXCLUDED.value, updated_at = now()
    `;
    await sql`
      INSERT INTO worker_state (key, value, updated_at)
      VALUES ('worker_health', ${JSON.stringify({ ...health, lastEdAt: health.lastEdAt != null ? new Date(health.lastEdAt).toISOString() : null, lastBgAt: health.lastBgAt != null ? new Date(health.lastBgAt).toISOString() : null, lastSignalAt: health.lastSignalAt != null ? new Date(health.lastSignalAt).toISOString() : null, lastHeartbeatAt: new Date(health.lastHeartbeatAt).toISOString() })}, now())
      ON CONFLICT (key) DO UPDATE
      SET value = EXCLUDED.value, updated_at = now()
    `.catch((e) => {
      logger.debug(
        { component: "live-supervisor", error: String(e) },
        "worker_health persistence failed (soft)",
      );
    });

    // P2.11: Persist incremental state on each heartbeat
    try {
      await persistIncrementalState(sql);
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

export async function heartbeatWorkerLock(sql: Sql): Promise<void> {
  await sql`
    UPDATE worker_locks
    SET heartbeat_at = now(),
        expires_at = now() + (${LOCK_TTL_SECONDS}::int * interval '1 second')
    WHERE lock_key = ${LOCK_KEY} AND owner_id = ${WORKER_ID}
  `;
}

// P2.11: Persist Incremental State
// Save incremental state alongside worker health
export async function persistIncrementalState(sql: Sql): Promise<void> {
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
      { component: "live-supervisor", error: String(e) },
      "incremental state persistence failed (soft)",
    );
  }
}
