/**
 * Production readiness / liveness probes.
 *
 * `isReadyForLive()` — quick synchronous check used at boot. Returns true
 * when the database is reachable (the only hard dependency for the live
 * prediction pipeline).
 *
 * `getReadinessReport()` — full async report covering DB, worker lock,
 * and outbox depth. Used by /healthz-style endpoints and operator dashboards.
 */

import { getSql, dbSource } from "@/lib/db";
import { getLogger } from "./logger";

const logger = getLogger("readiness");

const LOCK_KEY = "prediction_worker";
/** Consider the worker unhealthy if the lock heartbeat is older than this. */
const LOCK_STALE_MS = Number(process.env.WORKER_LOCK_STALE_MS ?? 120_000);

export type ReadinessReport = {
  ready: boolean;
  live: boolean;
  checks: {
    database: { ok: boolean; source: string; error?: string };
    workerLock: { ok: boolean; ageMs: number | null; ownerId: string | null };
    outbox: { ok: boolean; pending: number; error?: string };
  };
  checkedAt: string;
};

/**
 * Quick liveness predicate. Returns true if a SELECT against the active
 * database backend succeeds. Safe to call repeatedly; the underlying
 * connection pool reuses established sockets.
 */
export async function isReadyForLive(): Promise<boolean> {
  try {
    const sql = await getSql();
    await sql`select 1 as ok`;
    return true;
  } catch (e) {
    logger.warn({ error: String(e) }, "isReadyForLive: db check failed");
    return false;
  }
}

export async function getReadinessReport(): Promise<ReadinessReport> {
  const checkedAt = new Date().toISOString();
  const checks: ReadinessReport["checks"] = {
    database: { ok: false, source: dbSource },
    workerLock: { ok: false, ageMs: null, ownerId: null },
    outbox: { ok: true, pending: 0 },
  };

  try {
    const sql = await getSql();
    await sql`select 1 as ok`;
    checks.database = { ok: true, source: dbSource };

    try {
      const lockRows = await sql<{
        owner_id: string;
        heartbeat_at: string | Date;
        expires_at: string | Date;
      }>`
        select owner_id, heartbeat_at, expires_at
        from worker_locks
        where lock_key = ${LOCK_KEY}
      `;
      const lock = lockRows[0];
      if (!lock) {
        checks.workerLock = { ok: false, ageMs: null, ownerId: null };
      } else {
        const hb =
          lock.heartbeat_at instanceof Date
            ? lock.heartbeat_at.getTime()
            : new Date(lock.heartbeat_at).getTime();
        const ageMs = Date.now() - hb;
        checks.workerLock = {
          ok: ageMs <= LOCK_STALE_MS,
          ageMs,
          ownerId: lock.owner_id,
        };
      }
    } catch (e) {
      checks.workerLock = { ok: false, ageMs: null, ownerId: null };
      logger.warn({ error: e }, "worker lock check failed");
    }

    try {
      const rows = await sql<{ pending: number }>`
        select count(*)::int as pending
        from notification_outbox
        where status = 'pending'
      `;
      checks.outbox = { ok: true, pending: rows[0]?.pending ?? 0 };
    } catch (e) {
      checks.outbox = {
        ok: false,
        pending: 0,
        error: (e as Error)?.message ?? String(e),
      };
    }
  } catch (e) {
    checks.database = {
      ok: false,
      source: dbSource,
      error: (e as Error)?.message ?? String(e),
    };
  }

  const live = checks.database.ok;
  const ready =
    live &&
    (checks.workerLock.ok || checks.workerLock.ownerId === null);

  return { ready, live, checks, checkedAt };
}

/**
 * Backwards-compatible synchronous shim. The old signature was
 * `isReadyForLive(): boolean`; some callers (vite config) want the value
 * immediately. Returns true when DATABASE_URL is configured (operator
 * should run the worker with a real DB). For the async deep check use
 * `await isReadyForLive()`.
 */
export function isReadyForLiveSync(): boolean {
  return typeof process !== "undefined" && Boolean(process.env.DATABASE_URL?.trim());
}
