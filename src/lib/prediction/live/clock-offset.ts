/**
 * DB clock offset synced at boot / heartbeat — avoids SELECT now() on hot path.
 */
import type { Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("clock-offset");

let dbClockOffsetMs = 0;
let lastSyncAt = 0;

export function authoritativeNowMs(): number {
  return Date.now() + dbClockOffsetMs;
}

export function getDbClockOffsetMs(): number {
  return dbClockOffsetMs;
}

export async function syncDbClockOffset(sql: Sql): Promise<number> {
  const t0 = Date.now();
  const rows = await sql<{ now: string | Date }>`SELECT now() AS now`;
  const t1 = Date.now();
  const dbMs = new Date(rows[0]?.now ?? t1).getTime();
  // Mid-RTT estimate
  const localMid = (t0 + t1) / 2;
  dbClockOffsetMs = dbMs - localMid;
  lastSyncAt = t1;
  logger.info({ dbClockOffsetMs, rttMs: t1 - t0 }, "DB clock offset synced");
  return dbClockOffsetMs;
}

export function shouldResyncClock(maxAgeMs = 60_000): boolean {
  return Date.now() - lastSyncAt > maxAgeMs;
}
