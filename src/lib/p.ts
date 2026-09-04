import { createServerFn } from "@tanstack/react-start";
import {
  getDailyTarget,
  setDailyTarget,
  getTodayStats,
  getLifetimeStats,
  getStreaks,
  getRecentValidations,
  getValidationHistory,
  getPendingStatus,
  type ValidationHistoryOpts,
} from "./prediction/service.ts";
import { getSql } from "./db";

export type WorkerStatus = {
  running: boolean;
  ownerId: string | null;
  heartbeatAgeMs: number | null;
  lastError: string | null;
};

async function getWorkerStatus(): Promise<WorkerStatus> {
  try {
    const sql = await getSql();
    const rows = await sql<{ owner_id: string; heartbeat_at: string | Date }>`
      select owner_id, heartbeat_at from worker_locks where lock_key = 'prediction_worker' limit 1
    `;
    if (rows.length === 0) {
      return { running: false, ownerId: null, heartbeatAgeMs: null, lastError: null };
    }
    const hb = rows[0]!.heartbeat_at;
    const hbMs = hb instanceof Date ? hb.getTime() : new Date(String(hb)).getTime();
    const age = Number.isFinite(hbMs) ? Date.now() - hbMs : null;
    const errRows = await sql<{ value: string }>`
      select value from worker_state where key = 'last_error' limit 1
    `;
    return {
      running: age != null && age < 120_000,
      ownerId: rows[0]!.owner_id,
      heartbeatAgeMs: age,
      lastError: errRows[0]?.value ?? null,
    };
  } catch (e) {
    return { running: false, ownerId: null, heartbeatAgeMs: null, lastError: String(e) };
  }
}

export const predictionGetDailyTarget = createServerFn({ method: "GET" }).handler(getDailyTarget);

export const predictionSetDailyTarget = createServerFn({ method: "POST" })
  .validator((data: unknown) => {
    const d = data as { target?: number } | undefined;
    return { target: typeof d?.target === "number" ? d.target : 100 };
  })
  .handler(({ data }) => setDailyTarget(data.target));

export const predictionGetTodayStats = createServerFn({ method: "GET" }).handler(getTodayStats);

export const predictionGetLifetimeStats = createServerFn({ method: "GET" }).handler(getLifetimeStats);

export const predictionGetStreaks = createServerFn({ method: "GET" }).handler(getStreaks);

export const predictionGetRecent = createServerFn({ method: "GET" })
  .validator((data: unknown) => {
    const d = data as { limit?: number } | undefined;
    return { limit: Math.max(1, Math.min(50, typeof d?.limit === "number" ? d.limit : 10)) };
  })
  .handler(({ data }) => getRecentValidations(data.limit));

export const predictionGetHistory = createServerFn({ method: "POST" })
  .validator((data: unknown) => {
    const d = data as ValidationHistoryOpts | undefined;
    return {
      page: Math.max(1, typeof d?.page === "number" ? d.page : 1),
      pageSize: Math.max(1, Math.min(100, typeof d?.pageSize === "number" ? d.pageSize : 20)),
      result: d?.result ?? null,
      fromDate: d?.fromDate ?? null,
      toDate: d?.toDate ?? null,
    };
  })
  .handler(({ data }) =>
    getValidationHistory({
      page: data.page,
      pageSize: data.pageSize,
      result: data.result,
      fromDate: data.fromDate ?? undefined,
      toDate: data.toDate ?? undefined,
    }),
  );

export const predictionGetPending = createServerFn({ method: "GET" }).handler(getPendingStatus);

export const predictionGetWorkerStatus = createServerFn({ method: "GET" }).handler(getWorkerStatus);
export type { WorkerStatus };
