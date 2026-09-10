import { createServerFn } from "@tanstack/react-start";
import {
  getDailyTarget,
  setDailyTarget,
  getTodayStats,
  getLifetimeStats,
  getStreaks,
  getRecentValidations,
  getValidationHistory,
  getAllValidationHistory,
  validationRecordsToCsv,
  getPendingStatus,
  getDashboardSnapshot,
  type ValidationHistoryOpts,
  type DashboardSnapshot,
} from "./prediction/service.ts";
import { getSql } from "./db";

export type WorkerStatus = {
  running: boolean;
  ownerId: string | null;
  expiresAt: string | null;
  heartbeatAt: string | null;
  lastSyncAt: string | null;
  lastSyncOk: boolean;
  lastError: string | null;
  lastFetchCount: number;
  lastInsertedCount: number;
  lastOnlinePlayers: number | null;
  lastSeenGameId: string | null;
  cyclesTotal: number;
  pendingCount: number;
  resolvedToday: number;
  dailyTarget: number;
  remainingToday: number;
  telegramEnabled: boolean;
  telegramLastSentAt: string | null;
  telegramLastError: string | null;
  healthKind?: "RUNNING" | "DEGRADED" | "DATABASE_ERROR" | "OFFLINE" | "UNKNOWN";
  pool?: { total: number; idle: number; waiting: number; max: number } | null;
};

async function getWorkerStatus(): Promise<WorkerStatus> {
  // P0: avoid nested Promise.all fan-out (was 5 concurrent pool acquires).
  // Use dashboard snapshot which pins one connection.
  const snap = await getDashboardSnapshot();
  const w = snap.worker;
  return {
    running: w.running,
    ownerId: w.ownerId,
    expiresAt: w.expiresAt,
    heartbeatAt: w.heartbeatAt,
    lastSyncAt: w.lastSyncAt,
    lastSyncOk: w.lastSyncOk,
    lastError: snap.dbOk ? w.lastError : `DATABASE: ${snap.dbError ?? w.lastError}`,
    lastFetchCount: w.lastFetchCount,
    lastInsertedCount: w.lastInsertedCount,
    lastOnlinePlayers: w.lastOnlinePlayers,
    lastSeenGameId: w.lastSeenGameId,
    cyclesTotal: w.cyclesTotal,
    pendingCount: w.pendingCount,
    resolvedToday: w.resolvedToday,
    dailyTarget: w.dailyTarget,
    remainingToday: w.remainingToday,
    telegramEnabled: w.telegramEnabled,
    telegramLastSentAt: w.telegramLastSentAt,
    telegramLastError: w.telegramLastError,
    healthKind: w.healthKind,
    pool: w.pool,
  };
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

/**
 * Full history export (all pages) for dashboard download.
 * Returns CSV text plus metadata. Caps at HISTORY_EXPORT_MAX rows.
 */
export const predictionExportHistory = createServerFn({ method: "POST" })
  .validator((data: unknown) => {
    const d = data as {
      result?: "WIN" | "LOSS" | null;
      fromDate?: string | null;
      toDate?: string | null;
      format?: "csv" | "json";
    } | undefined;
    const result =
      d?.result === "WIN" || d?.result === "LOSS" ? d.result : null;
    return {
      result,
      fromDate: typeof d?.fromDate === "string" ? d.fromDate : null,
      toDate: typeof d?.toDate === "string" ? d.toDate : null,
      format: d?.format === "json" ? ("json" as const) : ("csv" as const),
    };
  })
  .handler(async ({ data }) => {
    const { records, total, truncated } = await getAllValidationHistory({
      result: data.result,
      fromDate: data.fromDate ?? undefined,
      toDate: data.toDate ?? undefined,
    });
    if (data.format === "json") {
      return {
        format: "json" as const,
        total,
        truncated,
        count: records.length,
        records,
        body: JSON.stringify(records, null, 2),
      };
    }
    const body = validationRecordsToCsv(records);
    return {
      format: "csv" as const,
      total,
      truncated,
      count: records.length,
      records: null,
      body,
    };
  });

export const predictionGetPending = createServerFn({ method: "GET" }).handler(getPendingStatus);

export const predictionGetWorkerStatus = createServerFn({ method: "GET" }).handler(getWorkerStatus);

export const predictionGetDashboardSnapshot = createServerFn({ method: "GET" }).handler(
  getDashboardSnapshot,
);




/** §6.2 Per-model performance for dashboard (read-only). */
export type ModelPerformanceRow = {
  name: string;
  ewmaLogLoss: number;
  ewmaBrier: number;
  count: number;
  recentWinRate: number | null;
  suppressed: boolean;
};

export const predictionGetModelPerformance = createServerFn({ method: "GET" }).handler(
  async (): Promise<ModelPerformanceRow[]> => {
    try {
      const { globalModelPerformance } = await import("./prediction/ensemble/model-performance.ts");
      const all = globalModelPerformance.all();
      const rows: ModelPerformanceRow[] = [];
      for (const [name, perf] of all.entries()) {
        rows.push({
          name,
          ewmaLogLoss: perf.ewmaLogLoss,
          ewmaBrier: perf.ewmaBrier,
          count: perf.count,
          recentWinRate:
            perf.recentTotal > 0 ? perf.recentCorrect / perf.recentTotal : null,
          suppressed: perf.ewmaBrier > 0.3 && perf.count > 50,
        });
      }
      rows.sort((a, b) => a.ewmaBrier - b.ewmaBrier);
      return rows;
    } catch {
      return [];
    }
  },
);
