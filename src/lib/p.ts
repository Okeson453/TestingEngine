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
};

async function getWorkerStatus(): Promise<WorkerStatus> {
  const sql = await getSql();
  const empty: WorkerStatus = {
    running: false,
    ownerId: null,
    expiresAt: null,
    heartbeatAt: null,
    lastSyncAt: null,
    lastSyncOk: false,
    lastError: null,
    lastFetchCount: 0,
    lastInsertedCount: 0,
    lastOnlinePlayers: null,
    lastSeenGameId: null,
    cyclesTotal: 0,
    pendingCount: 0,
    resolvedToday: 0,
    dailyTarget: 100,
    remainingToday: 100,
    telegramEnabled: false,
    telegramLastSentAt: null,
    telegramLastError: null,
  };
  try {
    const [lockRows, stateRows, today, pending, target] = await Promise.all([
      sql<{ owner_id: string; expires_at: string | Date; heartbeat_at: string | Date }>`
        select owner_id, expires_at, heartbeat_at from worker_locks
        where lock_key = 'prediction_worker' limit 1
      `,
      sql<{ key: string; value: string }>`select key, value from worker_state`,
      getTodayStats(),
      getPendingStatus(),
      getDailyTarget(),
    ]);
    const state = new Map(stateRows.map((r) => [r.key, r.value]));
    const lock = lockRows[0];
    const now = Date.now();
    const expiresAt = lock?.expires_at
      ? lock.expires_at instanceof Date
        ? lock.expires_at.toISOString()
        : String(lock.expires_at)
      : null;
    const heartbeatAt = lock?.heartbeat_at
      ? lock.heartbeat_at instanceof Date
        ? lock.heartbeat_at.toISOString()
        : String(lock.heartbeat_at)
      : null;
    const running =
      !!lock &&
      expiresAt != null &&
      new Date(expiresAt).getTime() > now;
    const rawPlayers = state.get("last_online_players");
    return {
      running,
      ownerId: lock?.owner_id ?? null,
      expiresAt,
      heartbeatAt,
      lastSyncAt: state.get("last_sync_at") ?? null,
      lastSyncOk: state.get("last_sync_ok") === "1",
      lastError: state.get("last_error") ?? null,
      lastFetchCount: Number(state.get("last_fetch_count") ?? 0) || 0,
      lastInsertedCount: Number(state.get("last_inserted_count") ?? 0) || 0,
      lastOnlinePlayers:
        rawPlayers != null && rawPlayers !== "" ? Number(rawPlayers) : null,
      lastSeenGameId: state.get("last_seen_game_id") ?? null,
      cyclesTotal: Number(state.get("cycles_total") ?? 0) || 0,
      pendingCount: pending.pendingCount,
      resolvedToday: today.total,
      dailyTarget: target.dailyTarget,
      remainingToday: today.remaining,
      telegramEnabled: state.get("telegram_enabled") === "1",
      telegramLastSentAt: state.get("telegram_last_sent_at") ?? null,
      telegramLastError: state.get("telegram_last_error") ?? null,
    };
  } catch (e) {
    return { ...empty, lastError: String(e) };
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

