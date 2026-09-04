import { randomUUID } from "node:crypto";
import type { Sql } from "@/lib/db";
import { getSql } from "@/lib/db";
import { fetchCrashHistory, fetchOnlinePlayers, type FetchedRound } from "@/lib/crash/fetch-bc";
import { insertNewRounds } from "@/lib/crash/ingest";
import type { CrashRound } from "@/lib/crash/types";
import {
  generateAndQueuePrediction,
  validateAgainstNewRounds,
  getTodayStats,
  getPendingStatus,
  getDailyTarget,
} from "./service.ts";
import {
  getConfiguredChatIds,
  telegramConfigured,
} from "@/lib/notifications/telegram";
import {
  processOutbox,
  createPredictionNotification,
  createValidationNotification,
  getOutboxStats,
} from "@/lib/notifications/outbox";
import { getLogger } from "@/lib/observability/logger";
import { bcGameSocket, startEventDrivenPipeline, stopEventDrivenPipeline } from "./events/game-event-handlers";
import { withGenerationLock } from "./generation-lock";

const logger = getLogger("prediction-worker");

const POLL_INTERVAL_MS = Math.max(2000, Number(process.env.PREDICTION_POLL_MS ?? 3000));
const LOCK_TTL_SEC = Number(process.env.PREDICTION_LOCK_TTL_SEC ?? 60);
const FETCH_PAGES = Number(process.env.PREDICTION_FETCH_PAGES ?? 2);
const LOCK_KEY = "prediction_worker";

const STATE = {
  LAST_SYNC_AT: "last_sync_at",
  LAST_SYNC_OK: "last_sync_ok",
  LAST_ERROR: "last_error",
  LAST_FETCH_COUNT: "last_fetch_count",
  LAST_INSERTED_COUNT: "last_inserted_count",
  LAST_ONLINE_PLAYERS: "last_online_players",
  LAST_SEEN_GAME_ID: "last_seen_game_id",
  CYCLES_TOTAL: "cycles_total",
  TELEGRAM_ENABLED: "telegram_enabled",
  TELEGRAM_LAST_SENT_AT: "telegram_last_sent_at",
  TELEGRAM_LAST_ERROR: "telegram_last_error",
} as const;

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

export interface WorkerCycleResult {
  ran: boolean;
  fetched: number;
  inserted: number;
  onlinePlayers: number | null;
  resolved: number;
  generated: string | null;
  error: string | null;
  ok: boolean;
}

async function acquireLock(sql: Sql, ownerId: string): Promise<boolean> {
  const ttl = LOCK_TTL_SEC;
  await sql.query(
    `insert into worker_locks (lock_key, owner_id, acquired_at, expires_at, heartbeat_at)
       values ($1, $2, now(), now() + make_interval(secs => $3), now())
     on conflict (lock_key) do update
       set owner_id = $2,
           acquired_at = now(),
           expires_at = now() + make_interval(secs => $3),
           heartbeat_at = now()
       where worker_locks.expires_at < now()`,
    [LOCK_KEY, ownerId, ttl],
  );
  const rows = await sql<{ mine: boolean }>`
    select owner_id = ${ownerId} as mine from worker_locks where lock_key = ${LOCK_KEY}
  `;
  return rows.length > 0 && rows[0]!.mine;
}

async function heartbeat(sql: Sql, ownerId: string): Promise<boolean> {
  const ttl = LOCK_TTL_SEC;
  try {
    await sql.query(
      `update worker_locks
          set expires_at = now() + make_interval(secs => $3), heartbeat_at = now()
        where lock_key = $1 and owner_id = $2`,
      [LOCK_KEY, ownerId, ttl],
    );
    const rows = await sql<{ owner_id: string }>`
      select owner_id from worker_locks where lock_key = ${LOCK_KEY}
    `;
    return rows.length > 0 && rows[0]!.owner_id === ownerId;
  } catch (e) {
    console.warn("[worker] heartbeat failed:", (e as Error).message);
    return true;
  }
}

async function releaseLock(sql: Sql, ownerId: string): Promise<void> {
  await sql.query(
    `delete from worker_locks where lock_key = $1 and owner_id = $2`,
    [LOCK_KEY, ownerId],
  );
}

async function setState(sql: Sql, key: string, value: string): Promise<void> {
  await sql.query(
    `insert into worker_state (key, value) values ($1, $2)
     on conflict (key) do update set value = excluded.value, updated_at = now()`,
    [key, value],
  );
}

async function setStateNum(sql: Sql, key: string, value: number): Promise<void> {
  await setState(sql, key, String(value));
}

async function incrementCycles(sql: Sql): Promise<void> {
  await sql.query(
    `insert into worker_state (key, value) values ('cycles_total', '0')
     on conflict (key) do nothing`,
  );
  await sql.query(
    `update worker_state set value = (value::int + 1)::text, updated_at = now()
     where key = 'cycles_total'`,
  );
}

async function readStateMap(sql: Sql): Promise<Map<string, string>> {
  const rows = await sql<{ key: string; value: string | null }>`
    select key, value from worker_state
  `;
  const m = new Map<string, string>();
  for (const r of rows) if (r.value != null) m.set(r.key, r.value);
  return m;
}

export async function runWorkerCycle(): Promise<WorkerCycleResult> {
  const sql = await getSql();
  const ownerId = randomUUID();
  const acquired = await acquireLock(sql, ownerId);
  if (!acquired) {
    return { ran: false, fetched: 0, inserted: 0, onlinePlayers: null, resolved: 0, generated: null, error: null, ok: true };
  }
  try {
    return await runCycleWork(sql);
  } finally {
    await releaseLock(sql, ownerId);
  }
}

async function runCycleWork(sql: Sql): Promise<WorkerCycleResult> {
  const result: WorkerCycleResult = {
    ran: true, fetched: 0, inserted: 0, onlinePlayers: null, resolved: 0, generated: null, error: null, ok: true,
  };
  let rounds: FetchedRound[] = [];
  let insertedRounds: CrashRound[] = [];
  let onlinePlayers: number | null = null;
  let firstError: string | null = null;
  const fail = (msg: string): void => {
    if (firstError == null) firstError = msg;
    result.error = msg;
    result.ok = false;
  };

  try {
    [rounds, onlinePlayers] = await Promise.all([
      fetchCrashHistory(FETCH_PAGES),
      fetchOnlinePlayers().catch(() => null as number | null),
    ]);
    result.fetched = rounds.length;
    result.onlinePlayers = onlinePlayers;
  } catch (e: unknown) {
    fail((e as Error)?.message ?? String(e));
    rounds = [];
    onlinePlayers = null;
    result.fetched = 0;
  }

  if (firstError == null) {
    try {
      const ins = await insertNewRounds(rounds);
      result.inserted = ins.inserted;
      insertedRounds = ins.rounds;
    } catch (e: unknown) {
      fail((e as Error)?.message ?? String(e));
      insertedRounds = [];
      result.inserted = 0;
    }
  } else {
    insertedRounds = [];
    result.inserted = 0;
  }

  if (firstError == null) {
    const outcome =
      insertedRounds.length > 0
        ? await validateAgainstNewRounds(insertedRounds).catch((e: unknown) => {
            fail((e as Error)?.message ?? String(e));
            return { resolved: 0, insertedRounds: 0, pairs: [] };
          })
        : { resolved: 0, insertedRounds: 0, pairs: [] as Array<never> };

    const [today, pending] = await Promise.all([getTodayStats(), getPendingStatus()]);
    result.resolved = outcome.resolved;

    for (const pair of outcome.pairs) {
      if (pair.targetMultiplier === 0) continue;
      void createValidationNotification(sql, {
        predictionId: pair.predictionId,
        gameId: pair.gameId,
        targetMultiplier: pair.targetMultiplier,
        actualMultiplier: pair.actualMultiplier,
        probability: pair.probability,
        result: pair.result,
        resolvedAt: pair.resolvedAt,
      }).catch((e) => {
        logger.error({ component: "PredictionWorker", error: e }, "Failed to queue validation notification");
      });
    }

    if (firstError == null) {
      if (today.remaining <= 0) {
        console.log(`[worker] skip generate: daily target met (resolvedToday=${today.total} remaining=0)`);
      } else if (pending.hasPending) {
        console.log(`[worker] skip generate: pending already exists (count=${pending.pendingCount})`);
      } else {
        try {
          const queued = await withGenerationLock(() => generateAndQueuePrediction(sql)).catch(
            (e: unknown) => {
              console.error("[worker] generateAndQueuePrediction threw:", (e as Error)?.message ?? e);
              return null;
            },
          );
          if (queued) {
            result.generated = queued.signal.predictionId;
            console.log(`[worker] generated prediction=${queued.signal.predictionId} target=${queued.signal.target ?? 1.3}`);
            try {
              await createPredictionNotification(sql, {
                predictionId: queued.signal.predictionId,
                targetMultiplier: Number(queued.signal.target ?? 1.3),
                probability: queued.signal.probability,
                confidence: queued.signal.confidence,
                regimeName: queued.signal.regimeId ?? null,
                lastRoundMultiplier: queued.lastRound?.multiplier ?? null,
                generatedAt: new Date().toISOString(),
              });
            } catch (e) {
              console.error("[worker] createPredictionNotification failed:", (e as Error)?.message ?? e);
            }
          } else {
            console.log("[worker] skip generate: generateAndQueuePrediction returned null");
          }
        } catch (e: unknown) {
          fail((e as Error)?.message ?? String(e));
        }
      }
    }
  }

  if (firstError) {
    logger.error({ component: "PredictionWorker", error: firstError }, "cycle error");
  }

  const nowIso = new Date().toISOString();
  const latest = insertedRounds[0] ?? rounds[0];
  try {
    await setState(sql, STATE.LAST_SYNC_AT, nowIso);
    await setState(sql, STATE.LAST_SYNC_OK, result.ok ? "1" : "0");
    await setState(sql, STATE.LAST_ERROR, result.ok ? "" : result.error ?? firstError ?? "");
    await setStateNum(sql, STATE.LAST_FETCH_COUNT, result.fetched);
    await setStateNum(sql, STATE.LAST_INSERTED_COUNT, result.inserted);
    await setState(sql, STATE.LAST_ONLINE_PLAYERS, onlinePlayers == null ? "" : String(onlinePlayers));
    if (latest) await setState(sql, STATE.LAST_SEEN_GAME_ID, latest.gameId);
    await setState(sql, STATE.TELEGRAM_ENABLED, telegramConfigured() ? "1" : "0");
    await incrementCycles(sql);
  } catch (e: unknown) {
    logger.error({ component: "PredictionWorker", error: e }, "state-write failed");
  }

  return result;
}

export async function getWorkerStatus(): Promise<WorkerStatus> {
  const sql = await getSql();
  const [lockRows, state, today, pending, target] = await Promise.all([
    sql<{ owner_id: string; expires_at: string; heartbeat_at: string }>`
      select owner_id, expires_at, heartbeat_at from worker_locks where lock_key = ${LOCK_KEY}
    `,
    readStateMap(sql),
    getTodayStats(),
    getPendingStatus(),
    getDailyTarget(),
  ]);
  const lock = lockRows[0];
  const now = Date.now();
  const running = !!lock && new Date(lock.expires_at).getTime() > now;
  const rawPlayers = state.get(STATE.LAST_ONLINE_PLAYERS);
  const lastOnlinePlayers = rawPlayers != null && rawPlayers !== "" ? Number(rawPlayers) : null;
  return {
    running,
    ownerId: lock?.owner_id ?? null,
    expiresAt: lock?.expires_at ?? null,
    heartbeatAt: lock?.heartbeat_at ?? null,
    lastSyncAt: state.get(STATE.LAST_SYNC_AT) ?? null,
    lastSyncOk: state.get(STATE.LAST_SYNC_OK) === "1",
    lastError: state.get(STATE.LAST_ERROR) ?? null,
    lastFetchCount: Number(state.get(STATE.LAST_FETCH_COUNT) ?? 0) || 0,
    lastInsertedCount: Number(state.get(STATE.LAST_INSERTED_COUNT) ?? 0) || 0,
    lastOnlinePlayers,
    lastSeenGameId: state.get(STATE.LAST_SEEN_GAME_ID) ?? null,
    cyclesTotal: Number(state.get(STATE.CYCLES_TOTAL) ?? 0) || 0,
    pendingCount: pending.pendingCount,
    resolvedToday: today.total,
    dailyTarget: target.dailyTarget,
    remainingToday: today.remaining,
    telegramEnabled: state.get(STATE.TELEGRAM_ENABLED) === "1",
    telegramLastSentAt: state.get(STATE.TELEGRAM_LAST_SENT_AT) ?? null,
    telegramLastError: state.get(STATE.TELEGRAM_LAST_ERROR) ?? null,
  };
}

type WorkerHandle = {
  running: boolean;
  ownerId: string;
  sleepTimer: ReturnType<typeof setTimeout> | null;
  hbTimer?: ReturnType<typeof setInterval> | null;
};

const globalRef = globalThis as typeof globalThis & {
  __predictionWorkerHandle__?: WorkerHandle;
};

function getHandle(): WorkerHandle {
  if (!globalRef.__predictionWorkerHandle__) {
    globalRef.__predictionWorkerHandle__ = { running: false, ownerId: "", sleepTimer: null };
  }
  return globalRef.__predictionWorkerHandle__;
}

export function startWorker(): WorkerHandle {
  if (typeof window !== "undefined") throw new Error("startWorker() is server-only");
  const handle = getHandle();
  if (handle.running) return handle;
  handle.running = true;
  handle.ownerId = randomUUID();
  const chatIds = getConfiguredChatIds();
  const bootMsg = `[worker] starting telegram=${telegramConfigured() ? "enabled" : "disabled (no env)"} telegramChatCount=${chatIds.length} pollIntervalMs=${POLL_INTERVAL_MS}`;
  if (telegramConfigured()) console.log(bootMsg);
  else console.warn(bootMsg);

  try {
    void startEventDrivenPipeline();
  } catch (e) {
    logger.warn({ component: "PredictionWorker", error: e }, "event pipeline start failed");
  }

  // Independent heartbeat so a long sync prediction cannot starve the lock TTL.
  const heartbeatEveryMs = Math.min(15_000, Math.max(5_000, Math.floor(LOCK_TTL_SEC * 1000 / 3)));
  handle.hbTimer = setInterval(() => {
    if (!handle.running) return;
    void (async () => {
      try {
        const sql = await getSql();
        await heartbeat(sql, handle.ownerId);
      } catch (e) {
        console.warn("[worker] independent heartbeat failed:", (e as Error)?.message ?? e);
      }
    })();
  }, heartbeatEveryMs);
  if (!process.env.DATABASE_URL?.trim()) handle.hbTimer.unref?.();

  void run(handle);
  return handle;
}

export async function stopWorker(): Promise<void> {
  const handle = getHandle();
  handle.running = false;
  if (handle.sleepTimer) {
    clearTimeout(handle.sleepTimer);
    handle.sleepTimer = null;
  }
  if (handle.hbTimer) {
    clearInterval(handle.hbTimer);
    handle.hbTimer = null;
  }
  try {
    const sql = await getSql();
    await releaseLock(sql, handle.ownerId);
  } catch { /* ignore */ }
  try {
    await stopEventDrivenPipeline();
  } catch (error) {
    logger.warn({ component: "PredictionWorker", error }, "Error stopping event-driven pipeline");
  }
  logger.info({ component: "PredictionWorker" }, "worker stopped");
}

function sleep(handle: WorkerHandle, ms: number): Promise<void> {
  return new Promise((res) => {
    handle.sleepTimer = setTimeout(res, ms);
    if (!process.env.DATABASE_URL?.trim()) handle.sleepTimer.unref?.();
  });
}

async function run(handle: WorkerHandle): Promise<void> {
  const sql = await getSql();
  let ours = await acquireLock(sql, handle.ownerId);
  if (!ours) {
    console.warn("[worker] lock held by another instance; backing off");
    await sleep(handle, Math.max(POLL_INTERVAL_MS, LOCK_TTL_SEC * 1000));
    if (handle.running) void run(handle);
    return;
  }

  try {
    while (handle.running) {
      try {
        const cycle = await runCycleWork(sql);
        console.log(
          `[worker] cycle ok=${cycle.ok} fetched=${cycle.fetched} inserted=${cycle.inserted} resolved=${cycle.resolved} generated=${cycle.generated ?? "-"} error=${cycle.error ?? "-"}`,
        );
      } catch (e: unknown) {
        console.error("[worker] cycle threw:", (e as Error)?.message ?? e);
      }

      try {
        const out = await processOutbox(sql);
        if (out.processed > 0) {
          console.log(
            `[worker] outbox processed=${out.processed} delivered=${out.delivered} failed=${out.failed} deadLetter=${out.deadLetter}`,
          );
        }
      } catch (e: unknown) {
        console.error("[worker] outbox threw:", (e as Error)?.message ?? e);
      }

      ours = await heartbeat(sql, handle.ownerId);
      if (!ours) {
        console.warn("[worker] lock lost; yielding");
        break;
      }
      await sleep(handle, POLL_INTERVAL_MS);
    }
  } finally {
    await releaseLock(sql, handle.ownerId);
    if (handle.running) void run(handle);
  }
}

export {
  bcGameSocket,
  startEventDrivenPipeline,
  stopEventDrivenPipeline,
  processOutbox,
  createPredictionNotification,
  createValidationNotification,
  getOutboxStats,
};
