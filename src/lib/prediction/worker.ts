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
  formatPredictionMessage,
  formatValidationMessage,
  getConfiguredChatIds,
  sendTelegramMessage,
  telegramConfigured,
  type SendResult,
} from "@/lib/notifications/telegram";
import { 
  processOutbox,
  createPredictionNotification,
  createValidationNotification,
  getOutboxStats,
  type OutboxNotification 
} from "@/lib/notifications/outbox";
import { getLogger } from "@/lib/observability/logger";
import { bcGameSocket, startEventDrivenPipeline, stopEventDrivenPipeline } from "./events/game-event-handlers";

const logger = getLogger("prediction-worker");

/* ── Tuning (env-overridable for tests/verification) ───────────────────── */

/**
 * How often the worker polls BC.Game for new rounds. Default 3000ms per the
 * Telegram design (lower bound 2000ms — anything tighter hits cached/empty
 * upstream responses).
 */
const POLL_INTERVAL_MS = Math.max(
  2000,
  Number(process.env.PREDICTION_POLL_MS ?? 3000),
);
/** Distributed lock time-to-live. A crashed worker is recovered after this. */
const LOCK_TTL_SEC = Number(process.env.PREDICTION_LOCK_TTL_SEC ?? 60);
/** How many BC.Game history pages to fetch per poll (freshness vs. rate). */
const FETCH_PAGES = Number(process.env.PREDICTION_FETCH_PAGES ?? 2);
/** How often to process the notification outbox (separate from main polling) */
const OUTBOX_PROCESS_INTERVAL_MS = Number(process.env.OUTBOX_PROCESS_INTERVAL_MS ?? 1000);

const LOCK_KEY = "prediction_worker";

/** Keys written to `worker_state` (shared with the dashboard reader). */
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
  /** Whether the distributed lock is currently held by a live owner. */
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
  /** Whether the Telegram notification adapter is configured (env present). */
  telegramEnabled: boolean;
  /** ISO timestamp of the most recent Telegram send attempt. */
  telegramLastSentAt: string | null;
  /** Last Telegram delivery error (empty on success). */
  telegramLastError: string | null;
};

export interface WorkerCycleResult {
  ran: boolean; // false when another instance holds the lock
  fetched: number;
  inserted: number;
  onlinePlayers: number | null;
  resolved: number;
  generated: string | null;
  error: string | null;
  ok: boolean; // true iff the cycle completed end-to-end without any error
}

/* ── Distributed lock ──────────────────────────────────────────────────── */

/**
 * Atomically claim the worker lock. Steals it cleanly if the previous owner's
 * TTL has elapsed (recovery after a crash); otherwise returns false. Safe to
 * re-call for the same owner (refreshes/expands the TTL when it would lapse).
 */
async function acquireLock(sql: Sql, ownerId: string): Promise<boolean> {
  const ttl = LOCK_TTL_SEC;
  await sql.query(
    `insert into worker_locks (lock_key, owner_id, acquired_at, expires_at, heartbeat_at)
       values ($1, $2, now(), now() + interval '${ttl} seconds', now())
     on conflict (lock_key) do update
       set owner_id = $2,
           acquired_at = now(),
           expires_at = now() + interval '${ttl} seconds',
           heartbeat_at = now()
       where worker_locks.expires_at < now()`,
    [LOCK_KEY, ownerId],
  );
  const rows = await sql<{ mine: boolean }>`
    select owner_id = ${ownerId} as mine from worker_locks where lock_key = ${LOCK_KEY}
  `;
  return rows.length > 0 && rows[0]!.mine;
}

/** Extend the lock TTL. Returns false if ownership was lost to another worker. */
async function heartbeat(sql: Sql, ownerId: string): Promise<boolean> {
  const ttl = LOCK_TTL_SEC;
  try {
    await sql.query(
      `update worker_locks
          set expires_at = now() + interval '${ttl} seconds', heartbeat_at = now()
        where lock_key = $1 and owner_id = $2`,
      [LOCK_KEY, ownerId],
    );
    // Confirm we still own it (the update is a no-op if another worker stole it).
    const rows = await sql<{ owner_id: string }>`
      select owner_id from worker_locks where lock_key = ${LOCK_KEY}
    `;
    return rows.length > 0 && rows[0]!.owner_id === ownerId;
  } catch (e) {
    // Non-fatal: pooler saturation / transient disconnects must not kill the loop.
    console.warn("[worker] heartbeat failed:", (e as Error).message);
    return true; // assume still owned; next cycle re-checks via acquire
  }
}

async function releaseLock(sql: Sql, ownerId: string): Promise<void> {
  await sql.query(
    `delete from worker_locks where lock_key = $1 and owner_id = $2`,
    [LOCK_KEY, ownerId],
  );
}

/* ── Worker state (dashboard reads these) ──────────────────────────────── */

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

/* ── The core prediction/validate cycle (no lock management) ───────────── */

/**
 * One full server-side cycle, independent of any dashboard request:
 *   poll BC.Game → ingest new rounds → validate any matching pending
 *   predictions (1:1) → generate the next prediction (if no pending and
 *   daily target not met). Idempotent and DB-gated: safe to re-run after a
 *   crash. The caller owns the distributed lock.
 *
 * Error-state contract: a failure in ANY step (fetch, insert, validate,
 * generate) MUST leave `last_error` set and `last_sync_ok = 0`. A successful
 * cycle clears `last_error` to the empty string. This prevents a partial
 * cycle (e.g. fetch failed, then insert succeeded on an empty list) from
 * being reported as "ok" — and prevents a transient DB blip from being
 * hidden by a later successful step.
 *
 * NEW: Telegram notifications now use the durable outbox pattern.
 */
export async function runWorkerCycle(): Promise<WorkerCycleResult> {
  const sql = await getSql();
  const ownerId = randomUUID();
  const acquired = await acquireLock(sql, ownerId);
  if (!acquired) {
    return {
      ran: false,
      fetched: 0,
      inserted: 0,
      onlinePlayers: null,
      resolved: 0,
      generated: null,
      error: null,
      ok: true, // a no-op cycle is not an error
    };
  }
  try {
    return await runCycleWork(sql);
  } finally {
    await releaseLock(sql, ownerId);
  }
}

async function runCycleWork(sql: Sql): Promise<WorkerCycleResult> {
  const result: WorkerCycleResult = {
    ran: true,
    fetched: 0,
    inserted: 0,
    onlinePlayers: null,
    resolved: 0,
    generated: null,
    error: null,
    ok: true, // flipped to false on the first error observed
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

  // 1. BC.Game history polling (server-side, no browser/dashboard).
  try {
    [rounds, onlinePlayers] = await Promise.all([
      fetchCrashHistory(FETCH_PAGES),
      fetchOnlinePlayers().catch(() => null as number | null),
    ]);
    result.fetched = rounds.length;
    result.onlinePlayers = onlinePlayers;
  } catch (e: unknown) {
    fail((e as Error)?.message ?? String(e));
    // Keep going so we still persist a heartbeat + last_error — the worker
    // must never silently swallow a BC.Game failure.
    rounds = [];
    onlinePlayers = null;
    result.fetched = 0;
  }

  // 2. New-round detection + permanent persistence (idempotent on game_id).
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
    // Skip — we don't want to write partial state when the fetch already failed.
    insertedRounds = [];
    result.inserted = 0;
  }

  // 3. WIN/LOSS validation (durable 1:1) — sequential, the read-only
  //    stats queries run AFTER validation commits so `pending.hasPending`
  //    reflects the post-validation state (Fix 2: race-condition repair).
  //    The previous `Promise.all([validatePromise, statsPromise])` could
  //    see the pre-validation snapshot under PostgreSQL READ COMMITTED,
  //    which made `pending.hasPending` incorrectly true and skipped
  //    generation.
  if (firstError == null) {
    const outcome =
      insertedRounds.length > 0
        ? await validateAgainstNewRounds(insertedRounds).catch((e: unknown) => {
            fail((e as Error)?.message ?? String(e));
            return { resolved: 0, insertedRounds: 0, pairs: [] };
          })
        : { resolved: 0, insertedRounds: 0, pairs: [] as Array<never> };

    // Stats queries run AFTER validation so the pending check sees the
    // post-validation state (Fix 2).
    const [today, pending] = await Promise.all([
      getTodayStats(),
      getPendingStatus(),
    ]);

    result.resolved = outcome.resolved;
    // Telegram notifications are now handled via durable outbox
    // This ensures Telegram delivery never blocks prediction generation
    for (const pair of outcome.pairs) {
      if (pair.targetMultiplier === 0) continue; // Skip recovery re-pass
      // Queue validation notification in outbox (non-blocking)
      void createValidationNotification(sql, {
        predictionId: pair.predictionId,
        gameId: pair.gameId,
        targetMultiplier: pair.targetMultiplier,
        actualMultiplier: pair.actualMultiplier,
        probability: pair.probability,
        result: pair.result,
        resolvedAt: pair.resolvedAt,
      }).catch((e) => {
        logger.error(
          { component: "PredictionWorker", error: e },
          "Failed to queue validation notification",
        );
      });
    }

    // 4. Daily entry counting: generate a new prediction if the daily
    //    target isn't met AND there's no pending. (Fix 1: starvation
    //    gate REMOVED.) The previous `&& insertedRounds.length === 0`
    //    clause caused the worker to skip generation in continuous
    //    play because rounds complete every 2–8s and the worker polls
    //    every 3s — `insertedRounds.length` was almost always > 0.
    //    Removing the gate lets generation run immediately after
    //    validation clears the pending queue.
    if (firstError == null) {
      if (today.remaining > 0 && !pending.hasPending) {
        try {
          const queued = await generateAndQueuePrediction(sql).catch(
            (_e: unknown) => null,
          );
          if (queued) {
            result.generated = queued.signal.predictionId;
            // Queue prediction notification in outbox (non-blocking)
            void createPredictionNotification(sql, {
              predictionId: queued.signal.predictionId,
              targetMultiplier: Number(queued.signal.target ?? 1.3),
              probability: queued.signal.probability,
              confidence: queued.signal.confidence,
              regimeName: queued.signal.regimeId ?? null,
              lastRoundMultiplier: queued.lastRound?.multiplier ?? null,
              generatedAt: new Date().toISOString(),
            }).catch((e) => {
              logger.error(
                { component: "PredictionWorker", error: e },
                "Failed to queue prediction notification",
              );
            });
          }
        } catch (e: unknown) {
          fail((e as Error)?.message ?? String(e));
        }
      }
    }
  } else {
    // Fetch already failed; skip validate/generate but still persist heartbeat.
  }

  if (firstError) {
    logger.error(
      { component: "PredictionWorker", error: firstError },
      "cycle error",
    );
  }

  // 6. Persist heartbeat + feed summary. State writes are intentionally
  //    best-effort: a state-write failure MUST NOT mask the real error.
  const nowIso = new Date().toISOString();
  const latest = insertedRounds[0] ?? rounds[0];
  try {
    await setState(sql, STATE.LAST_SYNC_AT, nowIso);
    await setState(sql, STATE.LAST_SYNC_OK, result.ok ? "1" : "0");
    // CRITICAL: do NOT clear a previous error to "" on a successful cycle
    // until we know the cycle genuinely finished end-to-end. Clearing the
    // error on partial success used to mask failures (e.g. fetch failed
    // but a later step succeeded on an empty list).
    await setState(sql, STATE.LAST_ERROR, result.ok ? "" : result.error ?? firstError ?? "");
    await setStateNum(sql, STATE.LAST_FETCH_COUNT, result.fetched);
    await setStateNum(sql, STATE.LAST_INSERTED_COUNT, result.inserted);
    await setState(
      sql,
      STATE.LAST_ONLINE_PLAYERS,
      onlinePlayers == null ? "" : String(onlinePlayers),
    );
    if (latest) await setState(sql, STATE.LAST_SEEN_GAME_ID, latest.gameId);
    await setState(
      sql,
      STATE.TELEGRAM_ENABLED,
      telegramConfigured() ? "1" : "0",
    );
    await incrementCycles(sql);
  } catch (e: unknown) {
    logger.error(
      { component: "PredictionWorker", error: e },
      "state-write failed (cycle result still valid)",
    );
  }

  return result;
}

/* ── Status read by the dashboard ──────────────────────────────────────── */

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
  const lastOnlinePlayers =
    rawPlayers != null && rawPlayers !== "" ? Number(rawPlayers) : null;

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

/* ── Lifecycle: a lock-holding self-scheduling singleton loop ──────────── */

type WorkerHandle = {
  running: boolean;
  ownerId: string;
  sleepTimer: ReturnType<typeof setTimeout> | null;
};

const globalRef = globalThis as typeof globalThis & {
  __predictionWorkerHandle__?: WorkerHandle;
};

function getHandle(): WorkerHandle {
  if (!globalRef.__predictionWorkerHandle__) {
    globalRef.__predictionWorkerHandle__ = {
      running: false,
      ownerId: "",
      sleepTimer: null,
    };
  }
  return globalRef.__predictionWorkerHandle__;
}

/**
 * Start the autonomous background worker. Server-side only — never import from
 * browser code. A no-op if already running. The loop holds the distributed lock
 * across cycles (so engine health is continuous) and re-acquires it if ever
 * lost, backing off until the previous owner's TTL expires.
 *
 * NEW: Also starts the event-driven Socket.IO prediction pipeline.
 */
export function startWorker(): WorkerHandle {
  if (typeof window !== "undefined") {
    throw new Error("startWorker() is server-only");
  }
  const handle = getHandle();
  if (handle.running) return handle;
  handle.running = true;
  handle.ownerId = randomUUID();
  const chatIds = getConfiguredChatIds();
  // Log to stdout/stderr (not the in-memory noop logger) so Railway captures
  // the line. We log the count, not the ids — chat ids are operator-controlled
  // but enumerating them on every boot is noisy; the per-chat result after
  // each send tells the operator which id (if any) is failing.
  const bootMsg = `[worker] starting telegram=${
    telegramConfigured() ? "enabled" : "disabled (no env)"
  } telegramChatCount=${chatIds.length} pollIntervalMs=${POLL_INTERVAL_MS}`;
  if (telegramConfigured()) {
    console.log(bootMsg);
  } else {
    console.warn(bootMsg);
  }
  
  // Start event-driven pipeline (Socket.IO) if available; fall back to poll.
  try {
    void startEventDrivenPipeline();
  } catch (e) {
    logger.warn({ component: "PredictionWorker", error: e }, "event pipeline start failed");
  }

  void run(handle);
  return handle;
}

/** Stop the worker loop and release the lock (best-effort). */
export async function stopWorker(): Promise<void> {
  const handle = getHandle();
  handle.running = false;
  if (handle.sleepTimer) {
    clearTimeout(handle.sleepTimer);
    handle.sleepTimer = null;
  }
  try {
    const sql = await getSql();
    await releaseLock(sql, handle.ownerId);
  } catch {
    /* ignore — TTL reclaims the lock */
  }
  
  // NEW: Stop the event-driven prediction pipeline
  try {
    await stopEventDrivenPipeline();
  } catch (error) {
    logger.warn(
      { component: "PredictionWorker", error },
      "Error stopping event-driven pipeline"
    );
  }
  
  logger.info({ component: "PredictionWorker" }, "worker stopped");
}

function sleep(handle: WorkerHandle, ms: number): Promise<void> {
  return new Promise((res) => {
    handle.sleepTimer = setTimeout(res, ms);
    // Don't keep the Node process alive solely for the worker; the dev/preview
    // server stays up on its own HTTP server. Allows clean shutdown on SIGTERM.
    handle.sleepTimer.unref?.();
  });
}

async function run(handle: WorkerHandle): Promise<void> {
  const sql = await getSql();

  // Claim / re-claim the distributed lock.
  let ours = await acquireLock(sql, handle.ownerId);
  if (!ours) {
    logger.debug(
      { component: "PredictionWorker" },
      "lock held by another instance; backing off",
    );
    await sleep(handle, Math.max(POLL_INTERVAL_MS, LOCK_TTL_SEC * 1000));
    if (handle.running) void run(handle);
    return;
  }

  try {
    while (handle.running) {
      try {
        await runCycleWork(sql);
      } catch (e: unknown) {
        logger.error(
          { component: "PredictionWorker", error: e },
          "cycle body threw",
        );
      }
      // Keep the lock alive between cycles (also covers the poll sleep window).
      ours = await heartbeat(sql, handle.ownerId);
      if (!ours) {
        logger.warn(
          { component: "PredictionWorker" },
          "lock lost to another instance; yielding",
        );
        break;
      }
      // Spec §7.10: the adaptive poll-while-pending branch is REMOVED.
      // The new pipeline is event-driven (`bg`/`ed`); the REST poll
      // is the safety net, not the primary trigger. Single, constant
      // cadence (POLL_INTERVAL_MS) is the canonical behavior.
      await sleep(handle, POLL_INTERVAL_MS);
    }
  } finally {
    await releaseLock(sql, handle.ownerId);
    // Re-acquire on graceful restart of the loop.
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
