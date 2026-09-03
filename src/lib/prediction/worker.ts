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
/** Backoff poll used while a prediction is pending — see `run()`. */
const PENDING_POLL_INTERVAL_MS = Math.max(
  2000,
  Number(process.env.PREDICTION_PENDING_POLL_MS ?? 10_000),
);
/** Distributed lock time-to-live. A crashed worker is recovered after this. */
const LOCK_TTL_SEC = Number(process.env.PREDICTION_LOCK_TTL_SEC ?? 60);
/** How many BC.Game history pages to fetch per poll (freshness vs. rate). */
const FETCH_PAGES = Number(process.env.PREDICTION_FETCH_PAGES ?? 2);

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
  // NEW: Socket.IO connection state
  SOCKET_CONNECTED: "socket_connected",
  SOCKET_LAST_CONNECTED_AT: "socket_last_connected_at",
  SOCKET_LAST_ERROR: "socket_last_error",
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
  // NEW: Socket.IO connection status
  socketConnected: boolean;
  socketLastConnectedAt: string | null;
  socketLastError: string | null;
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

/* ── Telegram notification fan-out (fire-and-forget) ─────────────────── */

/**
 * Fire-and-forget Telegram send. The cycle never awaits this. Each configured
 * chat is an independent destination (per-chat send, per-chat AbortController);
 * the array of results is collapsed to a single error string for the dashboard
 * (first non-ok wins; empty string when every destination succeeded). The
 * cycle never throws, never blocks, never corrupts the worker.
 */
function fireTelegram(text: string, sql: Sql): void {
  const sentAt = new Date().toISOString();
  void sendTelegramMessage(text)
    .then((results: SendResult[]) => {
      // Per-chat result log to stdout (Railway captures it) so the operator
      // can see in the deploy logs which destinations succeeded and which
      // failed. The chatId is the operator's own value (not a secret); we
      // log it so a failing destination is immediately identifiable.
      // Successes are debug-noise and stay in the in-memory noop logger.
      for (const r of results) {
        if (!r.ok) {
          console.warn(
            `[worker] telegram send failed chatId=${r.chatId || "<unset>"} status=${r.status} error=${r.error ?? "unknown_error"}`,
          );
        }
      }
      const errorSummary = results
        .filter((r) => !r.ok)
        .map((r) => `${r.chatId || "<unset>"}:${r.error ?? "unknown_error"}`)
        .join(";");
      const summary: SendResult = {
        ok: results.every((r) => r.ok),
        status: results.find((r) => !r.ok)?.status ?? results[0]?.status ?? 0,
        error: errorSummary || undefined,
        chatId: results.map((r) => r.chatId).filter(Boolean).join(","),
      };
      void recordTelegramResult(sql, sentAt, summary).catch((e: unknown) => {
        logger.warn(
          { component: "PredictionWorker", error: e },
          "telegram state-write failed",
        );
      });
    })
    .catch((e: unknown) => {
      // Defensive — sendTelegramMessage never rejects, so this branch is for
      // programmer-error only (e.g. a bad formatter). Keep the worker alive.
      console.warn(`[worker] telegram fan-out rejected error=${String(e)}`);
    });
}

/** Best-effort write of the last Telegram delivery outcome. */
async function recordTelegramResult(
  sql: Sql,
  sentAt: string,
  res: SendResult,
): Promise<void> {
  try {
    await setState(sql, STATE.TELEGRAM_LAST_SENT_AT, sentAt);
    await setState(
      sql,
      STATE.TELEGRAM_LAST_ERROR,
      res.ok ? "" : (res.error ?? "unknown_error"),
    );
  } catch (e: unknown) {
    logger.warn(
      { component: "PredictionWorker", error: e },
      "telegram state-write failed",
    );
  }
}

/* ── Socket.IO state management ──────────────────────────────────────── */

/**
 * Update Socket.IO connection state in worker state
 */
async function updateSocketState(sql: Sql): Promise<void> {
  const state = bcGameSocket.getState();
  try {
    await setState(sql, STATE.SOCKET_CONNECTED, state.status === "connected" ? "1" : "0");
    await setState(sql, STATE.SOCKET_LAST_CONNECTED_AT, state.lastConnectedAt ?? "");
    await setState(sql, STATE.SOCKET_LAST_ERROR, state.lastError ?? "");
  } catch (e: unknown) {
    logger.warn(
      { component: "PredictionWorker", error: e },
      "socket state-write failed",
    );
  }
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
 * NEW: This now works alongside the event-driven Socket.IO prediction pipeline.
 * The REST polling path remains as a reconciliation and recovery safety net.
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
  // This is now the FALLBACK path - the primary prediction trigger is Socket.IO
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

  // 3+4. WIN/LOSS validation (durable 1:1) and daily-stats/pending read
  //     can run concurrently — the read-only stats queries are independent of
  //     the validate step (which itself short-circuits when
  //     `insertedRounds.length === 0`, the common case). This is the safe
  //     Tier-1 pipeline parallelization from design §10.1.
  if (firstError == null) {
    const validatePromise =
      insertedRounds.length > 0
        ? validateAgainstNewRounds(insertedRounds).catch((e: unknown) => {
            fail((e as Error)?.message ?? String(e));
            return { resolved: 0, insertedRounds: 0, pairs: [] };
          })
        : Promise.resolve({
            resolved: 0,
            insertedRounds: 0,
            pairs: [] as Array<never>,
          });
    const statsPromise = Promise.all([getTodayStats(), getPendingStatus()]);
    const [outcome, [today, pending]] = await Promise.all([
      validatePromise,
      statsPromise,
    ]);
    result.resolved = outcome.resolved;
    // Telegram fan-out for each newly-resolved pair. targetMultiplier === 0
    // is the recovery re-pass sentinel (the row was validated by an earlier
    // worker); we skip notification in that case to avoid noise on restart.
    for (const pair of outcome.pairs) {
      if (pair.targetMultiplier === 0) continue;
      if (!telegramConfigured()) continue;
      fireTelegram(
        formatValidationMessage({
          predictionId: pair.predictionId,
          gameId: pair.gameId,
          targetMultiplier: pair.targetMultiplier,
          actualMultiplier: pair.actualMultiplier,
          probability: pair.probability,
          result: pair.result,
          resolvedAt: pair.resolvedAt,
        }),
        sql,
      );
    }

    // 4. Daily entry counting: only generate a new prediction if the daily
    //    target isn't met AND there's no pending AND no unmatched-new-round
    //    waiting to be resolved (otherwise the next cycle will handle it).
    //    
    // NEW: This is now the FALLBACK prediction generation path.
    // The primary path is the Socket.IO event-driven approach via bg events.
    // The REST polling path must NEVER become the primary prediction trigger
    // and must NEVER generate predictions from already-settled rounds.
    if (firstError == null) {
      try {
        if (today.remaining > 0 && !pending.hasPending && insertedRounds.length === 0) {
          const queued = await generateAndQueuePrediction().catch((_e: unknown) => null);
          if (queued) {
            result.generated = queued.signal.predictionId;
            if (telegramConfigured()) {
              const signal = queued.signal;
              fireTelegram(
                formatPredictionMessage({
                  predictionId: signal.predictionId,
                  targetMultiplier: Number(signal.target ?? 1.3),
                  probability: signal.probability,
                  confidence: signal.confidence,
                  regimeName: signal.regimeId ?? null,
                  lastRoundMultiplier: queued.lastRound?.multiplier ?? null,
                  generatedAt: new Date().toISOString(),
                }),
                sql,
              );
            }
          }
        }
      } catch (e: unknown) {
        fail((e as Error)?.message ?? String(e));
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
    
    // NEW: Update Socket.IO connection state
    await updateSocketState(sql);
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

  // NEW: Get Socket.IO connection state
  const socketConnected = state.get(STATE.SOCKET_CONNECTED) === "1";
  const socketLastConnectedAt = state.get(STATE.SOCKET_LAST_CONNECTED_AT) ?? null;
  const socketLastError = state.get(STATE.SOCKET_LAST_ERROR) ?? null;

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
    // NEW: Socket.IO state
    socketConnected,
    socketLastConnectedAt,
    socketLastError,
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
  } telegramChatCount=${chatIds.length} pollIntervalMs=${POLL_INTERVAL_MS} pendingPollIntervalMs=${PENDING_POLL_INTERVAL_MS}`;
  if (telegramConfigured()) {
    console.log(bootMsg);
  } else {
    console.warn(bootMsg);
  }
  
  // NEW: Start the event-driven prediction pipeline
  void startEventDrivenPipeline().catch((error) => {
    logger.error(
      { component: "PredictionWorker", error },
      "Failed to start event-driven prediction pipeline"
    );
  });
  
  void run(handle);
  return handle;
}

/** Stop the worker and release its lock. Best-effort on the lock release. */
export async function stopWorker(): Promise<void> {
  const handle = getHandle();
  if (!handle.running) return;
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
      // Adaptive poll (Telegram design §10.3): when a prediction is pending,
      // the next round's outcome is what matters, not new data. Drop the
      // cadence to a coarser poll so we save ~70% of upstream calls during
      // the 2–5s window where we're waiting for the round to land. End-to-end
      // latency is unchanged because the worker still polls; we just call the
      // upstream less aggressively.
      let nextInterval = POLL_INTERVAL_MS;
      try {
        const pending = await getPendingStatus();
        if (pending.hasPending) nextInterval = PENDING_POLL_INTERVAL_MS;
      } catch {
        /* fall through to default poll */
      }
      await sleep(handle, nextInterval);
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
};