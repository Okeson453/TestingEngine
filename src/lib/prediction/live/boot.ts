/**
 * Boot orchestration for the live prediction pipeline.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.7
 *
 * LIVE-FIRST order (directive 2026-09-12):
 *   1. Minimal DB connectivity + schema validation
 *   2. Shared ACIE singleton (cold construct — no DB)
 *   3. Worker lease / fencing epoch (authority)
 *   4. Sign readiness (WS dependency)
 *   5. OutboxDispatcher + native WS subscriber (PR/BG/ED live path)
 *   6. PollWorker + ClockSkewMonitor
 *   7. BACKGROUND (never blocks live events): cold-start seeder,
 *      ACIE/incremental/baseline restores, history warm, gate cache, prewarm
 *
 * Heavy restoration must not delay PR→N+1. History re-warm timer remains
 * the safety net if the buffer is not READY on the first events.
 */
import { runColdStartSeeder, type SeedResult } from "./cold-start-seeder";
import { OutboxDispatcher } from "./notification-worker";
import { PollWorker } from "./poll-worker";
import { ClockSkewMonitor } from "./clock-skew-monitor";
import { getLogger } from "@/lib/observability/logger";
import { maxLoopLagBetween } from "@/lib/observability/event-loop-lag";
import { getSql, type Sql } from "@/lib/db";
import { loadAcieStateFromDb } from "@/lib/prediction/acie/state-persistence";
import { getSharedPredictionEngine } from "@/lib/prediction/live/predictor";
// Fix 6/1/14: WORKER_ID + persistIncrementalState live in the supervisor now.
import { WORKER_ID, LiveSupervisor, persistIncrementalState, restoreBaselineAdaptiveState, restoreSafeBaselineState } from "@/lib/prediction/live/live-supervisor";
import { setWorkerAuthority, onAuthorityLost } from "@/lib/prediction/live/fencing";

const logger = getLogger("live-boot");

/**
 * P2 BOOT-STAGE ATTRIBUTION (sep 12 10:40Z directive).
 *
 * The 10:40Z boot showed two event-loop stalls (1679ms, 419ms) that the
 * directive requires attributed to an EXACT operation, not to Neon. Both
 * happened during boot (the WS pipeline only starts after "live prediction
 * pipeline started" / "ws open", minutes of wall-clock later), and mid-stream
 * loop lag in the same window stayed ≤12ms — so the suspects are boot-time
 * synchronous work: strip-types TS compilation of dynamically imported
 * module graphs, JSON.parse of restored state, and PredictionEngine
 * construction.
 *
 * withBootStage wraps each boot stage and, when the stage is slow OR the
 * event loop stalled inside its window, emits ONE line naming the stage,
 * its wall duration, and the max loop lag measured DURING the stage window.
 * Next prod boot, the 1679/419 class lands on a named stage by construction.
 */
const BOOT_STAGE_WARN_MS = Number(process.env.BOOT_STAGE_WARN_MS ?? 250) || 250;

async function withBootStage<T>(name: string, fn: () => Promise<T>): Promise<T> {
  const t0 = performance.now();
  try {
    return await fn();
  } finally {
    const t1 = performance.now();
    const ms = Math.round(t1 - t0);
    const lag = Math.round(maxLoopLagBetween(t0, t1));
    if (ms >= BOOT_STAGE_WARN_MS || lag >= BOOT_STAGE_WARN_MS) {
      // Inline values: Railway raw logs show message text only.
      logger.warn(
        { component: "live-boot", stage: name, ms, loopLagMs: lag },
        `[boot] stage=${name} ms=${ms} loop_lag_ms=${lag}`,
      );
    }
  }
}

/**
 * Pre-warm the PredictionEngine singleton at boot so the first real
 * prediction does not pay constructor + module-resolution cost under
 * time pressure. Safe to call multiple times (singleton guard inside).
 */
function prewarmPredictionEngine(): void {
  try {
    getSharedPredictionEngine();
    logger.info({ component: "live-boot" }, "PredictionEngine pre-warmed");
  } catch {
    /* soft — may fail in test contexts */
  }
}

/** Eager-load lazy modules so first ED never pays import cost. */
async function prewarmHotModules(): Promise<void> {
  const mods = [
    import("@/lib/prediction/live/live-history-buffer"),
    import("@/lib/prediction/live/outbox-wake"),
    import("@/lib/prediction/live/gate-cache"),
    import("@/lib/prediction/live/predictor"),
    import("@/lib/prediction/live/validator"),
    import("@/lib/prediction/live/feedback"),
    import("@/lib/prediction/live/live-round-state"),
    // Fix 15: Prewarm all modules used by PredictionEngine.predict()
    import("@/lib/prediction/prediction-engine"),
    import("@/lib/prediction/state/incremental-state-engine"),
    import("@/lib/prediction/calibration/calibration-state"),
    import("@/lib/prediction/prediction-pipeline"),
    import("@/lib/prediction/models/baseline-model"),
    import("@/lib/prediction/ensemble/model-performance"),
    import("@/lib/prediction/features/feature-engine-v2"),
    import("@/lib/prediction/regimes/regime-detector"),
    import("@/lib/prediction/models/model-registry"),
    import("@/lib/prediction/signals/signal"),
  ];
  const results = await Promise.allSettled(mods);
  const failed = results.filter((r) => r.status === "rejected").length;
  logger.info(
    { component: "live-boot", modules: results.length, failed },
    "hot-path modules pre-warmed",
  );
}

/** Event-loop lag probe — moved into LiveSupervisor (fix 6). */

const LOCK_KEY = "prediction_worker";
const LOCK_TTL_SECONDS = 8;

async function acquireWorkerLock(
  sql: Sql,
): Promise<{ ok: boolean; epoch: number | null }> {
  // Drop clearly dead rows first (expired or heartbeat stalled > 2× TTL).
  // This DELETE is conditional — it PROVES expiry; the plan's ban is on
  // unconditional lock deletion as a takeover mechanism.
  await sql`
    DELETE FROM worker_locks
    WHERE lock_key = ${LOCK_KEY}
      AND (
        expires_at < now()
        OR heartbeat_at < now() - (${LOCK_TTL_SECONDS * 2}::int * interval '1 second')
      )
  `.catch(() => undefined);

  const rows = await sql<{ owner_id: string; epoch: number }>`
    INSERT INTO worker_locks (lock_key, owner_id, acquired_at, expires_at, heartbeat_at, epoch)
    VALUES (
      ${LOCK_KEY},
      ${WORKER_ID},
      now(),
      now() + (${LOCK_TTL_SECONDS}::int * interval '1 second'),
      now(),
      1
    )
    ON CONFLICT (lock_key) DO UPDATE
    SET owner_id = EXCLUDED.owner_id,
        acquired_at = EXCLUDED.acquired_at,
        expires_at = EXCLUDED.expires_at,
        heartbeat_at = EXCLUDED.heartbeat_at,
        epoch = worker_locks.epoch + 1
    WHERE worker_locks.expires_at < now()
       OR worker_locks.heartbeat_at < now() - (${LOCK_TTL_SECONDS * 2}::int * interval '1 second')
       OR worker_locks.owner_id = EXCLUDED.owner_id
    RETURNING epoch, owner_id
  `;
  const row = rows[0];
  return row && row.owner_id === WORKER_ID
    ? { ok: true, epoch: Number(row.epoch) }
    : { ok: false, epoch: null };
}

/** Rolling deploy: the previous container keeps its lease until its heartbeat
 *  goes stale (≤ 2×TTL after its last heartbeat) or it releases on SIGTERM.
 *  Takeover must PROVE lease expiry — unconditional DELETE is banned
 *  (fix plan Phase 1). Callers retry on a bounded loop instead. */

/**
 * Phase 4 / 12 — Restore adaptive incremental state from real Crash observations.
 * Never fabricate crash points (no Array(n).fill(1.5)).
 * Prefer recent multipliers from crash_rounds; fall back to cold-start if insufficient.
 */
async function restoreIncrementalState(sql: Sql): Promise<void> {
  try {
    const { globalIncrementalState } = await import(
      "@/lib/prediction/state/incremental-state-engine"
    );
    // Authoritative history: last N real crash multipliers (chronological)
    const history = await sql<{ multiplier: string | number }>`
      SELECT multiplier
      FROM crash_rounds
      WHERE crashed_at IS NOT NULL
        AND multiplier IS NOT NULL
      ORDER BY crashed_at DESC, game_id DESC
      LIMIT 500
    `;
    const points = history
      .map((r) => Number(r.multiplier))
      .filter((n) => Number.isFinite(n) && n > 0)
      .reverse();

    if (points.length >= 5) {
      globalIncrementalState.seed(points);
      logger.info(
        {
          component: "live-boot",
          count: points.length,
          source: "crash_rounds",
        },
        "Incremental state restored from real crash_rounds history",
      );
      return;
    }

    // Insufficient history — controlled cold start (do not fabricate values)
    logger.warn(
      {
        component: "live-boot",
        available: points.length,
        reason: "insufficient_history",
      },
      "Incremental state cold-start: insufficient crash_rounds history (no fabricated points)",
    );
  } catch (e) {
    logger.debug(
      { component: "live-boot", error: String(e) },
      "incremental state restore failed (soft) — continuing cold",
    );
  }
}

async function releaseWorkerLock(sql: Sql): Promise<void> {
  await sql`
    DELETE FROM worker_locks
    WHERE lock_key = ${LOCK_KEY} AND owner_id = ${WORKER_ID}
  `;
  await sql`
    INSERT INTO worker_state (key, value, updated_at)
    VALUES ('worker_status', 'stopped', now())
    ON CONFLICT (key) DO UPDATE
    SET value = EXCLUDED.value, updated_at = now()
  `;
}

/** Spec §3.10 — startup schema validation. Verifies every required table
 *  exists before any worker role is started. A missing table is a hard
 *  failure: the worker would otherwise fail in an arbitrary place. */
const REQUIRED_TABLES = [
  "crash_rounds",
  "pending_predictions",
  "prediction_validations",
  "notification_outbox",
  "live_event_log",
  "worker_locks",
  "worker_state",
  "acie_online_state",
  "live_round_state",
] as const;

export async function validateSchema(sql: Sql): Promise<void> {
  // Single round-trip instead of 9× sequential information_schema checks.
  const rows = await sql<{ table_name: string }>`
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name IN ('crash_rounds', 'pending_predictions', 'prediction_validations', 'notification_outbox', 'live_event_log', 'worker_locks', 'worker_state', 'acie_online_state', 'live_round_state')
  `;
  const found = new Set(rows.map((r) => r.table_name));
  const missing = (REQUIRED_TABLES as unknown as string[]).filter((t) => !found.has(t));
  if (missing.length > 0) {
    throw new Error(
      `Required table(s) missing: ${missing.join(", ")}. Run migrations before starting the worker.`,
    );
  }
}

export interface BootResult {
  seed: SeedResult;
  bootStartedAt: string;
}

export interface BootDeps {
  seeder?: () => Promise<SeedResult>;
  dispatcher?: OutboxDispatcher;
  pollWorker?: PollWorker;
  clockMonitor?: ClockSkewMonitor;
  startSubscriber?: () => Promise<void>;
}


/**
 * Non-critical boot work — scheduled AFTER the live pipeline is up.
 * Failures are soft; the history re-warm timer and cold ACIE paths cover gaps.
 */
async function runBackgroundHydration(
  sql: Sql,
  seeder: () => Promise<SeedResult>,
): Promise<void> {
  // Yield once so the first PR/BG handlers can interleave before CPU-heavy work.
  await new Promise<void>((r) => setImmediate(r));

  try {
    await withBootStage("cold-start-seeder", () => seeder());
  } catch (e) {
    logger.warn(
      { component: "live-boot", error: String(e) },
      "background cold-start seeder failed (soft)",
    );
  }

  try {
    await withBootStage("acie-state-restore", async () => {
      const { getSharedACIEEngine, getSharedACIEInstanceId } = await import(
        "@/lib/prediction/acie/shared-engine"
      );
      const eng = getSharedACIEEngine();
      const result = await loadAcieStateFromDb(eng);
      (globalThis as { __acieEngine__?: typeof eng }).__acieEngine__ = eng;
      if (result.restored) {
        logger.info(
          {
            component: "live-boot",
            reason: result.reason,
            observationCount: result.observationCount,
            crashPoints: result.crashPoints,
            acieInstanceId: getSharedACIEInstanceId(),
          },
          "ACIE online state restored into shared singleton (warm, background)",
        );
      } else {
        logger.warn(
          {
            component: "live-boot",
            reason: result.reason,
            error: result.error ?? null,
            acieInstanceId: getSharedACIEInstanceId(),
          },
          `ACIE remaining cold in background — restore reason: ${result.reason}`,
        );
      }
    });
  } catch (e) {
    logger.warn(
      { component: "live-boot", error: String(e) },
      "background ACIE restore failed (soft)",
    );
  }

  try {
    await Promise.all([
      withBootStage("incremental-state-restore", () => restoreIncrementalState(sql)),
      withBootStage("baseline-adaptive-restore", () => restoreBaselineAdaptiveState(sql)),
      withBootStage("safe-baseline-restore", () => restoreSafeBaselineState(sql)),
    ]);
  } catch (e) {
    logger.warn(
      { component: "live-boot", error: String(e) },
      "background state restores failed (soft)",
    );
  }

  try {
    await withBootStage("acie-tail-hydrate", async () => {
      const { hydrateAcieTailFromCrashRounds } = await import(
        "@/lib/prediction/live/predictor"
      );
      const hydrated = await hydrateAcieTailFromCrashRounds(sql);
      if (hydrated > 0) {
        logger.info(
          { component: "live-boot", hydrated_rounds: hydrated },
          "ACIE tail hydrated from crash_rounds (background)",
        );
      }
    });
  } catch (e) {
    logger.warn(
      { component: "live-boot", error: String(e) },
      "background ACIE tail hydrate failed (soft)",
    );
  }

  try {
    await withBootStage("prediction-engine-prewarm", async () => prewarmPredictionEngine());
    await withBootStage("hot-module-prewarm", () => prewarmHotModules());
  } catch (e) {
    logger.warn(
      { component: "live-boot", error: String(e) },
      "background prewarm failed (soft)",
    );
  }

  try {
    const { warmLiveHistoryBuffer } = await import("./live-history-buffer");
    await withBootStage("history-warm", () => warmLiveHistoryBuffer(sql, 200));
    logger.info(
      { component: "live-boot" },
      "Live history buffer warmed and READY for N+1 prediction (background)",
    );
  } catch (e) {
    logger.warn(
      { component: "live-boot", error: String(e) },
      "background history warm failed (soft) — rewarm timer will retry",
    );
  }

  try {
    const {
      setMedianInterRoundGapMs,
      setWallClockSkewMs,
      setEffectiveSkipBelowMs,
    } = await import("@/lib/prediction/live/gate-cache");
    const rows = await withBootStage<{ key: string; value: string }[]>("gate-cache-warm", () =>
      sql<{ key: string; value: string }>`
        SELECT key, value FROM worker_state
        WHERE key IN ('effective_skip_below_ms', 'median_inter_round_gap_ms', 'wall_clock_skew_ms')
      `.catch(() => [] as { key: string; value: string }[]),
    );
    for (const row of rows) {
      const n = Number(row.value);
      if (!Number.isFinite(n)) continue;
      if (row.key === "median_inter_round_gap_ms") setMedianInterRoundGapMs(n);
      if (row.key === "wall_clock_skew_ms") setWallClockSkewMs(n);
      if (row.key === "effective_skip_below_ms") {
        setEffectiveSkipBelowMs(Math.min(200, Math.max(80, n)));
      }
    }
    logger.info({ component: "live-boot", keys: rows.length }, "gate cache warmed (background)");
  } catch (e) {
    logger.warn(
      { component: "live-boot", error: String(e) },
      "background gate cache warm failed (soft)",
    );
  }

  logger.info({ component: "live-boot" }, "background hydration complete");
}

class LiveBoot {
  private dispatcher: OutboxDispatcher | null = null;
  private pollWorker: PollWorker | null = null;
  private clockMonitor: ClockSkewMonitor | null = null;
  private started = false;
  private historyRewarmTimer: ReturnType<typeof setInterval> | null = null;
  private lastResult: BootResult | null = null;

  async start(deps: BootDeps = {}): Promise<BootResult> {
    if (this.started) {
      return this.lastResult!;
    }
    // Do NOT set started=true until live path is up (P0 startup state machine)
    const bootStartedAt = new Date().toISOString();

    try {
    const seeder = deps.seeder ?? (() => runColdStartSeeder());
    const dispatcher = deps.dispatcher ?? new OutboxDispatcher();
    const pollWorker = deps.pollWorker ?? new PollWorker();
    const clockMonitor = deps.clockMonitor ?? new ClockSkewMonitor();
    this.dispatcher = dispatcher;
    this.pollWorker = pollWorker;
    this.clockMonitor = clockMonitor;

    // ── 1. Minimal DB connectivity (no cold-start seeder on critical path) ──
    let sql: Sql | undefined;
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      try {
        sql = await getSql();
        await sql`select 1`;
        break;
      } catch (e) {
        logger.warn(
          { component: "live-boot", attempt, error: String(e) },
          "DB not ready; retrying",
        );
        await new Promise((r) => setTimeout(r, Math.min(500 * attempt, 3000)));
      }
    }
    if (!sql) {
      throw new Error("Database unreachable after connectivity retries — aborting boot");
    }

    // ── 2. Schema validation (fail fast) ──
    try {
      await withBootStage("schema-validation", () => validateSchema(sql));
      logger.info(
        { component: "live-boot" },
        "schema validation passed; all required tables present",
      );
    } catch (e) {
      logger.error(
        { component: "live-boot", error: String(e) },
        "schema validation failed; aborting boot",
      );
      throw e;
    }

    // ── 3. Cold ACIE singleton (no DB) so first PR can evaluate ──
    try {
      const { getSharedACIEEngine } = await import(
        "@/lib/prediction/acie/shared-engine"
      );
      const cold = getSharedACIEEngine();
      (globalThis as { __acieEngine__?: typeof cold }).__acieEngine__ = cold;
      logger.info({ component: "live-boot" }, "Shared ACIEEngine constructed (cold; restore in background)");
    } catch (e) {
      logger.warn(
        { component: "live-boot", error: String(e) },
        "ACIE cold construct failed (soft)",
      );
    }

    // ── 4. Worker lease EARLY (authority before live mutations) ──
    // Rolling deploy: previous container keeps lease until heartbeat stale.
    // Retry every 500ms (was 2s) so takeover after SIGTERM is faster without
    // weakening fencing — still requires prove-expired, no unconditional steal.
    const leaseWaitDeadline = Date.now() + Number(process.env.WORKER_LEASE_WAIT_MS ?? 45_000);
    let lock: { ok: boolean; epoch: number | null } = { ok: false, epoch: null };
    let leaseAttempts = 0;
    while (Date.now() < leaseWaitDeadline) {
      leaseAttempts += 1;
      lock = await acquireWorkerLock(sql);
      if (lock.ok) break;
      if (leaseAttempts === 1) {
        logger.warn(
          {
            component: "live-boot",
            workerId: WORKER_ID,
            leaseWaitMs: Number(process.env.WORKER_LEASE_WAIT_MS ?? 45_000),
          },
          "WORKER_LEASE_WAITING: another worker holds an unexpired lease — retrying until it proves expired",
        );
      }
      await new Promise((r) => setTimeout(r, Number(process.env.WORKER_LEASE_RETRY_MS ?? 500)));
    }
    if (!lock.ok) {
      logger.error(
        { component: "live-boot", workerId: WORKER_ID, attempts: leaseAttempts },
        "Another worker holds an unexpired lease. Refusing to start mutation roles.",
      );
      throw new Error(
        `Worker lease not acquired (another instance holds '${LOCK_KEY}' and its lease never proved expired)`,
      );
    }
    setWorkerAuthority(lock.epoch);
    logger.info(
      { component: "live-boot", workerId: WORKER_ID, workerEpoch: lock.epoch, leaseAttempts },
      "distributed worker lease acquired (fencing epoch active)",
    );

    // ── 5. Sign readiness (WS dependency) ──
    const signReadyTimeoutMs = Number(process.env.SIGN_READY_TIMEOUT_MS ?? 30_000);
    try {
      const { ensureSignReady } = await import("@/lib/crash/native-sign");
      await withBootStage("sign-ready", () => ensureSignReady(signReadyTimeoutMs));
    } catch (e) {
      if (process.env.NODE_ENV === "production") {
        logger.error(
          { component: "live-boot", workerId: WORKER_ID, error: String(e) },
          "FATAL: signing not ready — refusing to start live pipeline",
        );
        throw e;
      }
      logger.warn(
        { component: "live-boot", error: String(e) },
        "sign readiness failed (dev) — starting pipeline degraded",
      );
    }

    // ── 6. Supervisor + authority-loss cascade ──
    const supervisor = getLiveSupervisor();
    supervisor.start();
    onAuthorityLost(() => {
      logger.error(
        { component: "live-boot", workerId: WORKER_ID },
        "authority lost — stopping dispatcher/poll/clock components",
      );
      try { void this.dispatcher?.stop(); } catch { /* */ }
      try { void this.pollWorker?.stop(); } catch { /* */ }
      try { void this.clockMonitor?.stop(); } catch { /* */ }
      try {
        void import("./retention").then(({ stopRetentionSweep }) => stopRetentionSweep());
      } catch { /* */ }
    });

    // ── 7. LIVE PATH: dispatcher + WS subscriber + poll + clock ──
    await withBootStage("dispatcher-start", () => dispatcher.start());
    try {
      const { startRetentionSweep } = await import("./retention");
      startRetentionSweep();
    } catch (e) {
      logger.warn(
        { component: "live-boot", error: String(e) },
        "retention sweep failed to start (soft)",
      );
    }
    if (deps.startSubscriber) {
      try {
        await withBootStage("subscriber-start", () => deps.startSubscriber!());
      } catch (e) {
        logger.warn(
          { component: "live-boot", error: String(e) },
          "subscriber start failed; continuing with REST fallback",
        );
      }
    }
    await pollWorker.start();
    await clockMonitor.start();

    // History re-warm safety net (covers first events before background warm)
    if (!this.historyRewarmTimer) {
      this.historyRewarmTimer = setInterval(() => {
        void (async () => {
          try {
            const {
              isHistoryReadyForPrediction,
              warmLiveHistoryBuffer,
            } = await import("./live-history-buffer");
            if (isHistoryReadyForPrediction()) return;
            const s = await getSql();
            await warmLiveHistoryBuffer(s, 200, true);
            logger.info(
              { component: "live-boot" },
              "history buffer re-warm completed (was not READY)",
            );
          } catch (e) {
            logger.warn(
              { component: "live-boot", error: String(e) },
              "history buffer re-warm failed (soft)",
            );
          }
        })();
      }, Number(process.env.HISTORY_REWARM_MS ?? 15_000) || 15_000);
      this.historyRewarmTimer.unref?.();
    }

    // Live path is up — accept events. Heavy hydration must not block PR/BG.
    this.started = true;
    const emptySeed: SeedResult = {
      alreadySeeded: false,
      initialCount: 0,
      finalCount: 0,
      insertedTotal: 0,
      pagesFetched: 0,
      elapsedMs: 0,
      timedOut: false,
    };
    this.lastResult = { seed: emptySeed, bootStartedAt };
    logger.info(
      {
        component: "live-boot",
        bootStartedAt,
        liveReadyMs: Date.now() - new Date(bootStartedAt).getTime(),
      },
      "LIVE PATH READY — background hydration starting (does not block PR/BG/N+1)",
    );

    // ── 8. BACKGROUND hydration (fire-and-forget) ──
    void runBackgroundHydration(sql, seeder).catch((e) => {
      logger.warn(
        { component: "live-boot", error: String(e) },
        "background hydration chain failed (soft)",
      );
    });

    return this.lastResult;
    } catch (err) {
      // Cleanup partial init so retry can rebuild cleanly
      this.started = false;
      try { await getLiveSupervisor().stop(); } catch { /* */ }
      try { await this.dispatcher?.stop(); } catch { /* */ }
      try { await this.pollWorker?.stop(); } catch { /* */ }
      try { await this.clockMonitor?.stop(); } catch { /* */ }
      try {
        const s = await getSql();
        await releaseWorkerLock(s);
      } catch { /* */ }
      this.dispatcher = null;
      this.pollWorker = null;
      this.clockMonitor = null;
      throw err;
    }
  }

  async stop(): Promise<void> {
    if (this.historyRewarmTimer) {
      clearInterval(this.historyRewarmTimer);
      this.historyRewarmTimer = null;
    }
    if (!this.started) return;
    this.started = false;
    // Clean shutdown revokes authority explicitly (fencing registry).
    setWorkerAuthority(null);
    // Fix 6: supervisor stops ALL timers (heartbeat, invariants, warmer, probe)
    await getLiveSupervisor().stop();
    try {
      const { stopRetentionSweep } = await import("./retention");
      stopRetentionSweep();
    } catch { /* soft */ }
    if (this.dispatcher) {
      try { await this.dispatcher.stop(); } catch { /* best effort */ }
    }
    if (this.pollWorker) {
      try { await this.pollWorker.stop(); } catch { /* best effort */ }
    }
    if (this.clockMonitor) {
      try { await this.clockMonitor.stop(); } catch { /* best effort */ }
    }
    try {
      const sql = await getSql();
      await releaseWorkerLock(sql);
      // P2.11: Persist incremental state on shutdown
      await persistIncrementalState(sql);
    } catch (e) {
      logger.warn(
        { component: "live-boot", error: String(e) },
        "failed to release worker lock or persist state on stop",
      );
    }
  }
}

/**
 * Fix 6/1/14 — LiveSupervisor owns ALL control-loop timers:
 *   - worker lock heartbeat + derived worker health (10s)
 *   - production invariant monitor (30s) — created EXACTLY ONCE (fix 1)
 *   - connection warmer (3s), event-loop lag probe (2s)
 * and derives the authoritative WorkerHealth record (fix 14).
 */
export { WORKER_ID, persistIncrementalState, restoreBaselineAdaptiveState, restoreSafeBaselineState } from "@/lib/prediction/live/live-supervisor";
export { LiveSupervisor } from "@/lib/prediction/live/live-supervisor";

const globalSupervisorRef = globalThis as typeof globalThis & {
  __liveSupervisor__?: LiveSupervisor;
};
export function getLiveSupervisor(): LiveSupervisor {
  globalSupervisorRef.__liveSupervisor__ ??= new LiveSupervisor();
  return globalSupervisorRef.__liveSupervisor__;
}

const globalRef = globalThis as typeof globalThis & {
  __liveBoot__?: LiveBoot;
};

export function getLiveBoot(): LiveBoot {
  globalRef.__liveBoot__ ??= new LiveBoot();
  return globalRef.__liveBoot__;
}

export async function startLiveBoot(deps?: BootDeps): Promise<BootResult> {
  return getLiveBoot().start(deps);
}

export async function stopLiveBoot(): Promise<void> {
  await getLiveBoot().stop();
}
