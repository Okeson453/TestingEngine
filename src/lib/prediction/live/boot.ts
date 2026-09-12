/**
 * Boot orchestration for the live prediction pipeline.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.7
 *
 * Wires, in order:
 *   1. `ColdStartSeeder.runColdStartSeeder` — backfill crash_rounds if empty
 *   2. `OutboxDispatcher.start()` — drain queued Telegram notifications
 *   3. `LiveEventSubscriber` (via `events/game-event-handlers.startEventDrivenPipeline`)
 *      — subscribe to BC.Game's Socket.IO `bg`/`ed` events
 *   4. `PollWorker.start()` — REST safety net
 *   5. `ClockSkewMonitor.start()` — periodic skew measurement
 *
 * The boot is a single process. All four logical roles are independent
 * timers, so a failure in one does not take down the others.
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
    // Do NOT set started=true until all components initialize (P0 startup state machine)
    const bootStartedAt = new Date().toISOString();

    try {
    const seeder = deps.seeder ?? (() => runColdStartSeeder());
    const dispatcher = deps.dispatcher ?? new OutboxDispatcher();
    const pollWorker = deps.pollWorker ?? new PollWorker();
    const clockMonitor = deps.clockMonitor ?? new ClockSkewMonitor();
    this.dispatcher = dispatcher;
    this.pollWorker = pollWorker;
    this.clockMonitor = clockMonitor;

    // Cold-start must not crash the worker process on a transient DB timeout.
    // Railway will otherwise restart-loop while Neon is waking.
    let seed: SeedResult;
    try {
      seed = await withBootStage("cold-start-seeder", () => seeder());
    } catch (e) {
      logger.error(
        { component: "live-boot", error: String(e) },
        "cold-start seeder threw; continuing with empty seed result",
      );
      seed = {
        alreadySeeded: false,
        initialCount: 0,
        finalCount: 0,
        insertedTotal: 0,
        pagesFetched: 0,
        elapsedMs: 0,
        timedOut: true,
      };
    }

    let sql;
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      try {
        sql = await getSql();
        await sql`select 1`;
        break;
      } catch (e) {
        logger.warn(
          { component: "live-boot", attempt, error: String(e) },
          "DB not ready after cold-start; retrying",
        );
        await new Promise((r) => setTimeout(r, Math.min(2000 * attempt, 8000)));
      }
    }
    if (!sql) {
      throw new Error("Database unreachable after cold-start retries — aborting boot");
    }

    logger.info(
      { component: "live-boot", seed, bootStartedAt },
      "cold-start seeder complete; starting dispatcher / subscriber / poll / monitor",
    );

    // Spec §3.10 — schema validation. Run after the seeder so the seeder
    // has a chance to populate the table list; run before dispatcher /
    // poll so a missing migration fails the boot fast.
    try {
      await withBootStage("schema-validation", () => validateSchema(sql));
      logger.info(
        { component: "live-boot" },
        "schema validation passed; all required tables present",
      );
      // §5.1 Restore ACIE online state into the AUTHORITATIVE shared singleton.
      // P0: one process-wide ACIEEngine for observe + evaluate + emission.
      try {
        await withBootStage("acie-state-restore", async () => {
        const { getSharedACIEEngine, getSharedACIEInstanceId } = await import(
          "@/lib/prediction/acie/shared-engine"
        );
        const eng = getSharedACIEEngine();
        const result = await loadAcieStateFromDb(eng);
        // Keep globalThis bridge for any residual callers during rollout.
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
            "ACIE online state restored into shared singleton (warm)",
          );
        } else {
          logger.warn(
            {
              component: "live-boot",
              reason: result.reason,
              error: result.error ?? null,
              acieInstanceId: getSharedACIEInstanceId(),
            },
            `ACIE shared singleton starting cold — restore reason: ${result.reason}`,
          );
        }
        });
      } catch (e) {
        logger.warn(
          {
            component: "live-boot",
            reason: "db_error",
            error: String(e),
            stack: e instanceof Error ? e.stack : undefined,
          },
          "ACIE state restore threw — ensuring shared singleton exists cold",
        );
        try {
          const { getSharedACIEEngine } = await import(
            "@/lib/prediction/acie/shared-engine"
          );
          const cold = getSharedACIEEngine();
          (globalThis as { __acieEngine__?: typeof cold }).__acieEngine__ = cold;
        } catch {
          /* ACIE optional */
        }
      }

      // P2.11: Restore incremental state after schema validation
      await withBootStage("incremental-state-restore", () => restoreIncrementalState(sql));
      await withBootStage("baseline-adaptive-restore", () => restoreBaselineAdaptiveState(sql));
      await withBootStage("safe-baseline-restore", () => restoreSafeBaselineState(sql));

      // Pre-warm the PredictionEngine so the first live prediction avoids
      // constructor + module-resolution cost on the hot path.
      await withBootStage("prediction-engine-prewarm", async () => prewarmPredictionEngine());
      await withBootStage("hot-module-prewarm", () => prewarmHotModules());

      // Latency fix: collapse any historically inflated residual floor so the
      // first live ED is not systematically skipped_late → poll recovery.
      try {
        await sql`
          INSERT INTO worker_state (key, value, updated_at)
          VALUES ('effective_skip_below_ms', '120', now())
          ON CONFLICT (key) DO UPDATE
            SET value = '120', updated_at = now()
            WHERE worker_state.value::numeric > 200
        `;
        const { setEffectiveSkipBelowMs } = await import("@/lib/prediction/live/gate-cache");
        setEffectiveSkipBelowMs(120);
        logger.info({ component: "live-boot" }, "reset effective_skip_below_ms ceiling");
        try {
          const { clearSheathSamples } = await import("@/lib/core/sheath-mode");
          clearSheathSamples();
          logger.info({ component: "live-boot" }, "cleared sheath late-rate window");
        } catch { /* soft */ }
        try {
          const { globalProductionController } = await import(
            "@/lib/prediction/lifecycle/production-controller"
          );
          globalProductionController.manualRecoverDivergence();
          logger.info({ component: "live-boot" }, "divergence sheath recovered to level 0");
        } catch { /* soft */ }
        try {
          const { globalLiveDivergence } = await import(
            "@/lib/prediction/validation/live-divergence-monitor"
          );
          globalLiveDivergence.manualRecover(true);
        } catch { /* soft */ }
      } catch {
        /* soft */
      }

      // Warm the live rolling history buffer so the first ED predict hits
      // memory. P0: history is a hard prerequisite — without READY buffer
      // the predictor returns N+1_UNAVAILABLE_HISTORY (no silent SQL fallback).
      try {
        const {
          warmLiveHistoryBuffer,
          isHistoryReadyForPrediction,
          isLiveHistoryWarmed,
          liveHistorySize,
          MIN_HISTORY_FOR_PREDICTION,
        } = await import("@/lib/prediction/live/live-history-buffer");
        await withBootStage("history-warm", () => warmLiveHistoryBuffer(sql, 200));
        if (!isHistoryReadyForPrediction()) {
          logger.error(
            {
              component: "live-boot",
              warmed: isLiveHistoryWarmed(),
              size: liveHistorySize(),
              minRequired: MIN_HISTORY_FOR_PREDICTION,
            },
            "P0 health fault: live history NOT READY — N+1 predictions blocked until buffer recovers",
          );
        } else {
          logger.info(
            {
              component: "live-boot",
              size: liveHistorySize(),
            },
            "live history buffer READY (hot path memory-only, no history SQL)",
          );
        }
      } catch (e) {
        logger.error(
          { component: "live-boot", error: String(e) },
          "P0 health fault: live history warm failed — N+1 predictions blocked (no SQL fallback)",
        );
      }

      // Warm gate-cache so first predict skips worker_state SQL (another 700–1000ms).
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
        logger.info({ component: "live-boot", keys: rows.length }, "gate cache warmed");
      } catch (e) {
        logger.warn(
          { component: "live-boot", error: String(e) },
          "gate cache warm failed — first predict may query worker_state",
        );
      }
    } catch (e) {
      logger.error(
        { component: "live-boot", error: String(e) },
        "schema validation failed; aborting boot",
      );
      throw e;
    }

    // P0 (fix plan Phase 1): lease + fencing epoch = authority. Takeover must
    // PROVE the previous lease expired — no unconditional DELETE. During a
    // rolling deploy the old worker heartbeats until drained; we retry until
    // its lease proves expired (≤ 2×TTL after its last heartbeat) or it
    // releases on SIGTERM. Bounded by WORKER_LEASE_WAIT_MS.
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
      await new Promise((r) => setTimeout(r, 2_000));
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
      { component: "live-boot", workerId: WORKER_ID, workerEpoch: lock.epoch },
      "distributed worker lease acquired (fencing epoch active)",
    );

    // Second-opinion report #1: signing is a HARD readiness dependency. The
    // wr_utils bundle + self-test must land BEFORE the WS / pipeline starts —
    // an unsignable worker connects nothing and produces nothing while still
    // advertising ready. Production treats exhaustion as fatal (runtime
    // restarts); dev warns and continues so local work isn't blocked by
    // bc.game reachability.
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
    // Fix 6: supervisor owns ALL control-loop timers — lock heartbeat +
    // worker health (10s), invariant monitor (30s, exactly ONE timer — fix 1),
    // connection warmer (3s), event-loop probe (2s).
    const supervisor = getLiveSupervisor();
    supervisor.start();

    // Fix plan Phase 2: lock loss must cancel EVERY mutation-capable
    // component, not just supervisor timers. The supervisor calls
    // markAuthorityLost(); this cascade stops the dispatcher, poll worker and
    // clock monitor immediately. The ed/dispatch/poll gates consult
    // isAuthoritative() for the window before teardown completes.
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

    await withBootStage("dispatcher-start", () => dispatcher.start());
    // Retention sweep (audit 2026-09-11): live_event_log is append-only
    // observability with no cleanup — batched bounded DELETE on the general
    // pool, 6h cadence, unref'd timer. Prediction/result history is never
    // touched. Gated behind worker authority implicitly: this code only runs
    // on the worker that holds the lease (see lease gate above).
    try {
      const { startRetentionSweep } = await import("./retention");
      startRetentionSweep();
    } catch (e) {
      logger.warn(
        { component: "live-boot", error: String(e) },
        "retention sweep failed to start (soft) — observability table will grow",
      );
    }
    if (deps.startSubscriber) {
      try {
        await deps.startSubscriber();
      } catch (e) {
        logger.warn(
          { component: "live-boot", error: String(e) },
          "subscriber start failed; continuing with REST fallback",
        );
      }
    }
    await pollWorker.start();
    await clockMonitor.start();

    // Resilience: if the in-memory history buffer drops below READY (cold
    // boot race, partial warm, long WS gap), re-warm from crash_rounds on a
    // slow cadence without touching the prediction hot path.
    if (!this.historyRewarmTimer) {
      this.historyRewarmTimer = setInterval(() => {
        void (async () => {
          try {
            const {
              isHistoryReadyForPrediction,
              warmLiveHistoryBuffer,
            } = await import("./live-history-buffer");
            if (isHistoryReadyForPrediction()) return;
            const sql = await getSql();
            await warmLiveHistoryBuffer(sql, 200, true);
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
      }, Number(process.env.HISTORY_REWARM_MS ?? 60_000) || 60_000);
      this.historyRewarmTimer.unref?.();
    }

    this.started = true;
    this.lastResult = { seed, bootStartedAt };
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
