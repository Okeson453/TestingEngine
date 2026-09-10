/**
 * BC.Game native WS → live prediction pipeline.
 *
 * ED(N)  → owns N+1 prediction (signal-first)
 * BG(N+1) → reconciliation only (no prediction)
 * Poll   → recovery only
 */
import { randomUUID } from "node:crypto";
import { bcGameSocket } from "@/lib/crash/socket-client";
import { nativeBcGameSocket } from "@/lib/crash/native-socket-client";
import { prewarmSign } from "@/lib/crash/native-sign";
import { getRealtimePipeline, logRealtimeSnapshot } from "@/lib/realtime/realtime-pipeline";
import { getSql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";
import { onGameEnd } from "@/lib/prediction/live/validator";
import { attemptNPlusOnePrediction } from "@/lib/prediction/live/prediction-attempt";
import { globalIncrementalState } from "@/lib/prediction/state/incremental-state-engine";
import {
  markLiveRoundStarted,
  markLiveRoundEnded,
} from "@/lib/prediction/live/live-round-state";
import { appendCompletedRound } from "@/lib/prediction/live/live-history-buffer";
import { isAuthoritative } from "@/lib/prediction/live/fencing";
import {
  completeTarget,
  releaseTarget,
} from "@/lib/prediction/live/target-coordinator";
import {
  startTrace,
  mark,
  finishSignalReady,
  finishPersist,
  logLatencyBudgetSnapshot,
} from "@/lib/prediction/live/latency-trace";
import { syncDbClockOffset, shouldResyncClock } from "@/lib/prediction/live/clock-offset";

const logger = getLogger("game-event-handlers");
const inFlightEd = new Set<string>();
const inFlightBg = new Set<string>();

// P0 — native WS duplicate-event dedup (idempotency by canonical round ID).
// inFlightEd only blocks CONCURRENT re-entry; the same crash event arriving
// again after the first handler finished (observed 350ms–1.2s apart in prod)
// re-ran the entire ED pipeline. game ID is the idempotency key — never the
// timestamp. Bounded ledger: pruned on insert, capped.
const completedEdRounds = new Map<string, number>();
const ED_DEDUP_TTL_MS = 10 * 60_000;
const ED_DEDUP_MAX = 1_000;

export type EdReentryClassification =
  | "new"
  | "duplicate_event"
  | "already_in_progress";

/** Pure classification of a re-entrant ED crash event for one round ID. */
export function classifyEdReentry(gameId: string): EdReentryClassification {
  if (completedEdRounds.has(gameId)) return "duplicate_event";
  if (inFlightEd.has(gameId)) return "already_in_progress";
  return "new";
}

function recordEdRoundProcessed(gameId: string): void {
  const now = Date.now();
  completedEdRounds.set(gameId, now);
  // Drop expired entries, then enforce the hard cap (oldest first).
  for (const [id, ts] of completedEdRounds) {
    if (now - ts > ED_DEDUP_TTL_MS) completedEdRounds.delete(id);
  }
  while (completedEdRounds.size > ED_DEDUP_MAX) {
    let oldestId: string | null = null;
    let oldestTs = Number.POSITIVE_INFINITY;
    for (const [id, ts] of completedEdRounds) {
      if (ts < oldestTs) {
        oldestTs = ts;
        oldestId = id;
      }
    }
    if (oldestId == null) break;
    completedEdRounds.delete(oldestId);
  }
}

/** Test/observability hook: mark a round processed (used by tests). */
export function recordEdRoundProcessedForTests(gameId: string): void {
  recordEdRoundProcessed(gameId);
}

export function _resetEdDedupForTests(): void {
  completedEdRounds.clear();
}

// Phase 5: recovery control — max 1 immediate recovery per source, then quarantine.
const recoveryAttempts = new Map<string, number>();
const RECOVERY_MAX = 1;

function scheduleImmediateN1Recovery(input: {
  sourceRoundId: string;
  sourceCrashAt: string;
  sourceMultiplier: number;
  correlationId: string;
}): void {
  const key = input.sourceRoundId;
  const n = (recoveryAttempts.get(key) ?? 0) + 1;
  recoveryAttempts.set(key, n);
  if (n > RECOVERY_MAX) {
    logger.warn(
      {
        component: "game-event-handlers",
        sourceGameId: key,
        attempts: n,
        correlationId: input.correlationId,
      },
      "N+1 recovery quarantined — max immediate attempts reached; wait for next ED",
    );
    return;
  }
  // Fire-and-forget single recovery via the authoritative attempt path.
  void (async () => {
    try {
      const result = await attemptNPlusOnePrediction({
        sourceRoundId: input.sourceRoundId,
        sourceCrashAt: input.sourceCrashAt,
        sourceMultiplier: input.sourceMultiplier,
        source: "RECOVERY",
        correlationId: input.correlationId,
      });
      logger.info(
        {
          component: "game-event-handlers",
          sourceGameId: input.sourceRoundId,
          targetGameId: result.targetGameId,
          attempted: result.attempted,
          kind: result.kind,
          recoveryAttempt: n,
        },
        result.attempted
          ? "immediate N+1 recovery succeeded"
          : "immediate N+1 recovery soft result",
      );
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      logger.error(
        {
          component: "game-event-handlers",
          sourceGameId: input.sourceRoundId,
          recoveryAttempt: n,
          errorName: e.name,
          errorMessage: e.message,
          errorStack: e.stack?.slice(0, 1500) ?? null,
        },
        "immediate N+1 recovery threw",
      );
    }
  })();
}

function toIsoString(timestamp: number | string | undefined): string | null {
  if (timestamp == null || timestamp === "") return null;
  if (typeof timestamp === "string") {
    const date = new Date(timestamp);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }
  let ms = timestamp;
  if (timestamp > 0 && timestamp < 1e11) {
    ms = timestamp * 1000;
  }
  if (Math.abs(ms - Date.now()) > 86_400_000) {
    return new Date().toISOString();
  }
  const date = new Date(ms);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function extractLastGameId(payload: unknown): string | null {
  if (typeof payload !== "object" || payload === null) return null;
  const p = payload as Record<string, unknown>;
  const id = p.gameId ?? p.id;
  if (typeof id === "string" && id.length > 0) return id;
  if (typeof id === "number" && Number.isFinite(id)) return String(Math.trunc(id));
  return null;
}

function nextTargetGameId(sourceGameId: string): string {
  try {
    return String(BigInt(sourceGameId) + 1n);
  } catch {
    const n = Number(sourceGameId);
    return Number.isFinite(n) ? String(n + 1) : sourceGameId;
  }
}

/**
 * BG: reconcile target start only — never create a new prediction.
 */
// Exported for tests: the BG-arrival signal-kill contract is asserted
// directly against this handler (see outbox-lifecycle.test.ts).
export async function bgHandler(payload: unknown): Promise<void> {
  const gameId = extractLastGameId(payload);
  if (!gameId) return;
  if (inFlightBg.has(gameId)) return;
  inFlightBg.add(gameId);

  const correlationId = randomUUID();
  const p = (payload ?? {}) as Record<string, unknown>;
  // BG is the ONLY authoritative source of the real round start time.
  const beganAt =
    toIsoString((p.beganAt ?? p.beginTime) as number | string | undefined) ??
    new Date().toISOString();
  // Fix 10: `received_at` must be the worker's actual receipt time, not the
  // event's game-start time — otherwise latency telemetry is corrupted.
  const receivedAt = new Date().toISOString();
  const processorLatencyMs = Math.max(
    0,
    new Date(receivedAt).getTime() - new Date(beganAt).getTime(),
  );

  try {
    const sql = await getSql();
    // Backfill began_at when known from BG (authoritative round start).
    await sql`
      UPDATE crash_rounds
      SET began_at = COALESCE(began_at, ${new Date(beganAt)})
      WHERE game_id = ${gameId}
    `.catch(() => undefined);

    await Promise.all([
      markLiveRoundStarted(gameId, beganAt, "socket", correlationId, sql).catch(() => undefined),
      // P0 correlation: stamp target_round_started_at on the pending prediction for N
      sql`
        UPDATE pending_predictions
        SET target_round_started_at = COALESCE(target_round_started_at, ${new Date(beganAt)})
        WHERE target_game_id = ${gameId}
          AND matched = false
      `.catch(() => undefined),
      // Forensics: reclassify delivered signals vs authoritative began_at
      (async () => {
        try {
          const { reclassifyOnTargetStart } = await import(
            "@/lib/prediction/live/delivery-forensics"
          );
          await reclassifyOnTargetStart(sql, gameId, beganAt);
        } catch {
          /* soft */
        }
      })(),
      // P0 (temporal validity): authoritative round-start backfill on the
      // prediction registry — re-evaluates createdAt < targetStartedAt.
      import("@/lib/prediction/identity/prediction-registry")
        .then(({ globalPredictionRegistry }) => {
          globalPredictionRegistry.noteTargetStarted(gameId, beganAt);
        })
        .catch(() => undefined),
      // Hard temporal contract (report #13): BG(N) arriving means round N has
      // STARTED — every undelivered prediction signal targeting N is now
      // EXPIRED. Atomic kill beats waiting for the dispatcher tick.
      // P0: do NOT fail-open — retry once and log hard if kill cannot run.
      (async () => {
        const killSql = async () =>
          sql`
            UPDATE notification_outbox
            SET status = 'dead_letter',
                last_error = 'expired_late_signal: target round started (BG received)'
            WHERE type = 'prediction'
              AND status IN ('pending', 'inflight')
              AND target_game_id = ${gameId}
          `;
        try {
          await killSql();
        } catch (e1) {
          logger.warn(
            { event: "bg", gameId, error: String(e1), attempt: 1 },
            "BG temporal kill failed — retrying once",
          );
          try {
            await killSql();
          } catch (e2) {
            logger.error(
              { event: "bg", gameId, error: String(e2), attempt: 2 },
              "BG temporal kill FAILED after retry — prediction may still be inflight",
            );
          }
        }
      })(),
      sql`
        INSERT INTO live_event_log (
          correlation_id, event_kind, game_id, payload, received_at, processed_at,
          processor_latency_ms, sla_violated
        ) VALUES (
          ${correlationId}::text, 'BG', ${gameId}, ${JSON.stringify({ beganAt, reconcileOnly: true })},
          ${receivedAt}::timestamptz, now(), ${processorLatencyMs}, false
        ) ON CONFLICT DO NOTHING
      `.catch(() => undefined),
    ]);

    logger.info(
      { event: "bg", gameId, correlationId },
      "bg reconcile complete (no prediction)",
    );
  } catch (error) {
    logger.error({ event: "bg", gameId, error: String(error) }, "bg observability failed");
  } finally {
    inFlightBg.delete(gameId);
  }
}

/**
 * Fix 11: single canonical crash-final normalization.
 *
 * `ed` (round ended) and `st` (settled) are the same semantic event on the
 * BC.Game protocol (see native-protocol.ts: both carry endTime/multiplier
 * with identical payload shape). They MUST be normalized to one canonical
 * shape here so exactly one handler (edHandler) owns crash finalization —
 * no second decision path.
 */
interface NormalizedCrashEnd {
  gameId: string;
  multiplier: number | null;
  crashedAt: string;
  hash: string | null;
  /** Original protocol event name, for telemetry only. */
  sourceEvent: "ed" | "st";
}

export function normalizeCrashEnd(
  raw: { gameId: string; multiplier?: number | null; endTime?: number | string | null; crashedAt?: number | string | null; hash?: string | null },
  sourceEvent: "ed" | "st",
): NormalizedCrashEnd | null {
  if (!raw.gameId) return null;
  const crashedAt =
    toIsoString((raw.crashedAt ?? raw.endTime) as number | string | undefined) ??
    new Date().toISOString();
  return {
    gameId: raw.gameId,
    multiplier: raw.multiplier ?? null,
    crashedAt,
    hash: raw.hash ?? null,
    sourceEvent,
  };
}

/**
 * ED(N): owns N+1 prediction — signal first, persistence async.
 * Phase 2: attemptNPlusOnePrediction is the sole ownership boundary.
 */
async function edHandler(payload: unknown): Promise<void> {
  // Fencing gate (fix plan Phase 1): a worker that lost authority must not
  // ingest authoritative events or compute predictions. No-op before boot
  // initializes the fencing registry (tests, non-boot processes).
  if (!isAuthoritative()) {
    logger.warn(
      { component: "game-event-handlers", ownership_result: "not_authoritative" },
      "ED event dropped — worker authority lost",
    );
    return;
  }
  const gameId = extractLastGameId(payload);
  if (!gameId) return;
  // P0: dedupe by canonical round ID BEFORE any computation. Duplicate native
  // WS events must never reach claim/N+1 compute — classify and stop here.
  const reentry = classifyEdReentry(gameId);
  if (reentry !== "new") {
    logger.info(
      {
        component: "game-event-handlers",
        event: "ed",
        gameId,
        ownership_result: reentry,
      },
      "ED crash event deduplicated — skipping reprocessing",
    );
    return;
  }
  inFlightEd.add(gameId);

  const correlationId = randomUUID();
  const trace = startTrace(correlationId, gameId);
  mark(trace, "decoded");
  mark(trace, "normalized");

  const p = (payload ?? {}) as Record<string, unknown>;
  const sourceEvent = (p.sourceEvent as "ed" | "st" | undefined) ?? "ed";
  let multiplier =
    typeof p.multiplier === "number"
      ? p.multiplier
      : typeof p.maxRate === "number"
        ? p.maxRate / 100
        : null;
  // Hundredths heuristic (BC.Game sometimes sends 150 for 1.50x)
  if (multiplier != null && multiplier > 50 && Number.isInteger(multiplier)) {
    multiplier = multiplier / 100;
  }
  const crashedAt =
    toIsoString((p.crashedAt ?? p.endTime) as number | string | undefined) ??
    new Date().toISOString();

  try {
    if (multiplier == null || !Number.isFinite(multiplier)) {
      logger.warn({ event: sourceEvent, gameId }, "crash event missing multiplier — skip predict");
      return;
    }

    // --- P0 REALTIME path (no await on DB before signal) ---
    try {
      globalIncrementalState.update(multiplier);
    } catch {
      /* soft */
    }
    try {
      appendCompletedRound({
        gameId,
        multiplier,
        crashedAt,
      });
    } catch {
      /* soft */
    }
    mark(trace, "state_updated");

    const targetGameId = nextTargetGameId(gameId);
    trace.targetGameId = targetGameId;
    // Phase 2: attemptNPlusOnePrediction is the sole ownership boundary
    // (claimTarget lives inside onGameEndPredict). ED no longer double-claims.
    mark(trace, "target_claimed");

    try {
      const result = await attemptNPlusOnePrediction({
        sourceRoundId: gameId,
        sourceCrashAt: crashedAt,
        sourceMultiplier: multiplier,
        source: "ED",
        correlationId,
        trace,
      });
      const totalMs = finishSignalReady(trace);
      if (result.attempted) {
        completeTarget(targetGameId, `ed:${gameId}`);
        logger.info(
          {
            event: sourceEvent,
            gameId,
            targetGameId,
            ownership_result: "owned_predicted",
            predictionId: result.predictionId,
            kind: result.kind,
            ed_to_signal_ms: Math.round(totalMs * 100) / 100,
            correlationId,
          },
          result.kind === "predicted"
            ? "ED→N+1 SIGNAL_READY (durable outbox enqueued)"
            : `ED→N+1 result kind=${result.kind}`,
        );
      } else {
        // soft miss / duplicate / insufficient_history / exception handled in attempt
        releaseTarget(targetGameId, `ed:${gameId}`);
        logger.info(
          {
            event: sourceEvent,
            gameId,
            targetGameId,
            ownership_result:
              result.kind === "duplicate" ? "already_persisted" : `soft:${result.kind ?? "unknown"}`,
            kind: result.kind,
            ed_to_signal_ms: Math.round(totalMs * 100) / 100,
            correlationId,
          },
          "ED→N+1 soft result",
        );
        if (
          result.kind &&
          (result.kind.startsWith("exception") ||
            result.kind === "insufficient_history")
        ) {
          scheduleImmediateN1Recovery({
            sourceRoundId: gameId,
            sourceCrashAt: crashedAt,
            sourceMultiplier: multiplier,
            correlationId,
          });
        }
      }
    } catch (error) {
      // attemptNPlusOnePrediction already swallows and returns; this is defensive.
      releaseTarget(targetGameId, `ed:${gameId}`);
      const err = error instanceof Error ? error : new Error(String(error));
      logger.error(
        {
          event: sourceEvent,
          gameId,
          targetGameId,
          correlationId,
          stage: (err as { stage?: string }).stage ?? "unknown",
          errorName: err.name,
          errorMessage: err.message,
          errorStack: err.stack?.slice(0, 2000) ?? null,
        },
        "ED→N+1 prediction failed",
      );
      scheduleImmediateN1Recovery({
        sourceRoundId: gameId,
        sourceCrashAt: crashedAt,
        sourceMultiplier: multiplier,
        correlationId,
      });
    }

    // --- P2 DURABILITY / validation async (must not block signal) ---
    void (async () => {
      mark(trace, "persist_started");
      try {
        const sql = await getSql();
        const crashedAtDate = new Date(crashedAt);
        // began_at NULL until BG arrives (BG is the authoritative round start)
        if (!Number.isNaN(crashedAtDate.getTime())) {
          await sql`
            INSERT INTO crash_rounds (game_id, multiplier, hash, seed, began_at, crashed_at)
            VALUES (${gameId}, ${multiplier}, null, null, null, ${crashedAtDate})
            ON CONFLICT (game_id) DO UPDATE SET
              multiplier = EXCLUDED.multiplier,
              crashed_at = COALESCE(EXCLUDED.crashed_at, crash_rounds.crashed_at)
          `.catch(() => undefined);
        }
        await markLiveRoundEnded(gameId, crashedAt, multiplier, sql, "socket").catch(
          () => undefined,
        );
        // Validation of N (separate from N+1 predict)
        await onGameEnd({
          gameId,
          endTime: crashedAt,
          multiplier,
          receivedAt: new Date().toISOString(),
          skipPredict: true, // ED already owns N+1
        }).catch((error) => {
          logger.error(
            { event: sourceEvent, gameId, error: String(error), correlationId },
            "ed validation failed",
          );
        });
        mark(trace, "persist_completed");
        // P1: record persist/outbox-enqueue leg durations into the latency budget.
        finishPersist(trace);
      } catch (error) {
        logger.error({ event: sourceEvent, gameId, error: String(error) }, "ed async persist failed");
      }
    })();
  } catch (error) {
    logger.error({ event: sourceEvent, gameId, error: String(error) }, "ed handler failed");
  } finally {
    inFlightEd.delete(gameId);
    // Round fully processed (predict attempt + async persist kicked off).
    // Any further native WS event for this ID is a duplicate by definition.
    recordEdRoundProcessed(gameId);
  }
}

let handlersWired = false;

export function initializeEventHandlers(): void {
  if (handlersWired) return;
  handlersWired = true;

  bcGameSocket.on("bg", bgHandler);
  bcGameSocket.on("ed", edHandler);

  nativeBcGameSocket.onEvent((ev) => {
    if (ev.event === "bg" || ev.event === "pr") {
      void bgHandler({
        gameId: ev.gameId,
        beginTime: ev.beginTime ?? ev.receivedAt,
        beganAt: ev.beginTime ?? ev.receivedAt,
      });
    } else if (ev.event === "ed" || ev.event === "st") {
      // Fix 11: ed and st normalize to ONE canonical crash-final event —
      // a single edHandler owns finalization for both protocol events.
      const normalized = normalizeCrashEnd(
        {
          gameId: ev.gameId,
          multiplier: ev.multiplier,
          endTime: ev.endTime ?? ev.receivedAt,
          crashedAt: ev.endTime ?? ev.receivedAt,
          hash: ev.hash,
        },
        ev.event === "st" ? "st" : "ed",
      );
      if (normalized) {
        void edHandler({
          gameId: normalized.gameId,
          multiplier: normalized.multiplier,
          endTime: normalized.crashedAt,
          crashedAt: normalized.crashedAt,
          hash: normalized.hash,
          sourceEvent: normalized.sourceEvent,
        });
      }
    }
  });
  nativeBcGameSocket.onStatus((status, detail) => {
    logger.info(
      { component: "game-event-handlers", nativeStatus: status, detail },
      "native BC socket status",
    );
  });

  logger.info({ component: "game-event-handlers" }, "event handlers wired (ED-first)");
}

/** Called by worker boot — native WS primary; socket.io optional fallback. */
export async function startEventDrivenPipeline(): Promise<void> {
  initializeEventHandlers();
  try {
    const sql = await getSql();
    if (shouldResyncClock(0)) {
      await syncDbClockOffset(sql).catch(() => undefined);
    }
    const rows = await sql<{ game_id: string }>`
      SELECT game_id FROM crash_rounds ORDER BY crashed_at DESC LIMIT 100
    `;
    if (rows.length > 0) getRealtimePipeline().hydrate(rows.map((r) => r.game_id));
  } catch (e) {
    logger.warn({ error: String(e) }, "realtime validator hydration failed");
  }

  const snapshotTimer = setInterval(() => {
    logRealtimeSnapshot();
    logLatencyBudgetSnapshot();
    void (async () => {
      try {
        if (shouldResyncClock()) {
          const sql = await getSql();
          await syncDbClockOffset(sql);
        }
      } catch {
        /* soft */
      }
    })();
  }, 5 * 60_000);
  snapshotTimer.unref?.();

  const useNative = process.env.USE_NATIVE_BC_WS !== "0";
  if (useNative) {
    try {
      prewarmSign();
      await Promise.race([
        new Promise((r) => setTimeout(r, 1_500)),
        new Promise((r) => setTimeout(r, 0)),
      ]);
      await nativeBcGameSocket.start();
      logger.info({ component: "game-event-handlers" }, "native BC websocket started");
    } catch (e) {
      logger.warn(
        { error: String(e) },
        "native BC websocket failed — falling back to socket.io client",
      );
      try {
        await bcGameSocket.connect();
      } catch (e2) {
        logger.warn({ error: String(e2) }, "socket.io fallback also failed — poll path active");
      }
    }
  } else {
    try {
      await bcGameSocket.connect();
    } catch (e) {
      logger.warn({ error: String(e) }, "socket.io connect failed — poll path active");
    }
  }
}

export async function stopEventDrivenPipeline(): Promise<void> {
  inFlightEd.clear();
  inFlightBg.clear();
  completedEdRounds.clear();
  recoveryAttempts.clear();
  try {
    await nativeBcGameSocket.stop();
  } catch {
    /* soft */
  }
  bcGameSocket.disconnect();
}

export function wireGameEventHandlers(): void {
  initializeEventHandlers();
}
