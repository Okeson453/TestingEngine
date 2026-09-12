/**
 * BC.Game native WS → live prediction pipeline.
 *
 * Directive 2026-09-12 PR-primary:
 *   PR(N)  → PRIMARY N+1 prediction at betting-open (~7s before BG)
 *   BG(N)  → CONFIRM/reconcile (began_at, temporal kill); N+1 only if PR missed
 *   ED(N)  → FALLBACK N+1 only if primary did not own target; validation of N
 * Opt out of primary tier: ED_PRIMARY_PREDICT=1 or BG_PRIMARY_PREDICT=0
 * Opt out of PR trigger only: PR_PRIMARY_PREDICT=0 (BG remains primary)
 * Poll   → recovery only
 */
import { randomUUID } from "node:crypto";
import { bcGameSocket } from "@/lib/crash/socket-client";
import { nativeBcGameSocket } from "@/lib/crash/native-socket-client";
import { prewarmSign } from "@/lib/crash/native-sign";
import { getRealtimePipeline, logRealtimeSnapshot } from "@/lib/realtime/realtime-pipeline";
import { getSql, getCriticalSql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";
import { onGameEnd } from "@/lib/prediction/live/validator";
import {
  attemptNPlusOnePrediction,
  bgPrimaryEnabled,
  prPrimaryEnabled,
} from "@/lib/prediction/live/prediction-attempt";
import {
  isBgOwnedOrTerminal,
  isPrimaryOwnedOrTerminal,
} from "@/lib/prediction/live/target-coordinator";
import { observeCrashForACIE } from "@/lib/prediction/live/predictor";
import { globalIncrementalState } from "@/lib/prediction/state/incremental-state-engine";
import {
  markLiveRoundEnded,
} from "@/lib/prediction/live/live-round-state";
import { appendCompletedRound } from "@/lib/prediction/live/live-history-buffer";
import { isAuthoritative } from "@/lib/prediction/live/fencing";
import {
  completeTarget,
  releaseTarget,
  reserveTargetForBg,
  reserveTargetForPr,
  markBgRunning,
  markPrRunning,
  isBgBlocking,
  peekClaim,
} from "@/lib/prediction/live/target-coordinator";
import {
  startTrace,
  mark,
  finishSignalReady,
  finishPersist,
  logLatencyBudgetSnapshot,
} from "@/lib/prediction/live/latency-trace";
import { syncDbClockOffset, shouldResyncClock } from "@/lib/prediction/live/clock-offset";
import { noteRoundEnded, noteRoundStarted, getRoundStartedAtMs } from "@/lib/prediction/live/live-round-registry";

const logger = getLogger("game-event-handlers");

const inFlightEd = new Set<string>();
const inFlightBg = new Set<string>();
const inFlightPr = new Set<string>();

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
 * BG(N): reconcile target start + temporal kill of late signals for N.
 *
 * Default (BG-primary): reserve + attemptNPlusOnePrediction for N+1 at
 * BG receipt (history/ACIE through N-1). ED is fallback when BG missed.
 *
 * Always:
 *  1. noteRoundStarted (zero-RTT registry)
 *  2. Stamp began_at / target_round_started_at / live_round_state
 *  3. Hard temporal kill of undelivered prediction outbox rows targeting N
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
  // ZERO-RTT registry write — synchronous, before any await. The dispatcher's
  // pre-send gate consults this so it never relies on the lagging DB writes.
  // ONLY the real BG (round start) writes startedAt — never pr.
  noteRoundStarted(gameId, new Date(beganAt).getTime());

  // BG-PRIMARY (latency): N+1 at BG so the signal is ready before the
  // target round starts. bgPrimaryEnabled() (prediction-attempt.ts) is the
  // single authoritative toggle; opt into ED-primary with ED_PRIMARY_PREDICT=1
  // or BG_PRIMARY_PREDICT=0.
  const targetGameIdForBg = nextTargetGameId(gameId);
  const bgReserveAt = Date.now();
  let bgReserved = false;
  const bgPrimaryPredict = bgPrimaryEnabled();
  if (bgPrimaryPredict && isAuthoritative() && /^\d+$/.test(gameId)) {
    const reserve = reserveTargetForBg(targetGameIdForBg, gameId);
    bgReserved = reserve.owned;
    logger.info(
      {
        component: "game-event-handlers",
        event: "bg",
        gameId,
        targetGameId: targetGameIdForBg,
        ownership: reserve.owned ? "RESERVED_BG" : reserve.reason,
        owner: reserve.owned ? `bg:${gameId}` : reserve.owner,
        state: reserve.owned ? reserve.state : reserve.state,
        bg_receipt_to_reserve_ms: Math.max(0, bgReserveAt - new Date(receivedAt).getTime()),
        correlationId,
      },
      reserve.owned
        ? `BG→N+1 ownership RESERVED (BG-primary — single authoritative path) target=${targetGameIdForBg} correlation=${correlationId}`
        : `BG→N+1 reserve skipped reason=${reserve.reason} target=${targetGameIdForBg} correlation=${correlationId}`,
    );
  }

  try {
    // PASS 15 OVERLAP TIMING: reconcile duration is captured when its
    // continuation resumes; the concurrent attempt reads it at profile
    // emission.
    const reconcileT0 = Date.now();
    let reconcileMs: number | null = null;

    // ── PRIMARY N+1 PREDICTION TRIGGER (sep 11 architecture change) ──
    // Round N has just started; its N+1 prediction is generated NOW, during
    // the round, instead of waiting for ED(N). Fire-and-forget, launched
    // CONCURRENTLY with the reconcile TX (pass 15 overlap): the attempt
    // shares ZERO data dependencies with the reconcile (disjoint rows —
    // target N+1 vs round N; claim/temporal gates are memory-only; history
    // is the in-process buffer), so serializing them cost one full Neon
    // RTT of pure ordering. Ownership is reserved synchronously BEFORE
    // both; the dispatcher pre-send gate remains the backstop if the
    // reconcile fails after retry. attemptNPlusOnePrediction is the
    // SINGLE ownership boundary — atomic claim inside; if any other trigger
    // (duplicate BG, WS+poll race, ED fallback) already owns target N+1,
    // this returns duplicate/completed and no compute happens.
    // Fencing: a worker that lost authority must not compute predictions.
    // Numeric-ID gate: BG reconciliation is exercised by tests with
    // non-round IDs; prediction targets are strictly numeric sequences.
    // LATENCY PROFILE (sep 11 P1): production measured ~1.24s between BG
    // receipt and "reconcile complete" — the reconcile TX (pool acquire +
    // 5 statements), NOT the model, owns that cost. bg_receipt_to_reconcile
    // and bg_to_prediction_total below make the split visible per round.
    // PR-primary: if PR already reserved/completed N+1, BG is confirmation
    // only (reconcile + temporal kill below). Attempt N+1 only when PR missed.
    const primaryAlreadyOwns =
      isPrimaryOwnedOrTerminal(targetGameIdForBg) && !bgReserved;
    if (primaryAlreadyOwns) {
      logger.info(
        {
          component: "game-event-handlers",
          event: "bg",
          gameId,
          targetGameId: targetGameIdForBg,
          correlationId,
        },
        `BG confirmation only — N+1 already owned by primary (PR); reconcile continues target=${targetGameIdForBg}`,
      );
    }
    if (
      bgPrimaryPredict &&
      isAuthoritative() &&
      /^\d+$/.test(gameId) &&
      !primaryAlreadyOwns
    ) {
      void (async () => {
        const bgCorrelationId = `${correlationId}:bg-n1`;
        const bgTrace = startTrace(bgCorrelationId, gameId);
        const bgReceivedMs = new Date(receivedAt).getTime();
        const attemptT0 = Date.now();
        try {
          // Stage: ownership already reserved synchronously at handler entry.
          bgTrace.marks.ownership_reserved = bgReserveAt;
          bgTrace.marks.ws_received = bgReceivedMs;
          const result = await attemptNPlusOnePrediction({
            sourceRoundId: gameId,
            sourceCrashAt: beganAt,
            source: "BG",
            correlationId: bgCorrelationId,
            trace: bgTrace,
          });
          const predictionMs = Date.now() - attemptT0;
          const targetGameId = nextTargetGameId(gameId);
          // Timing split: event receipt → reconcile TX commit → prediction
          // done. bg_receipt_to_reconcile_ms isolates the DB reconcile cost
          // (pool pressure shows up here first); prediction_ms is the model
          // + persist leg.
          const profile = {
            component: "game-event-handlers",
            event: "bg",
            gameId,
            targetGameId,
            trigger: "BG_PRIMARY",
            kind: result.kind,
            // PASS 15: reconcile now OVERLAPS the attempt — the holder is
            // set by the reconcile continuation; if the attempt finishes
            // first, fall back to elapsed-at-emission (both honest).
            bg_receipt_to_reconcile_ms: reconcileMs ?? Math.max(
              0,
              Date.now() - bgReceivedMs - predictionMs,
            ),
            prediction_ms: predictionMs,
            bg_receipt_to_prediction_done_ms: Math.max(0, Date.now() - bgReceivedMs),
            predictionId: result.predictionId,
            correlationId: bgCorrelationId,
          };
          if (result.attempted) {
            completeTarget(targetGameId, `bg:${gameId}`);
            // Independent stage samples for BG critical path forensics.
            const totalMs = finishSignalReady(bgTrace);
            const stageBreakdown: Record<string, number | null> = {};
            const marks = bgTrace.marks;
            const pairs: Array<[string, keyof typeof marks, keyof typeof marks]> = [
              ["receipt_to_ownership", "ws_received", "ownership_reserved"],
              ["ownership_to_claim", "ownership_reserved", "target_claimed"],
              ["claim_to_state", "target_claimed", "state_acquired"],
              ["state_to_predict", "state_acquired", "prediction_started"],
              ["prediction_compute", "prediction_started", "prediction_completed"],
              ["gates_to_signal", "gates_passed", "signal_ready"],
            ];
            for (const [name, a, b] of pairs) {
              const ta = marks[a];
              const tb = marks[b];
              stageBreakdown[name] =
                ta != null && tb != null ? Math.round(tb - ta) : null;
            }
            logger.info(
              { ...profile, total_signal_ms: Math.round(totalMs), stages: stageBreakdown },
              `BG→N+1 SIGNAL_READY (primary path — durable outbox enqueued) [reconcile=${profile.bg_receipt_to_reconcile_ms}ms prediction=${profile.prediction_ms}ms total=${profile.bg_receipt_to_prediction_done_ms}ms]`,
            );
          } else {
            // P0 state semantics (sep 11): skipped_no_edge is an EVALUATED,
            // TERMINAL NO_BET — the claim was completed, not released, so ED
            // must not recompute. Only genuine failures (exception:*,
            // insufficient_history, persist_failed, skipped window) release
            // the claim and leave the target recoverable by ED fallback.
            if (result.kind !== "skipped_no_edge") {
              releaseTarget(targetGameId, `bg:${gameId}`);
            }
            logger.info(
              { ...profile, terminal_no_bet: result.kind === "skipped_no_edge" },
              result.kind === "duplicate"
                ? `BG→N+1 already claimed/persisted by another trigger [reconcile=${profile.bg_receipt_to_reconcile_ms}ms prediction=${profile.prediction_ms}ms total=${profile.bg_receipt_to_prediction_done_ms}ms]`
                : result.kind === "skipped_no_edge"
                  ? `BG→N+1 evaluated NO_BET (terminal — ED will not recompute) [reconcile=${profile.bg_receipt_to_reconcile_ms}ms prediction=${profile.prediction_ms}ms total=${profile.bg_receipt_to_prediction_done_ms}ms]`
                  : `BG→N+1 soft result kind=${result.kind} — target recoverable by ED fallback [reconcile=${profile.bg_receipt_to_reconcile_ms}ms prediction=${profile.prediction_ms}ms total=${profile.bg_receipt_to_prediction_done_ms}ms]`,
            );
          }
        } catch (err) {
          const e = err instanceof Error ? err : new Error(String(err));
          logger.error(
            {
              component: "game-event-handlers",
              event: "bg",
              gameId,
              trigger: "BG_PRIMARY",
              errorName: e.name,
              errorMessage: e.message,
              errorStack: e.stack?.slice(0, 1500) ?? null,
            },
            "BG→N+1 primary prediction attempt threw — ED(N) remains fallback",
          );
        }
      })();
    }


    // PASS 5 (sep 12, 11:23Z window): the 189-208ms reconcile measurements
    // were CRITICAL-POOL ACQUIRE WAIT, not query time — this handler's CTE
    // shared the critical pool (max 4, 2 permanently checked out by the
    // pinned persist/dispatch lanes) with validation TXs and fallback runs.
    // Split by criticality:
    //   - TEMPORAL KILL (ob): safety/temporal gate — stays on the critical
    //     pool, awaited, concurrent with the attempt. It dead-letters every
    //     undelivered prediction targeting the round that just started; the
    //     dispatcher's claim-time auth is the independent backstop.
    //   - LIFECYCLE (cr/pp/lrs): began_at backfill, target stamp,
    //     live_round_state upsert — idempotent bookkeeping, required for
    //     correctness but NOT for the handoff or the kill. Moves to the
    //     GENERAL pool, detached, so it can never contest a critical slot.
    // Acquire-vs-query timing is logged per leg so the next slow window is
    // attributable by construction (directive: no unmeasured attribution).
    const beganParam = new Date(beganAt);

    // ── Leg 1: temporal kill (critical pool, awaited) ──
    const killAcquireT0 = Date.now();
    const killSql = await getCriticalSql();
    const killAcquireMs = Date.now() - killAcquireT0;
    const killQueryT0 = Date.now();
    let signalsKilled = 0;
    try {
      const killRows = await killSql`
        UPDATE notification_outbox
        SET status = 'dead_letter',
            last_error = 'expired_late_signal: target round started (BG received)'
        WHERE type = 'prediction'
          AND status IN ('pending', 'inflight')
          AND target_game_id = ${gameId}
        RETURNING 1
      `;
      signalsKilled = killRows.length;
    } catch (killErr1) {
      logger.warn(
        { event: "bg", gameId, error: String(killErr1), attempt: 1 },
        `BG temporal kill failed — retrying once: ${String(killErr1).slice(0, 300)}`,
      );
      try {
        const killRows = await killSql`
          UPDATE notification_outbox
          SET status = 'dead_letter',
              last_error = 'expired_late_signal: target round started (BG received)'
          WHERE type = 'prediction'
            AND status IN ('pending', 'inflight')
            AND target_game_id = ${gameId}
          RETURNING 1
        `;
        signalsKilled = killRows.length;
      } catch (killErr2) {
        logger.error(
          { event: "bg", gameId, error: String(killErr2), attempt: 2 },
          `BG temporal kill FAILED after retry — dispatcher claim-time auth is the backstop: ${String(killErr2).slice(0, 300)}`,
        );
      }
    }
    reconcileMs = Date.now() - reconcileT0;
    const killQueryMs = Date.now() - killQueryT0;

    // ── Leg 2: lifecycle bookkeeping (general pool, detached) ──
    void (async () => {
      try {
        const lifecycleT0 = Date.now();
        const generalSql = await getSql();
        const lifecycleAcquireMs = Date.now() - lifecycleT0;
        const lifecycleQueryT0 = Date.now();
        const rows = await generalSql`
          WITH cr AS (
            UPDATE crash_rounds
            SET began_at = COALESCE(began_at, ${beganParam})
            WHERE game_id = ${gameId}
            RETURNING 1
          ),
          pp AS (
            UPDATE pending_predictions
            SET target_round_started_at = COALESCE(target_round_started_at, ${beganParam})
            WHERE target_game_id = ${gameId}
              AND matched = false
            RETURNING 1
          ),
          lrs AS (
            INSERT INTO live_round_state (
              game_id, lifecycle, began_at, source, correlation_id, updated_at
            ) VALUES (
              ${gameId}, 'STARTED', ${beganParam}, 'socket', ${correlationId}, now()
            )
            ON CONFLICT (game_id) DO UPDATE SET
              lifecycle = CASE
                WHEN live_round_state.lifecycle IN ('DISCOVERED') THEN 'STARTED'
                WHEN live_round_state.lifecycle IN ('STARTED', 'RUNNING', 'ENDED', 'RECONCILED')
                  THEN live_round_state.lifecycle
                ELSE 'STARTED'
              END,
              began_at = COALESCE(live_round_state.began_at, EXCLUDED.began_at),
              source = CASE
                WHEN live_round_state.source = 'socket' THEN live_round_state.source
                ELSE EXCLUDED.source
              END,
              correlation_id = COALESCE(live_round_state.correlation_id, EXCLUDED.correlation_id),
              updated_at = now()
          )
          SELECT
            (SELECT count(*) FROM cr) AS crash_backfilled,
            (SELECT count(*) FROM pp) AS targets_stamped
        `;
        const lifecycleQueryMs = Date.now() - lifecycleQueryT0;
        logger.info(
          {
            event: "bg",
            gameId,
            correlationId,
            leg: "lifecycle",
            crash_backfilled: Number(rows[0]?.crash_backfilled ?? 0),
            targets_stamped: Number(rows[0]?.targets_stamped ?? 0),
            lifecycleAcquireMs,
            lifecycleQueryMs,
          },
          "bg lifecycle reconciled (general pool — post-critical-path)",
        );
      } catch (lifecycleErr) {
        logger.warn(
          { event: "bg", gameId, error: String(lifecycleErr) },
          "bg lifecycle reconcile failed (idempotent — next BG event or poll recovery restamps)",
        );
      }
    })();

    // CAN-BE-DEFERRED: live_event_log audit row — not required for ownership,
    // temporal kill, prediction correctness, or crash recovery. Runs on the
    // general pool so it never contends with BG→N+1 persist / outbox handoff.
    setImmediate(() => {
      void (async () => {
        try {
          const generalSql = await getSql();
          await generalSql`
            INSERT INTO live_event_log (
              correlation_id, event_kind, game_id, payload, received_at, processed_at,
              processor_latency_ms, sla_violated
            ) VALUES (
              ${correlationId}::text, 'BG', ${gameId},
              ${JSON.stringify({ beganAt, reconcileOnly: false, predictionTrigger: "BG_PRIMARY" })},
              ${receivedAt}::timestamptz, now(), ${processorLatencyMs}, false
            ) ON CONFLICT DO NOTHING
          `;
        } catch {
          /* soft — audit only */
        }
      })();
    });

    // NON-FATAL BG RECONCILE TELEMETRY (unchanged): analytics and in-memory
    // registry work must never gate (or roll back with) the temporal kill.
    import("@/lib/prediction/identity/prediction-registry")
      .then(({ globalPredictionRegistry }) => {
        globalPredictionRegistry.noteTargetStarted(gameId, beganAt);
      })
      .catch(() => undefined);

    // P4 (sep 12 pass 4): forensics reclassify is analytics — defer it out of
    // the round-start window where the reconcile CTE, BG persist and dispatcher
    // claim all run. setImmediate still collided with that burst in prod.
    const reclassifyTimer = setTimeout(() => {
      void (async () => {
        const { reclassifyOnTargetStart } = await import(
          "@/lib/prediction/live/delivery-forensics"
        );
        // Telemetry, not realtime: deliberately on the GENERAL pool so the
        // forensic reclassify can never queue behind / steal critical slots.
        const generalSql = await getSql();
        await reclassifyOnTargetStart(generalSql, gameId, beganAt);
      })().catch(() => {
        /* soft */
      });
    }, Math.max(0, Number(process.env.FORENSICS_DEFER_MS ?? 2_500)));
    reclassifyTimer.unref?.();

    logger.info(
      {
        event: "bg",
        gameId,
        correlationId,
        // Event receipt → temporal kill committed. Production measured
        // ~1.24s here (pool contention) vs ~2ms of model time — this
        // field is the per-round proof of where the cost sits.
        bg_receipt_to_reconcile_ms: Math.max(0, Date.now() - new Date(receivedAt).getTime()),
        // PASS 5: acquire-vs-query split for the kill leg — if this is slow
        // again, these two fields decide contention vs network with no
        // inference.
        kill_acquire_ms: killAcquireMs,
        kill_query_ms: killQueryMs,
        // signals_killed>0 is the proof the temporal kill actually ran.
        signals_killed: signalsKilled,
      },
      "bg reconcile complete — kill committed; lifecycle leg detached (general pool)",
    );
    // Railway strips JSON fields — the acquire-vs-query split must be in the
    // MESSAGE to be readable from raw logs (that split is the P1 evidence).
    console.log(
      `[bg] kill leg: acquire_ms=${killAcquireMs} query_ms=${killQueryMs} killed=${signalsKilled} (acquire≈RTT-floor when 0-wait; acquire≫query ⇒ pool contention)`,
    );
  } catch (error) {
    logger.error({ event: "bg", gameId, error: String(error) }, "bg observability failed");
  } finally {
    inFlightBg.delete(gameId);
  }
}

/**
 * PR (prepare — betting opens) — PRIMARY N+1 prediction trigger.
 *
 * Directive 2026-09-12: promote pr to authoritative N+1 trigger so the
 * signal is generated ~7s before BG (upstream BC.Game betting window), not after
 * round start. Does NOT write began_at or noteRoundStarted — BG remains
 * the sole round-start authority for temporal kill / registry. Does NOT
 * run temporal kill (that would dead-letter signals ~7s early).
 *
 * Ownership: reserveTargetForPr → attemptNPlusOnePrediction(source=PR).
 * BG later becomes confirmation; ED remains fallback only.
 */
export async function prHandler(payload: unknown): Promise<void> {
  const gameId = extractLastGameId(payload);
  if (!gameId) return;
  if (inFlightPr.has(gameId)) return;
  inFlightPr.add(gameId);

  const correlationId = randomUUID();
  const p = (payload ?? {}) as Record<string, unknown>;
  const beginAt =
    toIsoString((p.beginTime ?? p.beganAt) as number | string | undefined) ??
    new Date().toISOString();
  const receivedAt = new Date().toISOString();
  const processorLatencyMs = Math.max(
    0,
    new Date(receivedAt).getTime() - new Date(beginAt).getTime(),
  );

  const targetGameId = nextTargetGameId(gameId);
  const prPrimary = prPrimaryEnabled();
  const prReserveAt = Date.now();
  let prReserved = false;

  if (prPrimary && isAuthoritative() && /^\d+$/.test(gameId)) {
    const reserve = reserveTargetForPr(targetGameId, gameId);
    prReserved = reserve.owned;
    logger.info(
      {
        component: "game-event-handlers",
        event: "pr",
        gameId,
        targetGameId,
        ownership: reserve.owned ? "RESERVED_PR" : reserve.reason,
        owner: reserve.owned ? `pr:${gameId}` : reserve.owner,
        state: reserve.state,
        correlationId,
      },
      reserve.owned
        ? `PR→N+1 ownership RESERVED (PR-primary) target=${targetGameId} correlation=${correlationId}`
        : `PR→N+1 reserve skipped reason=${reserve.reason} target=${targetGameId} correlation=${correlationId}`,
    );
  }

  try {
    // Observability only — general pool, must not block primary path.
    void (async () => {
      try {
        const sql = await getSql();
        await sql`
          INSERT INTO live_event_log (
            correlation_id, event_kind, game_id, payload, received_at, processed_at,
            processor_latency_ms, sla_violated
          ) VALUES (
            ${correlationId}::text, 'PR', ${gameId}, ${JSON.stringify({ beginAt })},
            ${receivedAt}::timestamptz, now(), ${processorLatencyMs}, false
          ) ON CONFLICT DO NOTHING
        `;
      } catch (error) {
        logger.warn({ event: "pr", gameId, error: String(error) }, "pr event log failed");
      }
    })();

    logger.info(
      { event: "pr", gameId, correlationId, targetGameId, prReserved },
      "bc pr (betting-open) observed — PR-primary N+1 trigger; expect bg ~7s later (upstream window)",
    );

    // PRIMARY N+1: fire concurrently with the detached log write.
    if (prPrimary && isAuthoritative() && /^\d+$/.test(gameId)) {
      void (async () => {
        const prCorrelationId = `${correlationId}:pr-n1`;
        const prTrace = startTrace(prCorrelationId, gameId);
        const attemptT0 = Date.now();
        try {
          prTrace.marks.ownership_reserved = prReserveAt;
          prTrace.marks.ws_received = new Date(receivedAt).getTime();
          if (prReserved) {
            markPrRunning(targetGameId, `pr:${gameId}`);
          }
          const result = await attemptNPlusOnePrediction({
            sourceRoundId: gameId,
            sourceCrashAt: beginAt,
            source: "PR",
            correlationId: prCorrelationId,
            trace: prTrace,
          });
          const predictionMs = Date.now() - attemptT0;
          logger.info(
            {
              component: "game-event-handlers",
              event: "pr",
              gameId,
              targetGameId: result.targetGameId ?? targetGameId,
              kind: result.kind,
              predictionId: result.predictionId,
              attempted: result.attempted,
              prediction_ms: predictionMs,
              pr_receipt_to_prediction_ms: Math.max(
                0,
                Date.now() - new Date(receivedAt).getTime(),
              ),
              correlationId: prCorrelationId,
            },
            `PR→N+1 attempt done kind=${result.kind ?? "null"} prediction_ms=${predictionMs} target=${result.targetGameId ?? targetGameId}`,
          );
        } catch (err) {
          logger.error(
            { event: "pr", gameId, error: String(err) },
            `PR→N+1 attempt failed: ${String(err).slice(0, 300)}`,
          );
        }
      })();
    }
  } finally {
    inFlightPr.delete(gameId);
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
 * ED(N): FALLBACK N+1 when BG missed ownership; always validates outcome N.
 * Crash N is observed, then N+1 is predicted and enqueued for immediate
 * delivery so the signal is ready before BG(N+1). attemptNPlusOnePrediction
 * remains the sole ownership boundary (dedup vs poll recovery / optional
 * ED_PRIMARY_PREDICT). BG still stamps start + temporal-kills late
 * signals for the started target.
 */
export async function edHandler(payload: unknown): Promise<void> {
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
  // ED RECEIPT ANCHOR: the earliest in-process instant for this authoritative
  // crash event. Threads through the attempt chain into outbox metadata as
  // ed_received_at — the start of the measured ED→Telegram critical path.
  const edReceivedAt = new Date().toISOString();
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
  // ZERO-RTT registry write — synchronous, before any await. crash_rounds is
  // only persisted DETACHED after the N+1 attempt completes (1-3s later); the
  // dispatcher's pre-send gate consults this registry in the meantime so a
  // late signal can never deliver into a round that already crashed. Receipt
  // time is used deliberately: protocol crash timestamps are decode-clock
  // anyway (native-protocol endTime = Date.now()).
  noteRoundEnded(gameId);

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
      // FINAL_REPORT-2 #1: gap feature needs round-start times. BG events
      // stamp the registry synchronously at bgHandler entry (authoritative).
      const beganMs = getRoundStartedAtMs(gameId);
      if (beganMs != null) globalIncrementalState.recordBeganAt(beganMs);
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
    // SEP 11 FIX (production 14:28:18 STALE_REJECTED): learning is not
    // ownership. When BG already claimed/decided target N+1 (duplicate or
    // terminal NO_BET), onGameEndPredict returns early WITHOUT observing
    // round N — so ACIE last-observed lagged the history buffer tail and the
    // next BG prediction was rejected as stale. Observe every authoritative
    // crash here, unconditionally, before the ownership boundary. The helper
    // is idempotent per round, so the owned path's internal observe is a
    // no-op, and the bgTrigger mode in onGameEndPredict never double-counts.
    try {
      observeCrashForACIE(gameId, multiplier, crashedAt);
    } catch {
      /* soft — never block the prediction path on observation */
    }
    mark(trace, "state_updated");

    const targetGameId = nextTargetGameId(gameId);
    trace.targetGameId = targetGameId;
    // BG-PRIMARY: ED must not race BG for N+1. Skip predict when BG already
    // reserved/owned/terminal; validation of N still runs below.
    const bgOwnsTarget = isBgOwnedOrTerminal(targetGameId);
    mark(trace, "target_claimed");

    if (bgOwnsTarget) {
      logger.info(
        {
          component: "game-event-handlers",
          event: sourceEvent,
          gameId,
          targetGameId,
          ownership_result: "bg_primary_skip_ed_predict",
        },
        "ED→N+1 skipped — BG already owns target (ED is fallback only)",
      );
    }

    try {
      if (bgOwnsTarget) {
        // Fall through to validation-only path below without predicting.
        throw new Error("__BG_OWNED_SKIP_PREDICT__");
      }
      const result = await attemptNPlusOnePrediction({
        sourceRoundId: gameId,
        sourceCrashAt: crashedAt,
        sourceMultiplier: multiplier,
        source: "ED",
        correlationId,
        trace,
        edReceivedAt,
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
            ed_received_at: edReceivedAt,
            ed_to_signal_ms: Math.round(totalMs * 100) / 100,
            correlationId,
          },
          result.kind === "predicted"
            ? "ED→N+1 SIGNAL_READY (durable outbox enqueued)"
            : `ED→N+1 result kind=${result.kind}`,
        );
      } else {
        // soft miss / duplicate / insufficient_history / exception handled in attempt
        // P0 state semantics (sep 11): skipped_no_edge is a TERMINAL NO_BET —
        // the predictor completed the claim with a NO_BET decision; releasing
        // it here would re-open the target for pointless recomputes. Failures
        // still release and stay recoverable (immediate recovery below).
        if (result.kind !== "skipped_no_edge") {
          releaseTarget(targetGameId, `ed:${gameId}`);
        }
        logger.info(
          {
            event: sourceEvent,
            gameId,
            targetGameId,
            ownership_result:
              result.kind === "duplicate"
                ? "already_persisted"
                : result.kind === "skipped_no_edge"
                  ? "terminal_no_bet"
                  : `soft:${result.kind ?? "unknown"}`,
            kind: result.kind,
            ed_to_signal_ms: Math.round(totalMs * 100) / 100,
            correlationId,
          },
          result.kind === "skipped_no_edge"
            ? "ED→N+1 evaluated NO_BET (terminal — target closed)"
            : "ED→N+1 soft result",
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
      const err = error instanceof Error ? error : new Error(String(error));
      if (err.message === "__BG_OWNED_SKIP_PREDICT__") {
        // intentional — BG primary owns N+1
      } else {
        // attemptNPlusOnePrediction already swallows and returns; this is defensive.
        releaseTarget(targetGameId, `ed:${gameId}`);
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
    }

    // --- P2 DURABILITY / validation async (must not block signal) ---
    void (async () => {
      mark(trace, "persist_started");
      try {
        const sql = await getSql();
        const crashedAtDate = new Date(crashedAt);
        // began_at NULL until BG arrives (BG is the authoritative round start)
        if (!Number.isNaN(crashedAtDate.getTime())) {
          // Column is `salt` (not seed) — prior INSERT failed silently every ED.
          await sql`
            INSERT INTO crash_rounds (game_id, multiplier, hash, salt, began_at, crashed_at)
            VALUES (${gameId}, ${multiplier}, null, null, null, ${crashedAtDate})
            ON CONFLICT (game_id) DO UPDATE SET
              multiplier = EXCLUDED.multiplier,
              crashed_at = COALESCE(EXCLUDED.crashed_at, crash_rounds.crashed_at)
          `.catch(() => undefined);
          try {
            const { globalRecentRoundCache } = await import(
              "@/lib/observability/performance/hot-cache"
            );
            globalRecentRoundCache.set({
              gameId,
              multiplier,
              crashedAt,
            });
          } catch { /* soft */ }
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
          skipPredict: true, // N+1 already attempted above on ED primary path
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
    if (ev.event === "bg") {
      void bgHandler({
        gameId: ev.gameId,
        beginTime: ev.beginTime ?? ev.receivedAt,
        beganAt: ev.beginTime ?? ev.receivedAt,
      });
    } else if (ev.event === "pr") {
      // PR-primary: betting-open triggers N+1 prediction (~7s before BG).
      // prHandler does NOT write began_at / registry / temporal kill —
      // BG remains sole round-start authority for those.
      void prHandler({
        gameId: ev.gameId,
        beginTime: ev.beginTime ?? ev.receivedAt,
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

  logger.info({ component: "game-event-handlers" }, "event handlers wired (BG-primary N+1, ED fallback + validation)");
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
