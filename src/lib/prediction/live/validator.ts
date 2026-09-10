/**
 * Synchronous-on-event validator.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.3
 *
 * `onGameEnd` resolves the pending prediction for the just-completed round,
 * writing the WIN/LOSS result, updating the pending row to `matched=true`,
 * and enqueuing a validation Telegram notification — all in a single
 * transaction.
 *
 * Race handling (spec §7.3 step 4): the `bg` (begin) event for round N+1
 * may arrive AFTER the `ed` (end) event for round N (out-of-order delivery
 * on the Socket.IO stream, especially after a reconnect). The validator
 * returns `{ kind: 'bg_arrived_late' }` and commits a small recovery row
 * so the next `ed` re-emission (BC.Game occasionally re-broadcasts on
 * reconnect) can complete the validation.
 */
import { randomUUID } from "node:crypto";
import { getSql, type Sql } from "@/lib/db";
import { runInTransaction } from "@/lib/prediction/live/tx";
import { getConfiguredChatIds } from "@/lib/notifications/telegram";
import { getLogger } from "@/lib/observability/logger";
import { onGameEndPredict } from "@/lib/prediction/live/predictor";
import { globalIncrementalState } from "@/lib/prediction/state/incremental-state-engine";
import { markPredictionResolved } from "@/lib/prediction/live/live-round-state";
import { edProcessingLatencyMs } from "@/lib/observability/metrics/lifecycle-metrics";
import { processResolvedPredictionFeedback } from "@/lib/prediction/live/feedback";

const logger = getLogger("live-validator");

/** Spec §2 / §3.2: always attempt N+1 prediction after Round N is known.
 *  Non-blocking; failures never affect validation. Enables cold-start first
 *  prediction when there was no prior pending row for N. */

/**
 * Generate N+1 after learning for N has completed (audit §40).
 * Still non-fatal: prediction failure does not roll back validation.
 */
async function triggerNextPrediction(
  gameId: string,
  endTime: string,
  multiplier: number,
  correlationId: string | null | undefined,
): Promise<void> {
  const corr = correlationId ?? randomUUID();
  try {
    await onGameEndPredict(gameId, endTime, multiplier, corr);
  } catch (e) {
    logger.error(
      { gameId, error: String(e) },
      "Failed to generate N+1 prediction after ed processing",
    );
  }
}

/**
 * Latency report §4.1: the N+1 prediction must NOT be awaited on the ed
 * critical path. Validation of round N and generation of the prediction for
 * round N+1 are independent transactions; awaiting one after the other
 * serialized two DB pipelines and delayed the signal release.
 *
 * Correctness is preserved because:
 * - `globalIncrementalState.update(N)` runs (synchronously awaited) BEFORE
 *   the prediction is scheduled, so the model already observed round N;
 * - `onGameEndPredict` runs its own transaction with the temporal-invariant
 *   check (`prediction_generated_at < target_round_started_at`) and aborts
 *   if the target round has already started;
 * - failures are caught inside `triggerNextPrediction` and never affect the
 *   already-committed validation.
 *
 * In-flight predictions are tracked so tests and graceful shutdown can await
 * them deterministically via `waitForInFlightPredictions()`.
 */
const inFlightPredictions = new Set<Promise<void>>();

export async function waitForInFlightPredictions(): Promise<void> {
  while (inFlightPredictions.size > 0) {
    await Promise.all([...inFlightPredictions]);
  }
}

function scheduleNextPrediction(
  gameId: string,
  endTime: string,
  multiplier: number,
  correlationId: string | null | undefined,
): void {
  const p = triggerNextPrediction(gameId, endTime, multiplier, correlationId).finally(() => {
    inFlightPredictions.delete(p);
  });
  inFlightPredictions.add(p);
  // triggerNextPrediction never rejects (errors are caught inside), but guard
  // against future changes introducing an unhandled rejection.
  p.catch(() => {});
}

export interface GameEndEvent {
  gameId: string;
  endTime: string;
  multiplier: number;
  receivedAt: string;
  /**
   * When true (poll recovery path), validation runs but N+1 prediction is
   * NOT auto-triggered. Poll worker decides at most one newest-round attempt.
   * Spec: Diagnosis §3 — eliminate poll-batch prediction cascade.
   */
  skipPredict?: boolean;
  /**
   * When true, caller already applied globalIncrementalState.update for this
   * crash (parallel ED path). Skip duplicate state updates.
   */
  skipStateUpdate?: boolean;
}

export type OnGameEndResult =
  | {
      kind: "resolved";
      predictionId: string;
      targetGameId: string;
      result: "WIN" | "LOSS";
      targetMultiplier: number;
      actualMultiplier: number;
      resolvedAt: string;
      alreadyValidated: boolean;
      outboxEnqueued: number;
      correlationId: string;
    }
  | { kind: "bg_arrived_late"; targetGameId: string }
  | { kind: "orphaned"; targetGameId: string };

interface ValidatorDeps {
  getSqlFn?: () => Promise<Sql>;
  getChatIds?: () => string[];
  now?: () => number;
}

interface PendingRow {
  prediction_id: string;
  target_multiplier: string | number;
  probability: string | number;
  confidence: string | number;
  regime_name: string | null;
  correlation_id: string | null;
  requested_at: string | Date;
}

export async function onGameEnd(
  evt: GameEndEvent,
  deps: ValidatorDeps = {},
): Promise<OnGameEndResult> {
  const getSqlFn = deps.getSqlFn ?? getSql;
  const getChatIds = deps.getChatIds ?? getConfiguredChatIds;
  const now = deps.now ?? Date.now;

  // Fire N+1 prediction IMMEDIATELY — do not wait for validation TX.
  // Under live WS, validation of N and predict N+1 must race in parallel or
  // residual window collapses (logs: "too late" / "tight residual").
  if (!evt.skipPredict) {
    try {
      globalIncrementalState.update(evt.multiplier);
    } catch {
      /* soft */
    }
    scheduleNextPrediction(evt.gameId, evt.endTime, evt.multiplier, null);
  }

  const sql = await getSqlFn();

  // Step 1+2: anchor the round's crashed_at, then SELECT … FOR UPDATE
  // SKIP LOCKED to claim the pending row.
  const state: {
    pending: PendingRow | null;
    crashRow: { began_at: string | Date | null; crashed_at: string | Date | null } | null;
  } = { pending: null, crashRow: null };

  try {
    await runInTransaction(sql, async (tx) => {
      // First, anchor the round's crash outcome. The row may not exist
      // yet (the predictor doesn't pre-insert crash_rounds because the
      // schema requires multiplier+crashed_at to be NOT NULL — both
      // arrive on this ed event). Use UPSERT with RETURNING for idempotency
      // and to avoid a separate SELECT (P2.2: RETURNING clause).
      {
        const endDate = new Date(evt.endTime);
        const crashedParam = Number.isNaN(endDate.getTime()) ? new Date() : endDate;
        // Fix 3: do NOT fabricate began_at = crash - 3s. When ED arrives
        // before BG, began_at stays NULL; the BG handler is the ONLY
        // authoritative source of the real round start (COALESCE backfill).
        const beganParam: Date | null = null;
        const upserted = await tx<{
          began_at: string | Date | null;
          crashed_at: string | Date | null;
        }>`
          insert into crash_rounds (game_id, multiplier, hash, salt, began_at, crashed_at)
          values (
            ${evt.gameId}, ${evt.multiplier}, null, null,
            ${beganParam},
            ${crashedParam}
          )
          on conflict (game_id) do update
            set crashed_at = excluded.crashed_at,
                multiplier = excluded.multiplier
            where crash_rounds.crashed_at is null
          returning began_at, crashed_at
        `;
        state.crashRow = upserted[0] ?? null;
        // If conflict (row existed with crashed_at already set), RETURNING is empty.
        // Fall back to a single SELECT only in that case.
        if (upserted.length === 0) {
          const fetched = await tx<{
            began_at: string | Date | null;
            crashed_at: string | Date | null;
          }>`
            select began_at, crashed_at
            from crash_rounds
            where game_id = ${evt.gameId}
            limit 1
          `;
          state.crashRow = fetched[0] ?? null;
        }
      }

      const lockedRows = await tx<PendingRow>`
        select prediction_id, target_multiplier, probability, confidence,
               regime_name, correlation_id, requested_at
        from pending_predictions
        where target_game_id = ${evt.gameId} and matched = false
        limit 1
        for update skip locked
      `;
      if (lockedRows.length > 0) {
        state.pending = lockedRows[0]!;
      } else {
        // Detect: was there a row that was matched already (recovery re-pass)?
        const matchedRows = await tx<{ prediction_id: string }>`
          select prediction_id from prediction_validations
          where game_id = ${evt.gameId}
          limit 1
        `;
        if (matchedRows.length > 0) {
          // Already validated; record live_event_log and return.
          await tx`
            insert into live_event_log (
              correlation_id, event_kind, game_id, payload, received_at, processed_at,
              processor_latency_ms, sla_violated
            ) values (
              ${randomUUID()}::text, 'ED', ${evt.gameId},
              ${JSON.stringify({ endTime: evt.endTime, multiplier: evt.multiplier })},
              ${evt.receivedAt}::timestamptz, now(), 0, false
            )
            on conflict do nothing
          `;
        } else if (!state.crashRow || state.crashRow.began_at == null) {
          // No crash_rounds.began_at → the bg event was missed entirely.
          // Mark as orphaned for the poll-worker to clean up later.
          await tx`
            update crash_rounds
            set crashed_at = coalesce(crashed_at, ${evt.endTime}::timestamptz)
            where game_id = ${evt.gameId}
          `;
        }
        return;
      }

      if (state.pending == null) return;

      const target = Number(state.pending!.target_multiplier);
      const result: "WIN" | "LOSS" = evt.multiplier >= target ? "WIN" : "LOSS";
      const resolvedAt = new Date(now()).toISOString();

      const ins = await tx<{ prediction_id: string }>`
        insert into prediction_validations (
          prediction_id, game_id, target_multiplier, predicted_probability,
          predicted_confidence, actual_multiplier, result, model_version,
          regime_name, requested_at, resolved_at
        ) values (
          ${state.pending!.prediction_id}, ${evt.gameId}, ${target},
          ${Number(state.pending!.probability)}, ${Number(state.pending!.confidence)},
          ${evt.multiplier}, ${result}, 'v1',
          ${state.pending!.regime_name},
          ${
            state.pending!.requested_at instanceof Date
              ? state.pending!.requested_at.toISOString()
              : String(state.pending!.requested_at)
          },
          ${resolvedAt}
        )
        on conflict on constraint prediction_validations_prediction_id_key do nothing
        returning prediction_id
      `;
      const alreadyValidated = ins.length === 0;

      if (!alreadyValidated) {
        await tx`
          update pending_predictions
          set matched = true,
              matched_game_id = ${evt.gameId},
              matched_at = ${resolvedAt},
              status = 'MATCHED'
          where prediction_id = ${state.pending!.prediction_id}
        `;
        // ONE outbox row per validation event.
        // sendTelegramMessage() broadcasts to all configured chats — do NOT
        // insert one row per chat (that caused N×M duplicate deliveries).
        // Use the same WIN/LOSS formatter as createValidationNotification.
        const resultEmoji = result === "WIN" ? "🎉" : "💥";
        const multiplierText =
          evt.multiplier >= target
            ? `Actual: ${evt.multiplier.toFixed(2)}x`
            : `Crashed: ${evt.multiplier.toFixed(2)}x`;
        const validationContent = [
          `${resultEmoji} PREDICTION ${result}`,
          ``,
          `Target: ${target.toFixed(2)}x`,
          multiplierText,
          `Probability: ${(Number(state.pending!.probability) * 100).toFixed(1)}%`,
          ``,
          `Game ID: ${evt.gameId}`,
          `Prediction ID: ${state.pending!.prediction_id}`,
          `Resolved: ${resolvedAt}`,
        ].join("\n");

        // P1.6: Populate telegram_deadline_at for validation messages too.
        // Validation messages have a longer deadline (5 min) since they're
        // for completed rounds and are never stale in the prediction sense.
        const valDeadlineAt = new Date(Date.now() + 300_000).toISOString();
        await tx`
          insert into notification_outbox (
            notification_id, type, content, metadata, status, priority,
            attempt_count, next_attempt_at, telegram_deadline_at
          ) values (
            ${randomUUID()}::uuid, 'validation',
            ${validationContent},
            ${JSON.stringify({
              predictionId: state.pending!.prediction_id,
              gameId: evt.gameId,
              correlationId: state.pending!.correlation_id,
              targetMultiplier: target,
              actualMultiplier: evt.multiplier,
              probability: Number(state.pending!.probability),
              result,
              resolvedAt,
              slaViolated: false,
              kind: "validation",
            })},
            'pending', 2,
            0, now(), ${valDeadlineAt}::timestamptz
          )
        `;
      }

      await tx`
        insert into live_event_log (
          correlation_id, event_kind, game_id, payload, received_at, processed_at,
          processor_latency_ms, sla_violated
        ) values (
          ${state.pending!.correlation_id ?? randomUUID()}::text, 'ED', ${evt.gameId},
          ${JSON.stringify({ endTime: evt.endTime, multiplier: evt.multiplier, result })},
          ${evt.receivedAt}::timestamptz, now(),
          ${Math.max(0, now() - new Date(evt.receivedAt).getTime())},
          false
        )
        on conflict do nothing
      `;
    });
  } catch (e) {
    // Fix 9: structured error telemetry — name, message, stack, game,
    // correlation and event context. `String(e)` alone is not enough to
    // diagnose production failures.
    const err = e instanceof Error ? e : new Error(String(e));
    logger.error(
      {
        component: "live-validator",
        targetGameId: evt.gameId,
        errorName: err.name,
        errorMessage: err.message,
        errorStack: err.stack,
        endTime: evt.endTime,
        multiplier: evt.multiplier,
        skipPredict: evt.skipPredict === true,
      },
      "validator.onGameEnd failed",
    );
    try {
      // Fix 9: persist full error context in worker_state for post-mortem.
      const errJson = JSON.stringify({
        name: err.name,
        message: err.message,
        stack: err.stack,
        gameId: evt.gameId,
        endTime: evt.endTime,
        multiplier: evt.multiplier,
        at: new Date().toISOString(),
      });
      await sql`
        insert into worker_state (key, value)
        values ('last_error', ${errJson})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
      await sql`
        insert into worker_state (key, value)
        values ('last_error_name', ${err.name})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
      await sql`
        insert into worker_state (key, value)
        values ('last_error_message', ${err.message})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
      await sql`
        insert into worker_state (key, value)
        values ('last_error_stack', ${err.stack ?? ''})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
      await sql`
        insert into worker_state (key, value)
        values ('last_error_game_id', ${evt.gameId})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
      await sql`
        insert into worker_state (key, value)
        values ('last_error_at', ${new Date().toISOString()})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
    } catch {
      /* ignore */
    }
    return { kind: "bg_arrived_late", targetGameId: evt.gameId };
  }

  // Keep live history buffer current (zero-RTT prior rounds for N+1 predict).
  try {
    const { appendCompletedRound } = await import(
      "@/lib/prediction/live/live-history-buffer"
    );
    appendCompletedRound({
      gameId: evt.gameId,
      multiplier: evt.multiplier,
      crashedAt: evt.endTime,
    });
  } catch {
    /* soft */
  }

  // Incremental state MUST update on every crash (ED or poll).
  // Bug: poll path uses skipPredict=true; the old branch only updated when
  // pending!=null OR !skipPredict — so under WAF (poll-only) the model froze
  // at boot seed → identical probability/confidence every round.
  if (!evt.skipStateUpdate) {
    try {
      globalIncrementalState.update(evt.multiplier);
    } catch {
      /* soft */
    }
    try {
      const { getSharedACIEEngine } = await import(
        "@/lib/prediction/acie/shared-engine"
      );
      const eng = getSharedACIEEngine();
      eng.observeRound({ roundId: evt.gameId, crashPoint: evt.multiplier });
    } catch {
      /* soft — predictor path is authoritative observer */
    }
  }

  if (state.pending == null) {
    // N+1 already scheduled at entry when !skipPredict.
    if (state.crashRow && state.crashRow.began_at == null) {
      return { kind: "orphaned", targetGameId: evt.gameId };
    }
    return { kind: "bg_arrived_late", targetGameId: evt.gameId };
  }

  const target = Number(state.pending.target_multiplier);
  const result: "WIN" | "LOSS" = evt.multiplier >= target ? "WIN" : "LOSS";
  const resolvedAt = new Date(now()).toISOString();
  logger.info(
    {
      component: "live-validator",
      predictionId: state.pending.prediction_id,
      targetGameId: evt.gameId,
      actualMultiplier: evt.multiplier,
      result,
    },
    "round validated",
  );

  // Phase 11 — prediction resolved for this round
  try {
    await markPredictionResolved(evt.gameId);
  } catch {
    /* soft */
  }

  // Phase 18 — ed processing latency
  try {
    const lat = Math.max(0, now() - new Date(evt.receivedAt).getTime());
    edProcessingLatencyMs.observe(lat);
  } catch {
    /* soft */
  }

  // P0.3: Make feedback processing non-blocking via setImmediate.
  // Feedback is for learning (incremental state, calibration, model performance),
  // not correctness. The N+1 prediction can proceed without waiting for it.
  const pendingSnapshot = state.pending; // capture for async closure
  setImmediate(() => {
    if (!pendingSnapshot) return;
    // P0 (identity + temporal validity): resolve the EXACT registered
    // prediction for this target round and observe rolling metrics.
    // Temporally invalid predictions never feed learning or performance.
    void (async () => {
      try {
        const { globalPredictionRegistry, globalRollingPerformance } = await import(
          "@/lib/prediction/identity/prediction-registry"
        );
        const resolution = globalPredictionRegistry.resolve(
          evt.gameId,
          evt.multiplier,
          Number(pendingSnapshot.target_multiplier) || 1.3,
        );
        const rec =
          resolution?.record ?? globalPredictionRegistry.getByTarget(evt.gameId);
        if (rec?.temporalValidity !== "TEMPORALLY_INVALID") {
          globalRollingPerformance.observe(
            resolution?.probability ?? Number(pendingSnapshot.probability),
            result === "WIN",
            pendingSnapshot.confidence != null ? Number(pendingSnapshot.confidence) : null,
          );
        }
      } catch { /* soft — registry is best-effort */ }
    })();
    // P0 (Problem 6): hard temporal-validity gate — a prediction generated
    // after its target round already started must never train the models.
    void (async () => {
      try {
        const { globalPredictionRegistry } = await import(
          "@/lib/prediction/identity/prediction-registry"
        );
        const rec = globalPredictionRegistry.getByTarget(evt.gameId);
        if (rec?.temporalValidity === "TEMPORALLY_INVALID") {
          logger.warn(
            {
              component: "live-validator",
              predictionId: pendingSnapshot.prediction_id,
              targetGameId: evt.gameId,
              createdAt: rec.createdAt,
              targetStartedAt: rec.targetStartedAt,
            },
            "skipping closed-loop feedback — prediction TEMPORALLY_INVALID",
          );
          // Record the skip durably so the one_feedback_per_validation
          // invariant and the stuck-feedback sweep can distinguish
          // "intentionally never applied" from "genuinely stuck".
          try {
            const skipSql = await getSql();
            await skipSql`
              UPDATE prediction_validations
              SET feedback_skip_reason = 'TEMPORALLY_INVALID'
              WHERE prediction_id = ${pendingSnapshot.prediction_id}
                AND feedback_applied_at IS NULL
                AND feedback_skip_reason IS NULL
            `;
          } catch { /* soft — invariant may flag once, harmless */ }
          return;
        }
      } catch { /* soft */ }
      void processResolvedPredictionFeedback({
        predictionId: pendingSnapshot.prediction_id,
        targetGameId: evt.gameId,
        predictedProbability: Number(pendingSnapshot.probability),
        predictedConfidence:
          pendingSnapshot.confidence != null ? Number(pendingSnapshot.confidence) : null,
        targetMultiplier: Number(pendingSnapshot.target_multiplier),
        actualMultiplier: evt.multiplier,
        result,
        regimeAtPrediction: pendingSnapshot.regime_name ?? null,
        modelVersion: (pendingSnapshot as { model_version?: string | null }).model_version ?? null,
        correlationId: pendingSnapshot.correlation_id ?? null,
        resolvedAt,
      }).catch((fbErr) => {
        logger.warn(
          { component: "live-validator", error: String(fbErr) },
          "async closed-loop feedback failed",
        );
      });
    })();
  });

  // N+1 already scheduled at function entry (parallel with validation).
  // Keep a safety schedule only if entry was skipped (should not happen here).

  // Wake outbox dispatcher for the validation notification row.
  setImmediate(() => {
    void import("@/lib/prediction/live/outbox-wake")
      .then(({ notifyOutbox }) => notifyOutbox())
      .catch(() => undefined);
  });

  return {
    kind: "resolved",
    predictionId: state.pending.prediction_id,
    targetGameId: evt.gameId,
    result,
    targetMultiplier: target,
    actualMultiplier: evt.multiplier,
    resolvedAt: new Date(now()).toISOString(),
    alreadyValidated: false,
    outboxEnqueued: 1, // one row; sendTelegramMessage fans out to all chats
    correlationId: state.pending.correlation_id ?? "",
  };
}
