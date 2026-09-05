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
      // arrive on this ed event). Use UPSERT for idempotency.
      {
        const endDate = new Date(evt.endTime);
        const crashedParam = Number.isNaN(endDate.getTime()) ? new Date() : endDate;
        const beganParam = new Date(crashedParam.getTime() - 3_000);
        await tx`
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
        `;
      }
      const fetched = await tx<{ began_at: string | Date | null; crashed_at: string | Date | null }>`
        select began_at, crashed_at
        from crash_rounds
        where game_id = ${evt.gameId}
        limit 1
      `;
      state.crashRow = fetched[0] ?? null;

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
          ${state.pending!.requested_at instanceof Date
            ? state.pending!.requested_at.toISOString()
            : String(state.pending!.requested_at)},
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

        await tx`
          insert into notification_outbox (
            notification_id, type, content, metadata, status, priority,
            attempt_count, next_attempt_at
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
            0, now()
          )
        `;
      }

      await tx`
        insert into live_event_log (
          correlation_id, event_kind, game_id, payload, received_at, processed_at,
          processor_latency_ms, sla_violated
        ) values (
          ${(state.pending!.correlation_id ?? randomUUID())}::text, 'ED', ${evt.gameId},
          ${JSON.stringify({ endTime: evt.endTime, multiplier: evt.multiplier, result })},
          ${evt.receivedAt}::timestamptz, now(),
          ${Math.max(0, now() - new Date(evt.receivedAt).getTime())},
          false
        )
        on conflict do nothing
      `;
    });
  } catch (e) {
    logger.error(
      {
        component: "live-validator",
        targetGameId: evt.gameId,
        error: String(e),
      },
      "validator.onGameEnd failed",
    );
    try {
      await sql`
        insert into worker_state (key, value)
        values ('last_error', ${String(e)})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
    } catch {
      /* ignore */
    }
    return { kind: "bg_arrived_late", targetGameId: evt.gameId };
  }

  // P0.1: Connect Incremental State to Live Data
  // Update incremental state for EVERY crash, not just when pending==null
  if (state.pending != null) {
    try {
      const { globalIncrementalState } = await import(
        "@/lib/prediction/state/incremental-state-engine"
      );
      globalIncrementalState.update(evt.multiplier);
    } catch { /* soft */ }
  }

  if (state.pending == null) {
    // Even without a pending prediction for N, generate N+1 so cold-start
    // and missed-bg recovery still produce the next prediction — unless
    // the caller (poll worker) explicitly suppressed cascade.
    if (!evt.skipPredict) {
      // No pending row — still update incremental state for this crash, then predict
      try {
        const { globalIncrementalState } = await import(
          "@/lib/prediction/state/incremental-state-engine"
        );
        globalIncrementalState.update(evt.multiplier);
      } catch { /* soft */ }
      try {
        const eng = (globalThis as { __acieEngine__?: { observeRound: (r: { roundId: string; crashPoint: number }) => unknown } }).__acieEngine__;
        eng?.observeRound({ roundId: evt.gameId, crashPoint: evt.multiplier });
      } catch { /* soft */ }
      await triggerNextPrediction(evt.gameId, evt.endTime, evt.multiplier, null);
    }
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

  // Authoritative closed-loop feedback BEFORE N+1 (audit §27 / §40)
  try {
    const { processResolvedPredictionFeedback } = await import(
      "@/lib/prediction/live/feedback"
    );
    await processResolvedPredictionFeedback({
      predictionId: state.pending.prediction_id,
      targetGameId: evt.gameId,
      predictedProbability: Number(state.pending.probability),
      predictedConfidence: state.pending.confidence != null
        ? Number(state.pending.confidence)
        : null,
      targetMultiplier: Number(state.pending.target_multiplier),
      actualMultiplier: evt.multiplier,
      result,
      regimeAtPrediction: state.pending.regime_name ?? null,
      modelVersion: (state.pending as { model_version?: string | null }).model_version ?? null,
      correlationId: state.pending.correlation_id ?? null,
      resolvedAt,
    });
  } catch (fbErr) {
    logger.warn(
      { component: "live-validator", error: String(fbErr) },
      "closed-loop feedback failed — continuing to N+1 with partial learning",
    );
  }

  // Spec §2/§3.2: trigger N+1 prediction after Round N is processed.
  // Poll recovery path sets skipPredict to prevent historical cascade.
  if (!evt.skipPredict) {
    await triggerNextPrediction(
      evt.gameId,
      evt.endTime,
      evt.multiplier,
      state.pending.correlation_id,
    );
  }

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
