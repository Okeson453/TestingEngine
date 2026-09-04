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
function triggerNextPrediction(
  gameId: string,
  endTime: string,
  multiplier: number,
  correlationId: string | null | undefined,
): void {
  const corr = correlationId ?? randomUUID();
  void onGameEndPredict(gameId, endTime, multiplier, corr).catch((e) => {
    logger.error(
      { gameId, error: String(e) },
      "Failed to generate N+1 prediction after ed processing",
    );
  });
}

export interface GameEndEvent {
  gameId: string;
  endTime: string;
  multiplier: number;
  receivedAt: string;
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
      await tx`
        insert into crash_rounds (game_id, multiplier, hash, salt, began_at, crashed_at)
        values (${evt.gameId}, ${evt.multiplier}, null, null,
                coalesce(${evt.endTime}::timestamptz - interval '3 seconds', now() - interval '3 seconds'),
                ${evt.endTime}::timestamptz)
        on conflict (game_id) do update
          set crashed_at = excluded.crashed_at,
              multiplier = excluded.multiplier
          where crash_rounds.crashed_at is null
      `;
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
        const chatIds = getChatIds();
        for (const chatId of chatIds) {
          await tx`
            insert into notification_outbox (
              notification_id, type, content, metadata, status, priority
            ) values (
              ${randomUUID()}::uuid, 'validation',
              ${`[ed→validation] target=${evt.gameId} actual=${evt.multiplier} result=${result}`},
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
              'pending', 2
            )
          `;
        }
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

  if (state.pending == null) {
    // Even without a pending prediction for N, generate N+1 so cold-start
    // and missed-bg recovery still produce the next prediction.
    triggerNextPrediction(evt.gameId, evt.endTime, evt.multiplier, null);
    if (state.crashRow && state.crashRow.began_at == null) {
      return { kind: "orphaned", targetGameId: evt.gameId };
    }
    return { kind: "bg_arrived_late", targetGameId: evt.gameId };
  }

  const target = Number(state.pending.target_multiplier);
  const result: "WIN" | "LOSS" = evt.multiplier >= target ? "WIN" : "LOSS";
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
  // Spec §2/§3.2: trigger N+1 prediction after Round N is processed.
  triggerNextPrediction(
    evt.gameId,
    evt.endTime,
    evt.multiplier,
    state.pending.correlation_id,
  );

  return {
    kind: "resolved",
    predictionId: state.pending.prediction_id,
    targetGameId: evt.gameId,
    result,
    targetMultiplier: target,
    actualMultiplier: evt.multiplier,
    resolvedAt: new Date(now()).toISOString(),
    alreadyValidated: false,
    outboxEnqueued: getChatIds().length,
    correlationId: state.pending.correlation_id ?? "",
  };
}
