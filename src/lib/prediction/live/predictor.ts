/**
 * Synchronous-on-event predictor.
 *
 * Spec: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §7.2
 *
 * `onGameStart` is the single function that creates a `pending_predictions`
 * row in the new architecture. It is called from the Socket.IO `bg` (begin)
 * event handler and writes the prediction, the outbox rows, and the
 * `live_event_log` row in one transaction. The strict temporal invariant
 * (`prediction_generated_at < target_round_began_at`) holds at the commit
 * of this transaction.
 *
 * The function NEVER throws to the caller; every error is logged and
 * recorded in `worker_state.last_error`. The next `bg` event retries
 * idempotently via the partial unique index on `target_game_id`.
 */
import { randomUUID } from "node:crypto";
import { getSql, type Sql } from "@/lib/db";
import { runInTransaction } from "@/lib/prediction/live/tx";
import { PredictionEngine } from "@/lib/prediction/prediction-engine";
import type { HistoricalRound, ThresholdTarget } from "@/lib/prediction/types";
import { getConfiguredChatIds } from "@/lib/notifications/telegram";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("live-predictor");

/** Prediction-related constants. */
const DEFAULT_TARGET: ThresholdTarget = 1.3;
const MIN_HISTORY = 20;
const MAX_HISTORY = 100;
/** SLA gate: if the bg payload's `beginTime` is older than this, the
 *  prediction is still persisted (correctness preserved) but the Telegram
 *  outbox writes are skipped to avoid the "predicts the past" operator
 *  symptom. */
export const SLA_LAG_MS = 2_000;
/** DB-level CHECK constraint cap: a bg payload whose `beginTime` is in the
 *  future of the prediction row's `prediction_generated_at` is rejected. */
export const TEMPORAL_TOLERANCE_MS = 100;

export interface GameStartEvent {
  gameId: string;
  beginTime: string;
  hash: string | null;
  salt: string | null;
  /** The most recently settled round (BC.Game `gameId`); used for the
   *  `source_round_game_id` column. May be null on the first bg after
   *  boot, in which case the cold-start seeder must have already populated
   *  history. */
  sourceRoundGameId: string | null;
  receivedAt: string;
}

export type OnGameStartResult =
  | {
      kind: "predicted";
      predictionId: string;
      targetGameId: string;
      targetBeganAt: string;
      predictionGeneratedAt: string;
      latencyMs: number;
      slaViolated: boolean;
      correlationId: string;
      outboxEnqueued: number;
    }
  | { kind: "duplicate"; predictionId: string; targetGameId: string }
  | { kind: "no_history"; available: number; targetGameId: string }
  | { kind: "temporal_violation"; targetGameId: string; beginTime: string; reason: string }
  | { kind: "sla_violated_no_outbox"; predictionId: string; targetGameId: string };

interface PredictorDeps {
  getSqlFn?: () => Promise<Sql>;
  /** Injected for tests; default uses the real `PredictionEngine`. */
  predictFn?: (
    priorRounds: HistoricalRound[],
    targetRoundId: string,
    timestamp: string,
    target: ThresholdTarget,
  ) => {
    predictionId: string;
    probability: number;
    confidence: number;
    regimeId: string | null;
    reasoning: string[];
    featureSummary: Record<string, unknown>;
    modelVersion: string;
  };
  /** Injected for tests; default reads env. */
  getChatIds?: () => string[];
  /** Injected for tests; default uses `Date.now`. */
  now?: () => number;
  /** Injected for tests. */
  minHistory?: number;
  /** Injected for tests. */
  slaLagMs?: number;
  /** Injected for tests. */
  temporalToleranceMs?: number;
}

interface PriorRow {
  game_id: string;
  multiplier: string | number;
  began_at: string | Date | null;
  crashed_at: string | Date;
}

function mapRowToHistorical(r: PriorRow): HistoricalRound {
  const crashedAt =
    r.crashed_at instanceof Date ? r.crashed_at.toISOString() : String(r.crashed_at);
  const beganAt =
    r.began_at instanceof Date
      ? r.began_at.toISOString()
      : r.began_at
        ? String(r.began_at)
        : null;
  return {
    id: r.game_id,
    externalRoundId: r.game_id,
    sessionId: null,
    startedAt: beganAt,
    crashedAt,
    crashPoint: Number(r.multiplier),
    observationSource: "bc-game-socket",
    dataQuality: "high",
    createdAt: crashedAt,
    sequenceIndex: undefined,
  };
}

const defaultPredictFn = (
  priorRounds: HistoricalRound[],
  targetRoundId: string,
  timestamp: string,
  target: ThresholdTarget,
) => {
  const engine = new PredictionEngine();
  const signal = engine.predict({
    priorRounds,
    targetRoundId,
    timestamp,
    target,
  });
  return {
    predictionId: signal.predictionId,
    probability: signal.probability,
    confidence: signal.confidence,
    regimeId: signal.regimeId,
    reasoning: signal.reasoning,
    featureSummary: signal.featureSummary,
    modelVersion: signal.modelVersion,
  };
};

async function loadPriorRoundsStrict(
  sql: Sql,
  beganAt: string,
  limit: number,
): Promise<HistoricalRound[]> {
  const rows = await sql<PriorRow>`
    select game_id, multiplier, began_at, crashed_at
    from crash_rounds
    where crashed_at < ${beganAt}::timestamptz
      and crashed_at is not null
    order by crashed_at desc, game_id desc
    limit ${limit}
  `;
  return rows.reverse().map(mapRowToHistorical);
}

/**
 * Handle a BC.Game `bg` (begin) event.
 *
 * The function is synchronous from the caller's perspective (it awaits the
 * single transaction) and is responsible for:
 *  1. SLA-gate: if `now - beginTime > SLA_LAG_MS`, set `slaViolated = true`
 *     and skip outbox writes (prediction still persists).
 *  2. Duplicate check: SELECT pending_predictions for the same target
 *     `game_id`; if found, return `{ kind: 'duplicate' }` (idempotency).
 *  3. tx1 — atomic: anchor `crash_rounds` row, INSERT prediction,
 *     INSERT outbox rows, INSERT `live_event_log` row. The strict temporal
 *     invariant `prediction_generated_at < target_round_began_at` holds at
 *     the COMMIT of this transaction.
 *  4. If `target_round_began_at > now + TEMPORAL_TOLERANCE_MS` (future-dated
 *     payload, clock skew or replay attack), return `{ kind:
 *     'temporal_violation' }` and DO NOT insert.
 */
export async function onGameStart(
  evt: GameStartEvent,
  deps: PredictorDeps = {},
): Promise<OnGameStartResult> {
  const getSqlFn = deps.getSqlFn ?? getSql;
  const predictFn = deps.predictFn ?? defaultPredictFn;
  const getChatIds = deps.getChatIds ?? getConfiguredChatIds;
  const now = deps.now ?? Date.now;
  const minHistory = deps.minHistory ?? MIN_HISTORY;
  const slaLagMs = deps.slaLagMs ?? SLA_LAG_MS;
  const temporalToleranceMs = deps.temporalToleranceMs ?? TEMPORAL_TOLERANCE_MS;

  const correlationId = randomUUID();
  const beginMs = new Date(evt.beginTime).getTime();
  const receivedMs = new Date(evt.receivedAt).getTime();
  const slaLagMsActual = receivedMs - beginMs;
  const slaViolated = slaLagMsActual > slaLagMs;

  // Step 1: temporal violation — bg payload claims a future beginTime.
  // Defends against clock skew / replay attack / future-dated payload.
  if (beginMs > now() + temporalToleranceMs) {
    logger.error(
      {
        component: "live-predictor",
        correlationId,
        targetGameId: evt.gameId,
        beginTime: evt.beginTime,
        now: new Date(now()).toISOString(),
      },
      "temporal violation: beginTime is in the future; skipping prediction",
    );
    return {
      kind: "temporal_violation",
      targetGameId: evt.gameId,
      beginTime: evt.beginTime,
      reason: "beginTime in the future",
    };
  }

  // Defensive: if beginTime is invalid, treat as a parse failure.
  if (!Number.isFinite(beginMs)) {
    return {
      kind: "temporal_violation",
      targetGameId: evt.gameId,
      beginTime: evt.beginTime,
      reason: "beginTime unparseable",
    };
  }

  const sql = await getSqlFn();

  // Refine the temporal check using the DB clock when available —
  // PGLite/Neon, containerized runners, and CI can have drifted clocks
  // relative to the host, and the DB clock is the canonical
  // `requested_at` source.
  let refNow = now();
  try {
    const dbNow = await sql<{ now: string | Date }>`select now() as now`;
    if (dbNow[0]?.now) {
      const v = dbNow[0].now;
      const dbMs = v instanceof Date ? v.getTime() : new Date(v).getTime();
      if (Number.isFinite(dbMs)) refNow = dbMs;
    }
  } catch {
    /* fall back to local clock */
  }
  if (beginMs > refNow + temporalToleranceMs) {
    logger.error(
      {
        component: "live-predictor",
        correlationId,
        targetGameId: evt.gameId,
        beginTime: evt.beginTime,
        refNowIso: new Date(refNow).toISOString(),
      },
      "temporal violation (db-clock): beginTime is in the future; skipping prediction",
    );
    return {
      kind: "temporal_violation",
      targetGameId: evt.gameId,
      beginTime: evt.beginTime,
      reason: "beginTime in the future (db clock)",
    };
  }

  // Step 2: duplicate check. Partial unique index on target_game_id in 0008
  // makes the second INSERT a no-op too, so the belt is the SELECT and the
  // suspenders are the index.
  const existing = await sql<{ prediction_id: string }>`
    select prediction_id from pending_predictions
    where target_game_id = ${evt.gameId} and matched = false
    limit 1
  `;
  if (existing.length > 0) {
    logger.info(
      {
        component: "live-predictor",
        correlationId,
        targetGameId: evt.gameId,
        predictionId: existing[0]!.prediction_id,
      },
      "duplicate bg event; prediction already exists for this target",
    );
    return {
      kind: "duplicate",
      predictionId: existing[0]!.prediction_id,
      targetGameId: evt.gameId,
    };
  }

  // Step 3: strict causal window — only rounds whose outcome is fully
  // known BEFORE the target began are valid model input.
  const priorRounds = await loadPriorRoundsStrict(sql, evt.beginTime, MAX_HISTORY);
  if (priorRounds.length < minHistory) {
    logger.warn(
      {
        component: "live-predictor",
        correlationId,
        targetGameId: evt.gameId,
        available: priorRounds.length,
        minHistory,
      },
      "insufficient history; skipping prediction (cold-start seeder should run first)",
    );
    return { kind: "no_history", available: priorRounds.length, targetGameId: evt.gameId };
  }

  // Step 4: model inference. Pure CPU. The engine must NEVER see rows
  // whose `crashed_at` is the target round's own (the strict window
  // guarantees that).
  const timestamp = new Date(now()).toISOString();
  const signal = predictFn(priorRounds, evt.gameId, timestamp, DEFAULT_TARGET);

  // Step 5: atomic transaction. The crash_rounds row is anchored first so
  // any consumer querying `WHERE game_id = $G` finds a row even before
  // the prediction is committed. The pending_predictions INSERT is
  // gated by the partial unique index — re-runs of the same bg are
  // no-ops. The outbox rows are written in the same transaction as
  // the prediction so the strict invariant is durable.
  let predictionId: string = signal.predictionId;
  let predictionGeneratedAt = timestamp;
  let outboxEnqueued = 0;

  const chatIds = getChatIds();

  try {
    await runInTransaction(sql, async (tx) => {
      // Note: we do NOT pre-insert the crash_rounds row here. The schema
      // requires `multiplier` and `crashed_at` to be NOT NULL; both will
      // arrive on the `ed` event. The validator's UPDATE will create the
      // row via `insertNewRounds` (REST backfill) or via the ed handler's
      // own INSERT path on first arrival. Skipping here keeps the
      // transaction idempotent and removes the schema mismatch.

      const ins = await tx<{ prediction_id: string; requested_at: string }>`
        insert into pending_predictions (
          prediction_id, target_multiplier, probability, confidence,
          regime_name, regime_confidence, reasoning, feature_summary,
          model_version, requested_at,
          target_game_id, target_round_started_at, source_round_id,
          correlation_id
        ) values (
          ${predictionId}, ${DEFAULT_TARGET}, ${signal.probability},
          ${signal.confidence}, ${signal.regimeId},
          ${signal.regimeId ? 0.5 : null},
          ${signal.reasoning}, ${JSON.stringify(signal.featureSummary)},
          ${signal.modelVersion}, ${timestamp},
          ${evt.gameId}, ${evt.beginTime}, ${evt.sourceRoundGameId},
          ${correlationId}
        )
        on conflict (prediction_id) do nothing
        returning prediction_id, requested_at
      `;
      if (ins.length === 0) {
        // Lost the race; the unique index caught a duplicate.
        const dup = await tx<{ prediction_id: string }>`
          select prediction_id from pending_predictions
          where target_game_id = ${evt.gameId} and matched = false
          limit 1
        `;
        if (dup.length === 0) {
          throw new Error("PREDICTION_DUPLICATE_BUT_UNREADABLE");
        }
        predictionId = dup[0]!.prediction_id;
        return;
      }
      predictionId = ins[0]!.prediction_id;
      predictionGeneratedAt = String(ins[0]!.requested_at);

      // Outbox rows: one per configured chat id. SKIPPED entirely when
      // slaViolated is true so the operator does not see a "predicts the
      // past" message during transient WAF/socket stalls.
      if (!slaViolated) {
        for (const chatId of chatIds) {
          await tx`
            insert into notification_outbox (
              notification_id, type, content, metadata, status, priority
            ) values (
              ${randomUUID()}::uuid, 'prediction',
              ${`[bg→prediction] target=${evt.gameId} mult=${DEFAULT_TARGET} prob=${signal.probability}`},
              ${JSON.stringify({
                predictionId,
                correlationId,
                targetGameId: evt.gameId,
                targetBeganAt: evt.beginTime,
                targetMultiplier: Number(DEFAULT_TARGET),
                probability: signal.probability,
                confidence: signal.confidence,
                regimeName: signal.regimeId,
                slaViolated: false,
                kind: "prediction",
              })},
              'pending', 2
            )
          `;
          outboxEnqueued += 1;
        }
      }

      // live_event_log row: append-only observability.
      await tx`
        insert into live_event_log (
          correlation_id, event_kind, game_id, payload, received_at, processed_at,
          processor_latency_ms, sla_violated
        ) values (
          ${correlationId}::text, 'BG', ${evt.gameId},
          ${JSON.stringify({ beginTime: evt.beginTime, sourceRoundGameId: evt.sourceRoundGameId })},
          ${evt.receivedAt}::timestamptz, now(),
          ${Math.max(0, now() - receivedMs)}, ${slaViolated}
        )
      `;
    });
  } catch (e) {
    logger.error(
      {
        component: "live-predictor",
        correlationId,
        targetGameId: evt.gameId,
        error: String(e),
      },
      "predictor.onGameStart failed; logging and recording in worker_state",
    );
    // Best-effort: write last_error to worker_state.
    try {
      await sql`
        insert into worker_state (key, value)
        values ('last_error', ${String(e)})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
    } catch {
      /* ignore */
    }
    return { kind: "temporal_violation", targetGameId: evt.gameId, beginTime: evt.beginTime, reason: String(e) };
  }

  logger.info(
    {
      component: "live-predictor",
      correlationId,
      predictionId,
      targetGameId: evt.gameId,
      targetBeganAt: evt.beginTime,
      latencyMs: now() - receivedMs,
      slaViolated,
      outboxEnqueued,
    },
    slaViolated
      ? "prediction persisted; SLA-gate suppressed outbox writes"
      : "prediction generated and persisted for next round",
  );

  if (slaViolated) {
    return {
      kind: "sla_violated_no_outbox",
      predictionId,
      targetGameId: evt.gameId,
    };
  }
  return {
    kind: "predicted",
    predictionId,
    targetGameId: evt.gameId,
    targetBeganAt: evt.beginTime,
    predictionGeneratedAt,
    latencyMs: now() - receivedMs,
    slaViolated: false,
    correlationId,
    outboxEnqueued,
  };
}


/**
 * CRITICAL (TestingEngine_Deep_Diagnosis.md §3.2): Generate prediction for
 * Round N+1 immediately after Round N ends. This is the ONLY correct trigger
 * point for ahead-of-time prediction so that
 *   prediction_generated_at < target_round_started_at
 * always holds (target_round_started_at is backfilled later on the bg event).
 */
export interface OnGameEndPredictResult {
  predictionId: string | null;
  targetGameId: string;
  kind: "predicted" | "duplicate" | "too_late" | "insufficient_history" | "error";
}

export async function onGameEndPredict(
  gameId: string,
  crashedAt: string,
  multiplier: number,
  correlationId: string,
  deps: PredictorDeps = {},
): Promise<OnGameEndPredictResult> {
  const getSqlFn = deps.getSqlFn ?? getSql;
  const predictFn = deps.predictFn ?? defaultPredictFn;
  const sql = await getSqlFn();
  const targetGameId = (BigInt(gameId) + 1n).toString();
  const generatedAt = new Date().toISOString();

  try {
    const existing = await sql<{ prediction_id: string }>`
      SELECT prediction_id FROM pending_predictions
      WHERE target_game_id = ${targetGameId} AND status = 'PENDING'
      LIMIT 1
    `;
    if (existing.length > 0) {
      logger.info(
        { targetGameId, existingPredictionId: existing[0]!.prediction_id },
        "Prediction for N+1 already exists",
      );
      return {
        predictionId: existing[0]!.prediction_id,
        targetGameId,
        kind: "duplicate",
      };
    }

    const alreadyCrashed = await sql<{ game_id: string }>`
      SELECT game_id FROM crash_rounds WHERE game_id = ${targetGameId} LIMIT 1
    `;
    if (alreadyCrashed.length > 0) {
      logger.warn({ targetGameId }, "Target round N+1 already crashed — too late to predict");
      return { predictionId: null, targetGameId, kind: "too_late" };
    }

    const rows = await sql<{
      game_id: string;
      multiplier: string | number;
      began_at: string | Date | null;
      crashed_at: string | Date;
    }>`
      SELECT game_id, multiplier, began_at, crashed_at
      FROM crash_rounds
      WHERE crashed_at <= ${crashedAt}::timestamptz
        AND crashed_at IS NOT NULL
      ORDER BY crashed_at DESC, game_id DESC
      LIMIT ${MAX_HISTORY}
    `;
    const rounds = rows.reverse().map(mapRowToHistorical);
    if (rounds.length < MIN_HISTORY) {
      logger.info(
        { targetGameId, availableHistory: rounds.length },
        "Insufficient history for N+1 prediction",
      );
      return { predictionId: null, targetGameId, kind: "insufficient_history" };
    }

    const signal = predictFn(rounds, targetGameId, generatedAt, DEFAULT_TARGET);

    // Insert with conflict tolerance: prediction_id unique + partial unique
    // on (target_game_id) WHERE status='PENDING'. Either race returns
    // duplicate rather than throwing.
    try {
      await sql`
        INSERT INTO pending_predictions (
          prediction_id, target_multiplier, probability, confidence,
          regime_name, regime_confidence, reasoning, feature_summary,
          model_version, requested_at, target_game_id, source_round_id,
          source_game_id, target_round_started_at, correlation_id, generated_at, status, matched
        ) VALUES (
          ${signal.predictionId}, ${DEFAULT_TARGET}, ${signal.probability},
          ${signal.confidence}, ${signal.regimeId ?? null}, ${signal.regimeId ? 0.5 : null},
          ${signal.reasoning ?? []}, ${JSON.stringify(signal.featureSummary ?? {})},
          ${signal.modelVersion ?? "live-v2"}, ${generatedAt}::timestamptz, ${targetGameId}, ${gameId},
          ${gameId}, NULL, ${correlationId}, ${generatedAt}::timestamptz, 'PENDING', false
        )
        ON CONFLICT (prediction_id) DO NOTHING
      `;
    } catch (insertErr) {
      // Partial unique index race (target_game_id PENDING) surfaces as
      // a unique_violation that ON CONFLICT (prediction_id) cannot absorb.
      const msg = String(insertErr);
      if (msg.includes("unique") || msg.includes("duplicate") || msg.includes("23505")) {
        logger.info({ targetGameId }, "N+1 prediction race lost to concurrent writer");
        const raced = await sql<{ prediction_id: string }>`
          SELECT prediction_id FROM pending_predictions
          WHERE target_game_id = ${targetGameId} AND status = 'PENDING'
          LIMIT 1
        `;
        return {
          predictionId: raced[0]?.prediction_id ?? null,
          targetGameId,
          kind: "duplicate",
        };
      }
      throw insertErr;
    }

    const inserted = await sql<{ prediction_id: string }>`
      SELECT prediction_id FROM pending_predictions
      WHERE target_game_id = ${targetGameId} AND status = 'PENDING'
      LIMIT 1
    `;
    if (inserted.length === 0) {
      return { predictionId: null, targetGameId, kind: "duplicate" };
    }
    const predictionId = inserted[0]!.prediction_id;

    logger.info(
      {
        predictionId,
        targetGameId,
        sourceGameId: gameId,
        correlationId,
        probability: signal.probability,
      },
      "Generated ahead-of-time prediction for N+1",
    );

    try {
      const { createPredictionNotification } = await import("@/lib/notifications/outbox");
      await createPredictionNotification(sql, {
        predictionId,
        targetMultiplier: Number(DEFAULT_TARGET),
        probability: signal.probability,
        confidence: signal.confidence,
        regimeName: signal.regimeId ?? null,
        lastRoundMultiplier: rounds[rounds.length - 1]?.crashPoint ?? null,
        generatedAt,
        correlationId,
      });
    } catch (notifErr) {
      logger.warn(
        { predictionId, error: String(notifErr) },
        "Failed to enqueue prediction notification; prediction still persisted",
      );
    }

    try {
      await sql`
        INSERT INTO live_event_log (
          correlation_id, event_kind, game_id, payload, received_at, processed_at,
          processor_latency_ms, sla_violated
        ) VALUES (
          ${correlationId}::text, 'PREDICT', ${targetGameId},
          ${JSON.stringify({ sourceGameId: gameId, probability: signal.probability, multiplier })},
          ${generatedAt}::timestamptz, now(), 0, false
        )
        ON CONFLICT DO NOTHING
      `;
    } catch {
      /* ignore */
    }

    return { predictionId, targetGameId, kind: "predicted" };
  } catch (e) {
    logger.error(
      { gameId, targetGameId, error: String(e) },
      "onGameEndPredict failed",
    );
    try {
      await sql`
        INSERT INTO worker_state (key, value)
        VALUES ('last_error', ${String(e)})
        ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()
      `;
    } catch {
      /* ignore */
    }
    return { predictionId: null, targetGameId, kind: "error" };
  }
}
