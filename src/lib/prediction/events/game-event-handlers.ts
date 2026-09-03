/**
 * BC.Game Event Handlers for Prediction Pipeline
 * Implements the event-driven prediction generation and validation
 * 
 * Specification: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §4, §7, §8
 */

import { getSql } from "@/lib/db";
import type { Sql } from "@/lib/db";
import { bcGameSocket } from "@/lib/crash/socket-client";
import { PredictionEngine } from "@/lib/prediction/prediction-engine";
import type {
  PredictionSignal,
  HistoricalRound,
  ThresholdTarget,
} from "@/lib/prediction/types";
import type { CrashRound } from "@/lib/crash/types";
import { getLogger } from "@/lib/observability/logger";
import { randomUUID } from "node:crypto";

const logger = getLogger("game-event-handlers");

// Configuration
const DEFAULT_TARGET: ThresholdTarget = 1.3;
const MIN_HISTORY = 20;
const MAX_HISTORY = 100;
const SLA_GATE_MS = 100; // Must generate prediction within 100ms of bg event
const MAX_PREDICTION_LATENCY_MS = 5000; // Maximum allowed prediction latency

// Track the last bg event to prevent duplicate processing
let lastProcessedBgEvent: { gameId: string; timestamp: number } | null = null;

/**
 * Convert BC.Game event timestamp to ISO string
 */
function toIsoString(timestamp: number | string | undefined): string | null {
  if (!timestamp) return null;
  
  if (typeof timestamp === "string") {
    const date = new Date(timestamp);
    if (!isNaN(date.getTime())) return date.toISOString();
    return null;
  }
  
  if (typeof timestamp === "number") {
    // Handle both milliseconds and seconds
    const date = new Date(timestamp < 1e10 ? timestamp * 1000 : timestamp);
    if (!isNaN(date.getTime())) return date.toISOString();
    return null;
  }
  
  return null;
}

/**
 * Map crash round to historical round format
 */
function mapCrashRoundToHistorical(r: CrashRound): HistoricalRound {
  return {
    id: r.gameId,
    externalRoundId: r.gameId,
    sessionId: null,
    startedAt: r.beganAt,
    crashedAt: r.crashedAt,
    crashPoint: r.multiplier,
    observationSource: "bc-game-socket",
    dataQuality: "high",
    createdAt: r.crashedAt,
  };
}

/**
 * Load recent rounds for prediction from database
 * Only includes rounds whose outcome is fully known (crashed_at <= now)
 */
async function loadRecentRoundsForPrediction(
  sql: Sql,
  limit = MAX_HISTORY,
): Promise<HistoricalRound[]> {
  const rows = await sql<{
    game_id: string;
    multiplier: string | number;
    began_at: string | Date | null;
    crashed_at: string | Date;
  }>`
    select game_id, multiplier, began_at, crashed_at
    from crash_rounds
    where crashed_at <= now()
    order by crashed_at desc, game_id desc
    limit ${limit}
  `;
  
  return rows.reverse().map((row) =>
    mapCrashRoundToHistorical({
      gameId: row.game_id,
      multiplier: Number(row.multiplier),
      hash: null,
      salt: null,
      beganAt:
        row.began_at instanceof Date
          ? row.began_at.toISOString()
          : row.began_at,
      crashedAt:
        row.crashed_at instanceof Date
          ? row.crashed_at.toISOString()
          : row.crashed_at,
    }),
  );
}

/**
 * Check if we can generate a prediction (SLA gate)
 * Returns true if we're within the SLA window
 */
function canGeneratePrediction(bgEventTimestamp: number | string | undefined): boolean {
  if (!bgEventTimestamp) return false;
  
  const bgTime = typeof bgEventTimestamp === "string" 
    ? new Date(bgEventTimestamp).getTime() 
    : typeof bgEventTimestamp === "number" 
      ? bgEventTimestamp < 1e10 ? bgEventTimestamp * 1000 : bgEventTimestamp
      : 0;
  
  if (bgTime === 0) return false;
  
  const now = Date.now();
  const latency = now - bgTime;
  
  return latency <= MAX_PREDICTION_LATENCY_MS;
}

/**
 * Generate a prediction for a specific target round
 * This is the core function that replaces the old "next" target approach
 */
async function generatePredictionForTarget(
  targetGameId: string,
  targetRoundStartedAt: string | null,
  rounds: HistoricalRound[],
): Promise<PredictionSignal | null> {
  if (rounds.length < MIN_HISTORY) {
    logger.info(
      { component: "GameEventHandlers", targetGameId, availableHistory: rounds.length },
      "Insufficient history for prediction"
    );
    return null;
  }

  const engine = new PredictionEngine();
  const timestamp = new Date().toISOString();
  
  const signal = engine.predict({
    priorRounds: rounds,
    // Use the actual target gameId from the bg event
    targetRoundId: targetGameId,
    timestamp,
    target: DEFAULT_TARGET,
  });

  return signal;
}

/**
 * Store prediction in database with target round anchoring
 * This establishes the temporal invariant: prediction_generated_at < target_round_started_at
 */
async function storePredictionWithTarget(
  sql: Sql,
  signal: PredictionSignal,
  targetGameId: string,
  targetRoundStartedAt: string | null,
): Promise<string> {
  const predictionId = signal.predictionId || randomUUID();
  const timestamp = new Date().toISOString();
  
  // Ensure we have the target round started timestamp
  if (!targetRoundStartedAt) {
    throw new Error(`Missing target_round_started_at for prediction ${predictionId}`);
  }
  
  // Verify temporal invariant before insertion
  const predictionTime = new Date(timestamp).getTime();
  const targetStartTime = new Date(targetRoundStartedAt).getTime();
  
  if (predictionTime >= targetStartTime) {
    logger.error(
      { 
        component: "GameEventHandlers", 
        predictionId,
        predictionTime,
        targetStartTime,
        targetGameId 
      },
      "TEMPORAL INVARIANT VIOLATION: prediction_generated_at >= target_round_started_at"
    );
    throw new Error("Temporal invariant violation: prediction generated after round start");
  }

  await sql`
    insert into pending_predictions (
      prediction_id, target_multiplier, probability, confidence,
      regime_name, regime_confidence, reasoning, feature_summary,
      model_version, requested_at, target_game_id, target_round_started_at
    ) values (
      ${predictionId}, ${DEFAULT_TARGET}, ${signal.probability},
      ${signal.confidence}, ${signal.regimeId}, ${signal.regimeId ? 0.5 : null},
      ${signal.reasoning}, ${JSON.stringify(signal.featureSummary)},
      ${signal.modelVersion}, ${timestamp}, ${targetGameId}, ${targetRoundStartedAt}
    )
    on conflict (prediction_id) do nothing
  `;

  logger.info(
    { 
      component: "GameEventHandlers", 
      predictionId,
      targetGameId,
      targetRoundStartedAt,
      probability: signal.probability 
    },
    "Stored prediction with target round anchoring"
  );

  return predictionId;
}

/**
 * Check if a prediction already exists for this target game
 * This prevents duplicate predictions for the same round
 */
async function predictionExistsForTarget(sql: Sql, targetGameId: string): Promise<boolean> {
  const rows = await sql<{ count: number }>`
    select count(*)::int as count
    from pending_predictions
    where target_game_id = ${targetGameId}
  `;
  return (rows[0]?.count ?? 0) > 0;
}

/**
 * Handle bg (begin) event - PRIMARY PREDICTION TRIGGER
 * This is where genuine ahead-of-time prediction happens
 */
export async function handleGameStartEvent(
  payload: { gameId: string; beganAt?: number | string },
  event: string
): Promise<void> {
  // Prevent duplicate processing of the same bg event
  const eventId = `${payload.gameId}:${payload.beganAt}`;
  const eventTimestamp = payload.beganAt ? 
    typeof payload.beganAt === "string" 
      ? new Date(payload.beganAt).getTime() 
      : payload.beganAt < 1e10 ? payload.beganAt * 1000 : payload.beganAt
    : Date.now();
  
  if (lastProcessedBgEvent && 
      lastProcessedBgEvent.gameId === payload.gameId &&
      Math.abs(lastProcessedBgEvent.timestamp - eventTimestamp) < 1000) {
    logger.debug(
      { component: "GameEventHandlers", gameId: payload.gameId },
      "Duplicate bg event detected, skipping"
    );
    return;
  }

  lastProcessedBgEvent = {
    gameId: payload.gameId,
    timestamp: eventTimestamp,
  };

  // SLA gate check
  if (!canGeneratePrediction(payload.beganAt)) {
    logger.warn(
      { component: "GameEventHandlers", gameId: payload.gameId, beganAt: payload.beganAt },
      "SLA gate: bg event too old for prediction generation"
    );
    return;
  }

  try {
    const sql = await getSql();
    
    // Check if prediction already exists for this target
    const exists = await predictionExistsForTarget(sql, payload.gameId);
    if (exists) {
      logger.info(
        { component: "GameEventHandlers", gameId: payload.gameId },
        "Prediction already exists for this target round"
      );
      return;
    }

    // Load recent history for prediction
    const rounds = await loadRecentRoundsForPrediction(sql);
    
    if (rounds.length < MIN_HISTORY) {
      logger.info(
        { component: "GameEventHandlers", gameId: payload.gameId, available: rounds.length },
        "Insufficient history for prediction"
      );
      return;
    }

    // Generate prediction for this specific target round
    const targetRoundStartedAt = toIsoString(payload.beganAt);
    const signal = await generatePredictionForTarget(
      payload.gameId,
      targetRoundStartedAt,
      rounds
    );

    if (!signal) {
      logger.warn(
        { component: "GameEventHandlers", gameId: payload.gameId },
        "Failed to generate prediction signal"
      );
      return;
    }

    // Store prediction with target round anchoring
    const predictionId = await storePredictionWithTarget(
      sql,
      signal,
      payload.gameId,
      targetRoundStartedAt
    );

    logger.info(
      { 
        component: "GameEventHandlers", 
        predictionId,
        targetGameId: payload.gameId,
        targetRoundStartedAt 
      },
      "Generated and stored ahead-of-time prediction for target round"
    );

  } catch (error) {
    logger.error(
      { 
        component: "GameEventHandlers", 
        gameId: payload.gameId,
        error: error as Error 
      },
      "Failed to handle bg event"
    );
    throw error;
  }
}

/**
 * Handle ed (end) event - for validation and cleanup
 */
export async function handleGameEndEvent(
  payload: { gameId: string; crashedAt?: number | string; multiplier?: number },
  event: string
): Promise<void> {
  try {
    const sql = await getSql();
    
    // Check if there's a pending prediction for this game
    const pendingRows = await sql<{
      prediction_id: string;
      target_game_id: string;
      target_round_started_at: string | null;
    }>`
      select prediction_id, target_game_id, target_round_started_at
      from pending_predictions
      where target_game_id = ${payload.gameId} and matched = false
      limit 1
    `;

    if (pendingRows.length > 0) {
      const pending = pendingRows[0];
      
      // Verify temporal invariant: target_round_started_at < crashed_at
      if (pending.target_round_started_at) {
        const startedAt = new Date(pending.target_round_started_at).getTime();
        const crashedAt = payload.crashedAt ? 
          typeof payload.crashedAt === "string" 
            ? new Date(payload.crashedAt).getTime() 
            : payload.crashedAt < 1e10 ? payload.crashedAt * 1000 : payload.crashedAt
          : Date.now();
        
        if (startedAt >= crashedAt) {
          logger.error(
            { 
              component: "GameEventHandlers",
              predictionId: pending.prediction_id,
              targetGameId: payload.gameId,
              startedAt,
              crashedAt 
            },
            "TEMPORAL INVARIANT VIOLATION: target_round_started_at >= crashed_at"
          );
        }
      }

      logger.info(
        { 
          component: "GameEventHandlers", 
          predictionId: pending.prediction_id,
          gameId: payload.gameId,
          multiplier: payload.multiplier 
        },
        "Received ed event for round with pending prediction"
      );
    }

    // Note: The actual validation is handled by the existing validateAgainstNewRounds
    // function which is called by the worker when it ingests the REST history
    // The Socket.IO ed events are primarily for observability and potential optimization

  } catch (error) {
    logger.error(
      { 
        component: "GameEventHandlers", 
        gameId: payload.gameId,
        error: error as Error 
      },
      "Failed to handle ed event"
    );
    throw error;
  }
}

/**
 * Handle pg (progress) event - for monitoring
 * These events provide real-time progress of the current round
 */
export async function handleGameProgressEvent(
  payload: { gameId: string; multiplier?: number },
  event: string
): Promise<void> {
  // pg events are primarily for monitoring and don't trigger predictions
  // They can be used for debugging and observability
  logger.debug(
    { component: "GameEventHandlers", gameId: payload.gameId, multiplier: payload.multiplier },
    "Received game progress event"
  );
}

/**
 * Initialize event handlers with the Socket.IO client
 * This sets up the event-driven prediction pipeline
 */
export function initializeEventHandlers(): void {
  // Register bg (begin) event handler - PRIMARY PREDICTION TRIGGER
  bcGameSocket.on("bg", async (payload, event) => {
    try {
      await handleGameStartEvent(payload as { gameId: string; beganAt?: number | string }, event);
    } catch (error) {
      logger.error(
        { component: "EventHandlers", event, error: error as Error },
        "Error in bg event handler"
      );
    }
  });

  // Register ed (end) event handler - for validation
  bcGameSocket.on("ed", async (payload, event) => {
    try {
      await handleGameEndEvent(payload as { gameId: string; crashedAt?: number | string; multiplier?: number }, event);
    } catch (error) {
      logger.error(
        { component: "EventHandlers", event, error: error as Error },
        "Error in ed event handler"
      );
    }
  });

  // Register pg (progress) event handler - for monitoring
  bcGameSocket.on("pg", async (payload, event) => {
    try {
      await handleGameProgressEvent(payload as { gameId: string; multiplier?: number }, event);
    } catch (error) {
      logger.error(
        { component: "EventHandlers", event, error: error as Error },
        "Error in pg event handler"
      );
    }
  });

  logger.info({ component: "EventHandlers" }, "Initialized BC.Game event handlers");
}

/**
 * Start the event-driven prediction pipeline
 * This connects the Socket.IO client and initializes handlers
 */
export async function startEventDrivenPipeline(): Promise<void> {
  // Initialize event handlers
  initializeEventHandlers();
  
  // Connect to BC.Game Socket.IO
  await bcGameSocket.connect();
  
  logger.info({ component: "EventHandlers" }, "Event-driven prediction pipeline started");
}

/**
 * Stop the event-driven prediction pipeline
 */
export async function stopEventDrivenPipeline(): Promise<void> {
  bcGameSocket.disconnect();
  lastProcessedBgEvent = null;
  
  logger.info({ component: "EventHandlers" }, "Event-driven prediction pipeline stopped");
}

export {
  bcGameSocket,
  handleGameStartEvent as onGameStart,
  handleGameEndEvent as onGameEnd,
  handleGameProgressEvent as onGameProgress,
};