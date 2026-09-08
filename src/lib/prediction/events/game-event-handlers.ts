/**
 * BC.Game Socket.IO → live prediction pipeline bridge.
 *
 * bg  → prediction generation + observability + target start persistence
 * ed  → immediate async validation only
 * pg  → observability only
 */
import { randomUUID } from "node:crypto";
import { bcGameSocket } from "@/lib/crash/socket-client";
import { getSql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";
import { onGameEnd } from "@/lib/prediction/live/validator";
import { onGameStart, type GameStartEvent } from "@/lib/prediction/live/predictor";
import { globalIncrementalState } from "@/lib/prediction/state/incremental-state-engine";
import {
  markLiveRoundStarted,
  markLiveRoundEnded,
} from "@/lib/prediction/live/live-round-state";
import {
  edToPredictMs,
  predictionHandoffMs,
  roundDetectMs,
} from "@/lib/observability/performance/latency";

const logger = getLogger("game-event-handlers");
const inFlightEd = new Set<string>();
const inFlightBg = new Set<string>();

function toIsoString(timestamp: number | string | undefined): string | null {
  if (!timestamp) return null;
  if (typeof timestamp === "string") {
    const date = new Date(timestamp);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }
  const date = new Date(timestamp < 1e10 ? timestamp * 1000 : timestamp);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function extractLastGameId(payload: unknown): string | null {
  if (typeof payload !== "object" || payload === null) return null;
  const p = payload as Record<string, unknown>;
  const id = p.gameId ?? p.id;
  if (typeof id === "string" && /^\d+$/.test(id)) return id;
  if (typeof id === "number" && Number.isFinite(id)) return String(id);
  return null;
}

/**
 * Find the most recent crashed round that can serve as source for predicting
 * the target round. Returns the source gameId and its crash data.
 */
async function findSourceRoundForTarget(
  targetGameId: string,
  sql: import("@/lib/db").Sql,
): Promise<{ sourceGameId: string; crashedAt: string; multiplier: number } | null> {
  try {
    // Look for the round immediately before the target
    const sourceGameId = String(BigInt(targetGameId) - 1n);
    
    const row = await sql<{
      game_id: string;
      multiplier: number;
      crashed_at: string;
    }>`
      SELECT game_id, multiplier, crashed_at::text
      FROM crash_rounds
      WHERE game_id = ${sourceGameId}
      ORDER BY crashed_at DESC
      LIMIT 1
    `;
    
    if (row.length > 0 && row[0]!.crashed_at) {
      return {
        sourceGameId: row[0]!.game_id,
        crashedAt: row[0]!.crashed_at,
        multiplier: row[0]!.multiplier,
      };
    }
    
    // Fallback: find the most recent crashed round
    const recent = await sql<{
      game_id: string;
      multiplier: number;
      crashed_at: string;
    }>`
      SELECT game_id, multiplier, crashed_at::text
      FROM crash_rounds
      WHERE crashed_at IS NOT NULL
      ORDER BY crashed_at DESC, game_id DESC
      LIMIT 1
    `;
    
    if (recent.length > 0 && recent[0]!.crashed_at) {
      return {
        sourceGameId: recent[0]!.game_id,
        crashedAt: recent[0]!.crashed_at,
        multiplier: recent[0]!.multiplier,
      };
    }
    
    return null;
  } catch (error) {
    logger.error(
      { targetGameId, error: String(error) },
      "findSourceRoundForTarget failed",
    );
    return null;
  }
}

async function onBgEvent(payload: unknown): Promise<void> {
  const gameId = extractLastGameId(payload);
  if (!gameId) return;
  const p = (payload ?? {}) as Record<string, unknown>;
  const beganAt = toIsoString((p.beganAt ?? p.beginTime) as number | string | undefined);
  if (!beganAt) return;

  if (inFlightBg.has(gameId)) return;
  inFlightBg.add(gameId);

  const receivedAt = new Date().toISOString();
  const correlationId = randomUUID();

  try {
    const sql = await getSql();
    
    // Check if this bg event has already been processed (persistent deduplication)
    const alreadyProcessed = await sql<{ count: number }>`
      SELECT count(*)::int AS count
      FROM live_event_log
      WHERE event_kind = 'BG' AND game_id = ${gameId} AND payload->>'beganAt' = ${beganAt}
    `;
    if ((alreadyProcessed[0]?.count ?? 0) > 0) {
      inFlightBg.delete(gameId);
      return;
    }
    
    // Backfill target_round_started_at for any existing predictions
    await sql`
      UPDATE pending_predictions
      SET target_round_started_at = ${beganAt}::timestamptz
      WHERE target_game_id = ${gameId}
        AND status = 'PENDING'
        AND target_round_started_at IS NULL
    `.catch(() => undefined);

    // Find source round for prediction
    const source = await findSourceRoundForTarget(gameId, sql);
    
    if (source) {
      // Generate prediction for this target round using the bg event data
      const evt: GameStartEvent = {
        gameId,
        beginTime: beganAt,
        hash: p.hash as string | null ?? null,
        salt: p.salt as string | null ?? null,
        sourceRoundGameId: source.sourceGameId,
        receivedAt,
      };
      
      // Call onGameStart to create the prediction
      // This ensures prediction_generated_at < target_round_started_at
      // because we're generating the prediction when we receive the bg event
      void onGameStart(evt, { recoveryMode: false })
        .then((result) => {
          logger.info(
            {
              event: "bg",
              gameId,
              kind: result.kind,
              correlationId,
              sourceGameId: source.sourceGameId,
            },
            "bg prediction complete",
          );
        })
        .catch((error) => {
          logger.error(
            { event: "bg", gameId, error: String(error), correlationId },
            "bg prediction failed",
          );
        });
    }

    await Promise.all([
      markLiveRoundStarted(gameId, beganAt, "socket", correlationId, sql).catch(() => undefined),
      sql`
        INSERT INTO live_event_log (
          correlation_id, event_kind, game_id, payload, received_at, processed_at,
          processor_latency_ms, sla_violated
        ) VALUES (
          ${correlationId}::text, 'BG', ${gameId}, ${JSON.stringify({ beganAt })},
          ${beganAt}::timestamptz, now(), 0, false
        ) ON CONFLICT DO NOTHING
      `.catch(() => undefined),
    ]);
  } catch (error) {
    logger.error({ event: "bg", gameId, error: String(error) }, "bg observability failed");
  } finally {
    inFlightBg.delete(gameId);
  }
}

/**
 * The Socket.IO callback must not wait for durable receipt writes. Those writes
 * are idempotent safety-net work; the predictor/validator transaction is the
 * correctness boundary. This removes DB RTT from the ED→prediction critical path.
 */
async function onEdEvent(payload: unknown): Promise<void> {
  const detectT0 = performance.now();
  const gameId = extractLastGameId(payload);
  if (!gameId) return;
  const p = (payload ?? {}) as Record<string, unknown>;
  const endIso =
    toIsoString((p.crashedAt ?? p.endTime) as number | string | undefined) ??
    new Date().toISOString();
  const raw = (p.multiplier ?? p.rate) as number | string | undefined;
  const multiplier =
    typeof raw === "number" ? raw : typeof raw === "string" ? Number.parseFloat(raw) : 0;
  if (!Number.isFinite(multiplier) || multiplier <= 0) return;
  roundDetectMs.observe(performance.now() - detectT0);

  if (inFlightEd.has(gameId)) return;
  inFlightEd.add(gameId);
  const receivedAt = new Date().toISOString();
  const correlationId = randomUUID();
  const handoffT0 = performance.now();

  // Check if this ed event has already been processed (persistent deduplication)
  try {
    const sql = await getSql();
    const alreadyProcessed = await sql<{ count: number }>`
      SELECT count(*)::int AS count
      FROM live_event_log
      WHERE event_kind = 'ED_RECEIVED' AND game_id = ${gameId}
    `;
    if ((alreadyProcessed[0]?.count ?? 0) > 0) {
      inFlightEd.delete(gameId);
      return;
    }
  } catch (error) {
    logger.debug({ event: "ed", gameId, error: String(error) }, "ed deduplication check failed");
  }

  // Update incremental state immediately so features see this crash.
  try {
    globalIncrementalState.update(multiplier);
  } catch {
    /* soft */
  }

  // Only validation - prediction is now handled in onBgEvent
  const validatePromise = onGameEnd({
    gameId,
    endTime: endIso,
    multiplier,
    receivedAt,
    skipPredict: true,
    skipStateUpdate: true,
  })
    .then((result) => {
      logger.info({ event: "ed", gameId, kind: result.kind, correlationId, path: "validate" }, "ed validation complete");
      return result;
    })
    .catch((error) => {
      logger.error(
        { event: "ed", gameId, error: String(error), correlationId, path: "validate" },
        "ed validation failed",
      );
    });

  // Do not block Socket.IO callback
  void Promise.resolve(validatePromise).finally(() => inFlightEd.delete(gameId));
  predictionHandoffMs.observe(performance.now() - handoffT0);

  // Durable receipt and lifecycle updates run independently and concurrently.
  void (async () => {
    try {
      const sql = await getSql();
      const crashedAt = new Date(endIso);
      const beganAt = new Date(crashedAt.getTime() - 3_000);
      await Promise.all([
        sql`
          INSERT INTO crash_rounds (game_id, multiplier, hash, salt, began_at, crashed_at)
          VALUES (${gameId}, ${multiplier}, null, null, ${beganAt}, ${crashedAt})
          ON CONFLICT (game_id) DO UPDATE
            SET crashed_at = COALESCE(crash_rounds.crashed_at, excluded.crashed_at),
                multiplier = COALESCE(crash_rounds.multiplier, excluded.multiplier)
        `.catch(() => undefined),
        sql`
          INSERT INTO live_event_log (
            correlation_id, event_kind, game_id, payload, received_at, processed_at,
            processor_latency_ms, sla_violated
          ) VALUES (
            ${correlationId}::text, 'ED_RECEIVED', ${gameId},
            ${JSON.stringify({ endTime: endIso, multiplier, source: "socket" })},
            ${receivedAt}::timestamptz, now(), 0, false
          ) ON CONFLICT DO NOTHING
        `.catch(() => undefined),
        markLiveRoundEnded(gameId, endIso, multiplier, undefined, "socket").catch(() => undefined),
      ]);
    } catch (error) {
      logger.debug({ event: "ed", gameId, error: String(error) }, "ed receipt persistence failed");
    }
  })();
}

function onPgEvent(payload: unknown): void {
  logger.debug({ event: "pg", gameId: extractLastGameId(payload) }, "pg received");
}

const bgHandler = (payload: unknown): void => {
  void onBgEvent(payload);
};
const edHandler = (payload: unknown): void => {
  void onEdEvent(payload);
};
const pgHandler = (payload: unknown): void => {
  onPgEvent(payload);
};

export function initializeEventHandlers(): void {
  const sock = bcGameSocket as unknown as {
    off?: (ev: string, fn: (...args: unknown[]) => void) => void;
    removeListener?: (ev: string, fn: (...args: unknown[]) => void) => void;
  };
  const rem = sock.off ?? sock.removeListener;
  if (typeof rem === "function") {
    rem.call(bcGameSocket, "bg", bgHandler as (...args: unknown[]) => void);
    rem.call(bcGameSocket, "ed", edHandler as (...args: unknown[]) => void);
    rem.call(bcGameSocket, "pg", pgHandler as (...args: unknown[]) => void);
  }
  bcGameSocket.on("bg", bgHandler);
  bcGameSocket.on("ed", edHandler);
  bcGameSocket.on("pg", pgHandler);
  logger.info({ component: "game-event-handlers" }, "event handlers wired");
}

export async function startEventDrivenPipeline(): Promise<void> {
  initializeEventHandlers();
  await bcGameSocket.connect();
}

export async function stopEventDrivenPipeline(): Promise<void> {
  const sock = bcGameSocket as unknown as {
    off?: (ev: string, fn: (...args: unknown[]) => void) => void;
    removeListener?: (ev: string, fn: (...args: unknown[]) => void) => void;
  };
  const rem = sock.off ?? sock.removeListener;
  if (typeof rem === "function") {
    rem.call(bcGameSocket, "bg", bgHandler as (...args: unknown[]) => void);
    rem.call(bcGameSocket, "ed", edHandler as (...args: unknown[]) => void);
    rem.call(bcGameSocket, "pg", pgHandler as (...args: unknown[]) => void);
  }
  inFlightEd.clear();
  inFlightBg.clear();
  bcGameSocket.disconnect();
}

export { bcGameSocket, onBgEvent as onGameStartLegacy, onEdEvent as onGameEndLegacy };
