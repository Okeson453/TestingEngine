/**
 * BC.Game Socket.IO → live prediction pipeline bridge.
 *
 * Wires the BC.Game Socket.IO events directly into the production-grade
 * live pipeline:
 *
 *   bg  →  onGameStart (live/predictor)         // predict the just-begun round
 *   ed  →  onGameEnd   (live/validator)         // validate the just-ended round
 *   pg  →  observability only                   // live crash progress
 *
 * The `handleGameStartEvent` / `handleGameEndEvent` / `handleGameProgressEvent`
 * helpers in the previous (pre-v2) implementation have been removed: they
 * were either dead code or duplicated the (correct) logic in live/predictor
 * and live/validator. The new file is a thin adapter.
 */
import { bcGameSocket } from "@/lib/crash/socket-client";
import { getLogger } from "@/lib/observability/logger";
import { onGameStart } from "@/lib/prediction/live/predictor";
import { onGameEnd } from "@/lib/prediction/live/validator";

const logger = getLogger("game-event-handlers");

function toIsoString(timestamp: number | string | undefined): string | null {
  if (!timestamp) return null;
  if (typeof timestamp === "string") {
    const date = new Date(timestamp);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }
  if (typeof timestamp === "number") {
    const date = new Date(timestamp < 1e10 ? timestamp * 1000 : timestamp);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }
  return null;
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
 * bg event: a new round has just begun. The live predictor runs the strict
 * causal window against history whose `crashed_at < $beganAt` and writes
 * a `pending_predictions` row, the `live_event_log` row, and (when
 * configured) the Telegram outbox rows in one transaction.
 */
async function onBgEvent(payload: unknown): Promise<void> {
  const gameId = extractLastGameId(payload);
  if (!gameId) {
    logger.warn({ event: "bg" }, "bg event missing numeric gameId; ignoring");
    return;
  }
  const p = (payload ?? {}) as Record<string, unknown>;
  const beginTime = toIsoString((p.beganAt ?? p.beginTime) as number | string | undefined);
  if (!beginTime) {
    logger.warn({ event: "bg", gameId }, "bg event missing beginTime; ignoring");
    return;
  }
  const receivedAt = new Date().toISOString();
  try {
    const result = await onGameStart({
      gameId,
      beginTime,
      hash: typeof p.hash === "string" ? p.hash : null,
      salt: typeof p.salt === "string" ? p.salt : null,
      sourceRoundGameId: null,
      receivedAt,
    });
    logger.info(
      { event: "bg", gameId, kind: result.kind },
      `bg processed (${result.kind})`,
    );
  } catch (e) {
    logger.error({ event: "bg", gameId, error: String(e) }, "bg handler threw");
  }
}

/**
 * ed event: the just-ended round has a known multiplier. The live validator
 * upserts `crash_rounds`, locks the pending prediction FOR UPDATE SKIP LOCKED,
 * writes a `prediction_validations` row, and enqueues a validation
 * notification. All in one transaction.
 */
async function onEdEvent(payload: unknown): Promise<void> {
  const gameId = extractLastGameId(payload);
  if (!gameId) {
    logger.warn({ event: "ed" }, "ed event missing numeric gameId; ignoring");
    return;
  }
  const p = (payload ?? {}) as Record<string, unknown>;
  const endTime = toIsoString((p.crashedAt ?? p.endTime) as number | string | undefined);
  const multiplierRaw = (p.multiplier ?? p.rate) as number | string | undefined;
  const multiplier =
    typeof multiplierRaw === "number"
      ? multiplierRaw
      : typeof multiplierRaw === "string"
        ? Number.parseFloat(multiplierRaw)
        : 0;
  if (!Number.isFinite(multiplier) || multiplier <= 0) {
    logger.warn(
      { event: "ed", gameId, multiplier: multiplierRaw },
      "ed event missing/invalid multiplier; ignoring",
    );
    return;
  }
  const receivedAt = new Date().toISOString();
  try {
    const result = await onGameEnd({
      gameId,
      endTime: endTime ?? receivedAt,
      multiplier,
      receivedAt,
    });
    logger.info(
      { event: "ed", gameId, kind: result.kind },
      `ed processed (${result.kind})`,
    );
  } catch (e) {
    logger.error({ event: "ed", gameId, error: String(e) }, "ed handler threw");
  }
}

/**
 * pg event: live crash progress. Observed for diagnostic / observability
 * purposes only — never triggers prediction or validation.
 */
function onPgEvent(payload: unknown): void {
  const gameId = extractLastGameId(payload);
  const p = (payload ?? {}) as Record<string, unknown>;
  const multiplier =
    typeof p.multiplier === "number"
      ? p.multiplier
      : typeof p.current === "number"
        ? p.current
        : null;
  logger.debug({ event: "pg", gameId, multiplier }, "pg received");
}

/**
 * Register all socket event handlers. Idempotent — re-calling is a no-op.
 */
export function initializeEventHandlers(): void {
  bcGameSocket.on("bg", async (payload) => {
    await onBgEvent(payload);
  });
  bcGameSocket.on("ed", async (payload) => {
    await onEdEvent(payload);
  });
  bcGameSocket.on("pg", async (payload) => {
    onPgEvent(payload);
  });
  logger.info({ component: "game-event-handlers" }, "event handlers wired to live/predictor + live/validator");
}

/**
 * Start the event-driven prediction pipeline. Connects the socket and
 * (re-)initializes the handlers.
 */
export async function startEventDrivenPipeline(): Promise<void> {
  initializeEventHandlers();
  await bcGameSocket.connect();
  logger.info({ component: "game-event-handlers" }, "event-driven pipeline started");
}

export async function stopEventDrivenPipeline(): Promise<void> {
  bcGameSocket.disconnect();
  logger.info({ component: "game-event-handlers" }, "event-driven pipeline stopped");
}

export { bcGameSocket, onBgEvent as onGameStartLegacy, onEdEvent as onGameEndLegacy };
