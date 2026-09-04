/**
 * BC.Game Socket.IO → live prediction pipeline bridge.
 *
 * Spec: TestingEngine_Deep_Diagnosis.md §3.4
 *
 *   bg  →  observability only + backfill target_round_started_at
 *          (NEVER generate predictions — the round has already started)
 *   ed  →  live/validator.onGameEnd (validate Round N + trigger N+1 prediction)
 *   pg  →  observability only
 */
import { randomUUID } from "node:crypto";
import { bcGameSocket } from "@/lib/crash/socket-client";
import { getSql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";
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
 * bg event is only for observability and backfilling target_round_started_at.
 * NEVER generate predictions here — the round has already started.
 */
async function onBgEvent(payload: unknown): Promise<void> {
  const gameId = extractLastGameId(payload);
  if (!gameId) {
    logger.warn({ event: "bg" }, "bg event missing numeric gameId; ignoring");
    return;
  }
  const p = (payload ?? {}) as Record<string, unknown>;
  const beganAt = toIsoString((p.beganAt ?? p.beginTime) as number | string | undefined);
  if (!beganAt) {
    logger.warn({ event: "bg", gameId }, "bg event missing beginTime; ignoring");
    return;
  }

  try {
    const sql = await getSql();
    const corr = randomUUID();

    // Explicit live lifecycle (Diagnosis §6)
    try {
      const { markLiveRoundStarted } = await import(
        "@/lib/prediction/live/live-round-state"
      );
      await markLiveRoundStarted(gameId, beganAt, "socket", corr, sql);
    } catch (le) {
      logger.debug(
        { event: "bg", gameId, error: String(le) },
        "live_round_state update skipped",
      );
    }

    await sql`
      UPDATE pending_predictions
      SET target_round_started_at = ${beganAt}::timestamptz
      WHERE target_game_id = ${gameId}
        AND status = 'PENDING'
        AND target_round_started_at IS NULL
    `;

    await sql`
      INSERT INTO live_event_log (
        correlation_id, event_kind, game_id, payload, received_at, processed_at,
        processor_latency_ms, sla_violated
      ) VALUES (
        ${corr}::text, 'BG', ${gameId},
        ${JSON.stringify({ beganAt })},
        ${beganAt}::timestamptz, now(), 0, false
      )
      ON CONFLICT DO NOTHING
    `;

    logger.info({ event: "bg", gameId, beganAt }, "bg observability + backfill complete");
  } catch (error) {
    logger.error({ event: "bg", gameId, error: String(error) }, "bg observability failed");
  }
}

/**
 * ed event: validate Round N + trigger prediction for N+1 (via validator → predictor).
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
    // Explicit live lifecycle end (Diagnosis §6)
    try {
      const { markLiveRoundEnded } = await import(
        "@/lib/prediction/live/live-round-state"
      );
      await markLiveRoundEnded(
        gameId,
        endTime ?? receivedAt,
        multiplier,
        undefined,
        "socket",
      );
    } catch (le) {
      logger.debug(
        { event: "ed", gameId, error: String(le) },
        "live_round_state end update skipped",
      );
    }

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
  logger.info(
    { component: "game-event-handlers" },
    "event handlers wired: bg=observability, ed=validate+predict-N+1",
  );
}

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
