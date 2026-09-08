/**
 * BC.Game Socket.IO → live prediction pipeline bridge.
 *
 * bg  → observability + target start persistence (never creates predictions)
 * ed  → validation + N+1 prediction via onGameEnd (skipPredict: false)
 * pg  → observability only
 */
import { randomUUID } from "node:crypto";
import { bcGameSocket } from "@/lib/crash/socket-client";
import { getSql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";
import { onGameEnd } from "@/lib/prediction/live/validator";
import { globalIncrementalState } from "@/lib/prediction/state/incremental-state-engine";
import {
  markLiveRoundStarted,
  markLiveRoundEnded,
} from "@/lib/prediction/live/live-round-state";
import {
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

async function onBgEvent(payload: unknown): Promise<void> {
  const gameId = extractLastGameId(payload);
  if (!gameId) return;
  const p = (payload ?? {}) as Record<string, unknown>;
  const beganAt = toIsoString((p.beganAt ?? p.beginTime) as number | string | undefined);
  if (!beganAt) return;

  if (inFlightBg.has(gameId)) return;
  inFlightBg.add(gameId);

  const correlationId = randomUUID();

  try {
    const sql = await getSql();

    const alreadyProcessed = await sql<{ count: number }>`
      SELECT count(*)::int AS count
      FROM live_event_log
      WHERE event_kind = 'BG' AND game_id = ${gameId} AND payload->>'beganAt' = ${beganAt}
    `;
    if ((alreadyProcessed[0]?.count ?? 0) > 0) {
      return;
    }

    await sql`
      UPDATE pending_predictions
      SET target_round_started_at = ${beganAt}::timestamptz
      WHERE target_game_id = ${gameId}
        AND status = 'PENDING'
        AND target_round_started_at IS NULL
    `.catch(() => undefined);

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
 * correctness boundary.
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

  try {
    globalIncrementalState.update(multiplier);
  } catch {
    /* soft */
  }

  // Canonical path: validate N and schedule onGameEndPredict for N+1.
  const validatePromise = onGameEnd({
    gameId,
    endTime: endIso,
    multiplier,
    receivedAt,
    skipPredict: false,
    skipStateUpdate: true,
  })
    .then((result) => {
      logger.info({ event: "ed", gameId, kind: result.kind, correlationId, path: "validate+predict" }, "ed complete");
      return result;
    })
    .catch((error) => {
      logger.error(
        { event: "ed", gameId, error: String(error), correlationId, path: "validate+predict" },
        "ed validation failed",
      );
    });

  void Promise.resolve(validatePromise).finally(() => inFlightEd.delete(gameId));
  predictionHandoffMs.observe(performance.now() - handoffT0);

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
