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
  if (typeof id === "string" && /^\d+$/.test(id)) return id;
  if (typeof id === "number" && Number.isFinite(id)) return String(id);
  return null;
}

async function findSourceRoundForTarget(
  targetGameId: string,
  sql: import("@/lib/db").Sql,
): Promise<{ sourceGameId: string; crashedAt: string; multiplier: number } | null> {
  try {
    const sourceGameId = String(BigInt(targetGameId) - 1n);
    const row = await sql<{ game_id: string; multiplier: number; crashed_at: string }>`
      SELECT game_id, multiplier, crashed_at::text
      FROM crash_rounds
      WHERE game_id = ${sourceGameId}
      ORDER BY crashed_at DESC
      LIMIT 1
    `;
    if (row[0]) {
      return { sourceGameId: row[0].game_id, crashedAt: row[0].crashed_at, multiplier: Number(row[0].multiplier) };
    }
    const fallback = await sql<{ game_id: string; multiplier: number; crashed_at: string }>`
      SELECT game_id, multiplier, crashed_at::text
      FROM crash_rounds
      WHERE game_id < ${targetGameId}
      ORDER BY game_id DESC
      LIMIT 1
    `;
    if (fallback[0]) {
      return { sourceGameId: fallback[0].game_id, crashedAt: fallback[0].crashed_at, multiplier: Number(fallback[0].multiplier) };
    }
    return null;
  } catch {
    return null;
  }
}

async function bgHandler(payload: unknown): Promise<void> {
  const gameId = extractLastGameId(payload);
  if (!gameId) return;
  if (inFlightBg.has(gameId)) return;
  inFlightBg.add(gameId);

  const correlationId = randomUUID();
  const p = (payload ?? {}) as Record<string, unknown>;
  const beganAt =
    toIsoString((p.beganAt ?? p.beginTime) as number | string | undefined) ??
    new Date().toISOString();

  try {
    const sql = await getSql();
    const source = await findSourceRoundForTarget(gameId, sql);

    if (source) {
      const evt: GameStartEvent = {
        gameId,
        beginTime: beganAt,
        receivedAt: new Date().toISOString(),
        sourceRoundGameId: source.sourceGameId,
      };
      void onGameStart(evt)
        .then((result) => {
          const kind = result && typeof result === "object" && "kind" in result ? (result as { kind: string }).kind : "ok";
          if (kind === "temporal_violation" || kind === "insufficient_history") {
            logger.warn(
              { event: "bg", gameId, correlationId, sourceGameId: source.sourceGameId, resultKind: kind, result },
              `bg prediction skipped (${kind})`,
            );
          } else {
            logger.info(
              { event: "bg", gameId, correlationId, sourceGameId: source.sourceGameId, resultKind: kind },
              "bg prediction complete",
            );
          }
        })
        .catch((error) => {
          logger.error(
            { event: "bg", gameId, error: String(error), correlationId, stack: error instanceof Error ? error.stack?.slice(0, 500) : undefined },
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

async function edHandler(payload: unknown): Promise<void> {
  const gameId = extractLastGameId(payload);
  if (!gameId) return;
  if (inFlightEd.has(gameId)) return;
  inFlightEd.add(gameId);

  const correlationId = randomUUID();
  const p = (payload ?? {}) as Record<string, unknown>;
  const multiplier =
    typeof p.multiplier === "number"
      ? p.multiplier
      : typeof p.maxRate === "number"
        ? p.maxRate / 100
        : null;
  const crashedAt =
    toIsoString((p.crashedAt ?? p.endTime) as number | string | undefined) ??
    new Date().toISOString();

  try {
    const sql = await getSql();

    if (multiplier != null && Number.isFinite(multiplier)) {
      try {
        globalIncrementalState.observeRound({ gameId, multiplier, crashedAt });
      } catch {
        /* soft */
      }

      const crashedAtDate = new Date(crashedAt);
      if (!Number.isNaN(crashedAtDate.getTime())) {
        const beganAt = new Date(crashedAtDate.getTime() - 3_000);
        await sql`
          INSERT INTO crash_rounds (game_id, multiplier, hash, seed, began_at, crashed_at)
          VALUES (${gameId}, ${multiplier}, null, null, ${beganAt}, ${crashedAtDate})
          ON CONFLICT (game_id) DO UPDATE SET
            multiplier = EXCLUDED.multiplier,
            crashed_at = EXCLUDED.crashed_at
        `.catch(() => undefined);
      }

      void onGameEnd({
        gameId,
        endTime: crashedAt,
        multiplier,
        receivedAt: new Date().toISOString(),
      }).catch((error) => {
        logger.error({ event: "ed", gameId, error: String(error), correlationId }, "ed validation failed");
      });
    }

    // Signature: (gameId, crashedAt, multiplier, sql?, source?)
    await markLiveRoundEnded(gameId, crashedAt, multiplier ?? 0, sql, "socket").catch(() => undefined);
  } catch (error) {
    logger.error({ event: "ed", gameId, error: String(error) }, "ed observability failed");
  } finally {
    inFlightEd.delete(gameId);
  }
}

export function initializeEventHandlers(): void {
  bcGameSocket.on("bg", bgHandler);
  bcGameSocket.on("ed", edHandler);
  logger.info({ component: "game-event-handlers" }, "event handlers wired");
}

/** Called by worker boot — wires handlers and opens the BC.Game crash socket. */
export async function startEventDrivenPipeline(): Promise<void> {
  initializeEventHandlers();
  await bcGameSocket.connect();
}

export async function stopEventDrivenPipeline(): Promise<void> {
  inFlightEd.clear();
  inFlightBg.clear();
  bcGameSocket.disconnect();
}

// Back-compat alias
export function wireGameEventHandlers(): void {
  initializeEventHandlers();
}
