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
  const corr = randomUUID();
  try {
    // Phase 10 — Durable handoff BEFORE considering the event accepted.
    // Persist outcome + idempotency marker so a process crash after Socket.IO
    // receipt cannot permanently erase the ed event from the system.
    const sql = await getSql();
    const endIso = endTime ?? receivedAt;

    // 1. Durable crash outcome (idempotent UPSERT)
    try {
      const endDate = new Date(endIso);
      const crashedParam = Number.isNaN(endDate.getTime()) ? new Date() : endDate;
      const beganParam = new Date(crashedParam.getTime() - 3_000);
      await sql`
        INSERT INTO crash_rounds (game_id, multiplier, hash, salt, began_at, crashed_at)
        VALUES (
          ${gameId}, ${multiplier}, null, null,
          ${beganParam},
          ${crashedParam}
        )
        ON CONFLICT (game_id) DO UPDATE
          SET crashed_at = COALESCE(crash_rounds.crashed_at, excluded.crashed_at),
              multiplier = COALESCE(crash_rounds.multiplier, excluded.multiplier)
      `;
    } catch (persistErr) {
      logger.error(
        { event: "ed", gameId, error: String(persistErr) },
        "durable ed crash_rounds handoff failed",
      );
      // Still attempt lifecycle + processing; poll worker is the safety net
    }

    // 2. Durable receipt marker in live_event_log (idempotent)
    try {
      await sql`
        INSERT INTO live_event_log (
          correlation_id, event_kind, game_id, payload, received_at, processed_at,
          processor_latency_ms, sla_violated
        ) VALUES (
          ${corr}::text, 'ED_RECEIVED', ${gameId},
          ${JSON.stringify({ endTime: endIso, multiplier, source: "socket" })},
          ${receivedAt}::timestamptz, now(), 0, false
        )
        ON CONFLICT DO NOTHING
      `;
    } catch {
      /* soft — table may lack unique constraint on ED_RECEIVED */
    }

    // 3. Explicit live lifecycle end (Diagnosis §6)
    try {
      const { markLiveRoundEnded } = await import(
        "@/lib/prediction/live/live-round-state"
      );
      await markLiveRoundEnded(
        gameId,
        endIso,
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

    // 4. Async validate+predict — durable state already written above.
    // Ordering vs subsequent events preserved by Node event loop.
    setImmediate(() => {
      void (async () => {
        try {
          const result = await onGameEnd({
            gameId,
            endTime: endIso,
            multiplier,
            receivedAt,
            skipPredict: false, // Explicit: always trigger N+1 after ED validation
          });
          logger.info(
            { event: "ed", gameId, kind: result.kind, correlationId: corr },
            `ed processed (${result.kind})`,
          );
        } catch (e) {
          logger.error(
            { event: "ed", gameId, error: String(e), correlationId: corr },
            "ed handler threw (async)",
          );
        }
      })();
    });
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

const bgHandler = async (payload: unknown): Promise<void> => {
  await onBgEvent(payload);
};
const edHandler = async (payload: unknown): Promise<void> => {
  await onEdEvent(payload);
};
const pgHandler = (payload: unknown): void => {
  onPgEvent(payload);
};

let handlersWired = false;

export function initializeEventHandlers(): void {
  // Idempotent: remove prior listeners before attaching (P0 restart-safe)
  try {
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
  } catch { /* soft */ }

  bcGameSocket.on("bg", bgHandler);
  bcGameSocket.on("ed", edHandler);
  bcGameSocket.on("pg", pgHandler);
  handlersWired = true;
  logger.info(
    { component: "game-event-handlers" },
    "event handlers wired: bg=observability, ed=validate+predict-N+1",
  );
}

export async function startEventDrivenPipeline(): Promise<void> {
  initializeEventHandlers();
  await bcGameSocket.connect();
  // P0-3: probe path immediately so operators see Cloudflare/WAF vs other failures
  try {
    const { runSocketDiagnostics } = await import("@/lib/crash/socket-diagnostics");
    // fire-and-forget; do not block boot
    void runSocketDiagnostics().catch(() => undefined);
  } catch { /* optional */ }
  logger.info({ component: "game-event-handlers" }, "event-driven pipeline started");
}

export async function stopEventDrivenPipeline(): Promise<void> {
  try {
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
  } catch { /* soft */ }
  handlersWired = false;
  bcGameSocket.disconnect();
  logger.info({ component: "game-event-handlers" }, "event-driven pipeline stopped");
}

export { bcGameSocket, onBgEvent as onGameStartLegacy, onEdEvent as onGameEndLegacy };
