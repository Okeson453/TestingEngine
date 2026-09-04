/**
 * Explicit live round lifecycle — separate from historical crash_rounds.
 *
 * Spec: TestingEngine_Comprehensive_Diagnosis_and_Solution.md §6
 *
 *   DISCOVERED → STARTED → RUNNING → ENDED → RECONCILED
 *
 * Predictor uses this to answer "has target N+1 actually started?"
 * rather than treating crash_rounds existence as live timing evidence.
 */
import { getSql, type Sql } from "@/lib/db";
import type { FetchedRound } from "@/lib/crash/fetch-bc";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("live-round-state");

export type LiveLifecycle =
  | "DISCOVERED"
  | "STARTED"
  | "RUNNING"
  | "ENDED"
  | "RECONCILED";

export type LiveSource = "socket" | "poll" | "history" | "unknown";

export interface LiveRoundRow {
  gameId: string;
  lifecycle: LiveLifecycle;
  beganAt: string | null;
  crashedAt: string | null;
  multiplier: number | null;
  source: LiveSource;
  correlationId: string | null;
}

const ORDER: Record<LiveLifecycle, number> = {
  DISCOVERED: 0,
  STARTED: 1,
  RUNNING: 2,
  ENDED: 3,
  RECONCILED: 4,
};

function canAdvance(from: LiveLifecycle, to: LiveLifecycle): boolean {
  return ORDER[to] >= ORDER[from];
}

export async function getLiveRound(
  gameId: string,
  sql?: Sql,
): Promise<LiveRoundRow | null> {
  const db = sql ?? (await getSql());
  const rows = await db<{
    game_id: string;
    lifecycle: string;
    began_at: string | Date | null;
    crashed_at: string | Date | null;
    multiplier: number | null;
    source: string;
    correlation_id: string | null;
  }>`
    SELECT game_id, lifecycle, began_at, crashed_at, multiplier, source, correlation_id
    FROM live_round_state WHERE game_id = ${gameId} LIMIT 1
  `;
  if (rows.length === 0) return null;
  const r = rows[0]!;
  return {
    gameId: r.game_id,
    lifecycle: r.lifecycle as LiveLifecycle,
    beganAt: r.began_at ? new Date(r.began_at).toISOString() : null,
    crashedAt: r.crashed_at ? new Date(r.crashed_at).toISOString() : null,
    multiplier: r.multiplier,
    source: r.source as LiveSource,
    correlationId: r.correlation_id,
  };
}

/** True when the target has already begun as a live round. */
export async function hasTargetStarted(
  targetGameId: string,
  sql?: Sql,
): Promise<boolean> {
  const row = await getLiveRound(targetGameId, sql);
  if (!row) return false;
  return (
    row.lifecycle === "STARTED" ||
    row.lifecycle === "RUNNING" ||
    row.lifecycle === "ENDED" ||
    row.lifecycle === "RECONCILED"
  );
}

export async function markLiveRoundStarted(
  gameId: string,
  beganAt: string,
  source: LiveSource = "socket",
  correlationId?: string,
  sql?: Sql,
): Promise<void> {
  const db = sql ?? (await getSql());
  await db`
    INSERT INTO live_round_state (
      game_id, lifecycle, began_at, source, correlation_id, updated_at
    ) VALUES (
      ${gameId}, 'STARTED', ${beganAt}::timestamptz, ${source},
      ${correlationId ?? null}, now()
    )
    ON CONFLICT (game_id) DO UPDATE SET
      lifecycle = CASE
        WHEN live_round_state.lifecycle IN ('DISCOVERED') THEN 'STARTED'
        WHEN live_round_state.lifecycle IN ('STARTED', 'RUNNING', 'ENDED', 'RECONCILED')
          THEN live_round_state.lifecycle
        ELSE 'STARTED'
      END,
      began_at = COALESCE(live_round_state.began_at, EXCLUDED.began_at),
      source = CASE
        WHEN live_round_state.source = 'socket' THEN live_round_state.source
        ELSE EXCLUDED.source
      END,
      correlation_id = COALESCE(live_round_state.correlation_id, EXCLUDED.correlation_id),
      updated_at = now()
  `;
}

export async function markLiveRoundEnded(
  gameId: string,
  crashedAt: string,
  multiplier: number,
  sql?: Sql,
  source: LiveSource = "socket",
): Promise<void> {
  const db = sql ?? (await getSql());
  await db`
    INSERT INTO live_round_state (
      game_id, lifecycle, crashed_at, multiplier, source, updated_at
    ) VALUES (
      ${gameId}, 'ENDED', ${crashedAt}::timestamptz, ${multiplier}, ${source}, now()
    )
    ON CONFLICT (game_id) DO UPDATE SET
      lifecycle = CASE
        WHEN live_round_state.lifecycle = 'RECONCILED' THEN 'RECONCILED'
        ELSE 'ENDED'
      END,
      crashed_at = COALESCE(EXCLUDED.crashed_at, live_round_state.crashed_at),
      multiplier = COALESCE(EXCLUDED.multiplier, live_round_state.multiplier),
      updated_at = now()
  `;
}

export async function upsertLiveRoundFromHistory(
  round: FetchedRound,
  sql?: Sql,
): Promise<void> {
  const db = sql ?? (await getSql());
  const began =
    round.beganAt instanceof Date
      ? round.beganAt.toISOString()
      : round.beganAt
        ? String(round.beganAt)
        : null;
  const crashed =
    round.crashedAt instanceof Date
      ? round.crashedAt.toISOString()
      : round.crashedAt
        ? String(round.crashedAt)
        : null;
  const lifecycle: LiveLifecycle = crashed ? "ENDED" : began ? "STARTED" : "DISCOVERED";
  try {
    await db`
      INSERT INTO live_round_state (
        game_id, lifecycle, began_at, crashed_at, multiplier, source, updated_at
      ) VALUES (
        ${round.gameId},
        ${lifecycle},
        ${began}::timestamptz,
        ${crashed}::timestamptz,
        ${Number(round.multiplier)},
        'history',
        now()
      )
      ON CONFLICT (game_id) DO UPDATE SET
        lifecycle = CASE
          WHEN live_round_state.lifecycle = 'RECONCILED' THEN 'RECONCILED'
          WHEN live_round_state.source = 'socket'
            AND live_round_state.lifecycle IN ('STARTED', 'RUNNING', 'ENDED')
            THEN live_round_state.lifecycle
          ELSE EXCLUDED.lifecycle
        END,
        began_at = COALESCE(live_round_state.began_at, EXCLUDED.began_at),
        crashed_at = COALESCE(live_round_state.crashed_at, EXCLUDED.crashed_at),
        multiplier = COALESCE(live_round_state.multiplier, EXCLUDED.multiplier),
        updated_at = now()
    `;
  } catch (e) {
    logger.debug(
      { component: "live-round-state", gameId: round.gameId, error: String(e) },
      "upsert from history failed (table may be missing)",
    );
  }
}

export async function markReconciled(gameId: string, sql?: Sql): Promise<void> {
  const db = sql ?? (await getSql());
  await db`
    UPDATE live_round_state
    SET lifecycle = 'RECONCILED', updated_at = now()
    WHERE game_id = ${gameId}
  `;
}
