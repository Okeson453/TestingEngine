/**
 * Live-path rolling history buffer — process-level singleton.
 *
 * PostgreSQL remains authoritative. This buffer is warmed once from
 * `crash_rounds` at boot (or on first cold read) and then appended on every
 * completed ED so the hot prediction path can read prior rounds from memory
 * with zero DB RTT.
 *
 * Safe under concurrent append/getPrior (single-threaded Node event loop;
 * append is idempotent on game_id).
 */
import type { Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";
import type { HistoricalRound } from "@/lib/prediction/types";
import { RollingHistoryBuffer } from "@/lib/prediction/rolling-history-buffer";

const logger = getLogger("live-history-buffer");

const DEFAULT_MAX = 200;
/** Minimum rounds required before the N+1 hot path may compute a prediction.
 * Matches predictor MIN_HISTORY — freshness requires a ready buffer, not SQL. */
export const MIN_HISTORY_FOR_PREDICTION = Number(
  process.env.LIVE_HISTORY_MIN ?? 20,
);

const buffer = new RollingHistoryBuffer(DEFAULT_MAX);
let warmPromise: Promise<void> | null = null;
/** True only after a successful warm with size >= MIN_HISTORY_FOR_PREDICTION. */
let historyReady = false;

interface CrashRow {
  game_id: string;
  multiplier: string | number;
  began_at: string | Date | null;
  crashed_at: string | Date;
}

function mapRow(r: CrashRow): HistoricalRound {
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

/**
 * Warm (or re-warm) the buffer from crash_rounds. Concurrent-safe.
 */
export async function warmLiveHistoryBuffer(
  sql: Sql,
  limit = DEFAULT_MAX,
  force = false,
): Promise<void> {
  if (buffer.isWarmed() && !force) return;
  if (warmPromise && !force) {
    await warmPromise;
    return;
  }
  warmPromise = (async () => {
    try {
      const rows = await sql<CrashRow>`
        SELECT game_id, multiplier, began_at, crashed_at
        FROM crash_rounds
        WHERE crashed_at IS NOT NULL
        ORDER BY crashed_at DESC, game_id DESC
        LIMIT ${limit}
      `;
      // rows are newest-first; buffer expects chronological ascending
      const chronological = rows.reverse().map(mapRow);
      buffer.warm(chronological);
      historyReady = chronological.length >= MIN_HISTORY_FOR_PREDICTION;
      if (historyReady) {
        logger.info(
          {
            component: "live-history-buffer",
            size: chronological.length,
            minRequired: MIN_HISTORY_FOR_PREDICTION,
          },
          "Live history buffer warmed and READY for N+1 prediction",
        );
      } else {
        logger.error(
          {
            component: "live-history-buffer",
            size: chronological.length,
            minRequired: MIN_HISTORY_FOR_PREDICTION,
          },
          "P0 health fault: history buffer warmed but below MIN_HISTORY — N+1 predictions blocked (no SQL fallback on hot path)",
        );
      }
    } catch (e) {
      historyReady = false;
      logger.error(
        { component: "live-history-buffer", error: String(e) },
        "P0 health fault: failed to warm live history buffer — N+1 predictions blocked (no SQL fallback on hot path)",
      );
    }
  })();
  try {
    await warmPromise;
  } finally {
    warmPromise = null;
  }
}

/**
 * Append a just-completed round. Idempotent. No-op if buffer not yet warmed
 * (warm will pick it up on next load; append after warm is preferred).
 */
export function appendCompletedRound(round: {
  gameId: string;
  multiplier: number;
  crashedAt: string;
  beganAt?: string | null;
}): void {
  if (!Number.isFinite(round.multiplier) || round.multiplier <= 0) return;
  const h: HistoricalRound = {
    id: round.gameId,
    externalRoundId: round.gameId,
    sessionId: null,
    startedAt: round.beganAt ?? null,
    crashedAt: round.crashedAt,
    crashPoint: round.multiplier,
    observationSource: "bc-game-socket",
    dataQuality: "high",
    createdAt: round.crashedAt,
    sequenceIndex: undefined,
  };
  buffer.append(h);
  // Async recovery path: after a cold/failed warm, live appends can restore readiness.
  if (
    buffer.isWarmed() &&
    !historyReady &&
    buffer.size() >= MIN_HISTORY_FOR_PREDICTION
  ) {
    historyReady = true;
    logger.info(
      {
        component: "live-history-buffer",
        size: buffer.size(),
        minRequired: MIN_HISTORY_FOR_PREDICTION,
      },
      "Live history buffer recovered READY via live appends",
    );
  }
}

/**
 * Synchronous memory read for the prediction hot path.
 * Returns empty array if not warmed (caller falls back to SQL).
 */
export function getPriorRoundsSync(
  limit: number,
  excludeGameId?: string,
  beforeCrashedAt?: string,
): HistoricalRound[] {
  if (!buffer.isWarmed()) return [];
  // getPrior's 3rd arg excludes by external id — the time cutoff is
  // applied by the filter below, not by an id exclusion.
  let prior = buffer.getPrior(limit, excludeGameId);
  if (beforeCrashedAt) {
    const cutoff = new Date(beforeCrashedAt).getTime();
    if (Number.isFinite(cutoff)) {
      prior = prior.filter((r) => {
        if (!r.crashedAt) return false;
        const t = new Date(r.crashedAt).getTime();
        return Number.isFinite(t) && t < cutoff;
      });
    }
  }
  // Keep most recent `limit`
  if (prior.length > limit) {
    prior = prior.slice(-limit);
  }
  return prior;
}

export function isLiveHistoryWarmed(): boolean {
  return buffer.isWarmed();
}

/**
 * Hard readiness gate for the N+1 hot path: buffer must be warmed AND
 * hold at least MIN_HISTORY_FOR_PREDICTION rounds. Prefer explicit
 * N+1_UNAVAILABLE_HISTORY over a silent 700–1000ms Neon query.
 */
export function isHistoryReadyForPrediction(): boolean {
  return historyReady && buffer.isWarmed() && buffer.size() >= MIN_HISTORY_FOR_PREDICTION;
}

export function liveHistorySize(): number {
  return buffer.size();
}

/** Test helper only. */
export function _resetLiveHistoryBufferForTests(): void {
  buffer.clear();
  warmPromise = null;
  historyReady = false;
}
