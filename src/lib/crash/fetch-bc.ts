import { SOURCE_URL } from "./types";

const HISTORY_URL = "https://bc.game/api/game/bet/multi/history";
const STAT_URL = "https://bc.game/api/game/home/game-stat/";
const UA =
  "Mozilla/5.0 (compatible; CrashWave/1.0; +https://bc.game/game/crash)";

export type FetchedRound = {
  gameId: string;
  multiplier: number;
  hash: string | null;
  salt: string | null;
  beganAt: Date | null;
  crashedAt: Date;
};

type HistoryResponse = {
  code?: number;
  msg?: string | null;
  data?: {
    list?: Array<{ gameId?: string; gameDetail?: string }>;
  };
};

type StatResponse = {
  code?: number;
  data?: { onlinePlayers?: number | null };
};

type GameDetail = {
  hash?: string;
  salt?: string;
  rate?: string | number;
  beginTime?: number;
  endTime?: number;
  prepareTime?: number;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseDetail(raw: string): GameDetail | null {
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!isRecord(parsed)) return null;
    return parsed as GameDetail;
  } catch {
    return null;
  }
}

function toDate(ms: unknown): Date | null {
  if (typeof ms !== "number" || !Number.isFinite(ms) || ms < 1_000_000_000_000) {
    return null;
  }
  const date = new Date(ms);
  return Number.isNaN(date.getTime()) ? null : date;
}

export function parseFetchedRound(
  gameId: string | undefined,
  gameDetail: string | undefined,
): FetchedRound | null {
  if (!gameId || !/^\d+$/.test(gameId)) return null;
  if (!gameDetail) return null;
  const detail = parseDetail(gameDetail);
  if (!detail) return null;

  const multiplier = Number(detail.rate);
  if (!Number.isFinite(multiplier) || multiplier < 1 || multiplier > 1_000_000) {
    return null;
  }

  const crashedAt =
    toDate(detail.endTime) ?? toDate(detail.beginTime) ?? toDate(detail.prepareTime);
  if (!crashedAt) return null;

  return {
    gameId,
    multiplier,
    hash: typeof detail.hash === "string" ? detail.hash : null,
    salt: typeof detail.salt === "string" ? detail.salt : null,
    beganAt: toDate(detail.beginTime),
    crashedAt,
  };
}

async function postJson<T>(url: string, body: unknown, timeoutMs: number): Promise<T> {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      accept: "application/json, text/plain, */*",
      "content-type": "application/json",
      origin: "https://bc.game",
      referer: SOURCE_URL,
      "user-agent": UA,
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!response.ok) {
    throw new Error(`BC.Game responded ${response.status}`);
  }
  return (await response.json()) as T;
}

/**
 * Fetch crash history from BC.Game.
 * Fetches up to `maxPages` (default 20 = 1,000 rounds) to catch recent rounds.
 * Use `fetchCrashHistoryDeep` for full backfill.
 */
export async function fetchCrashHistory(maxPages = 20): Promise<FetchedRound[]> {
  const rounds: FetchedRound[] = [];
  const seen = new Set<string>();

  for (let page = 1; page <= maxPages; page++) {
    const payload = await postJson<HistoryResponse>(
      HISTORY_URL,
      { gameUrl: "crash", page, pageSize: 50 },
      12_000,
    );
    if (payload.code !== 0) {
      if (page === 1) {
        throw new Error(payload.msg || "BC.Game history request failed");
      }
      break; // Stop paginating if later pages error
    }

    const list = payload.data?.list ?? [];
    if (list.length === 0) break; // No more history

    for (const row of list) {
      const parsed = parseFetchedRound(row.gameId, row.gameDetail);
      if (!parsed) continue;
      if (seen.has(parsed.gameId)) continue;
      seen.add(parsed.gameId);
      rounds.push(parsed);
    }
  }

  return rounds;
}

export async function fetchOnlinePlayers(): Promise<number | null> {
  try {
    const payload = await postJson<StatResponse>(
      STAT_URL,
      { gameUrl: "crash" },
      8_000,
    );
    if (payload.code !== 0) return null;
    const n = payload.data?.onlinePlayers;
    return typeof n === "number" && Number.isFinite(n) ? n : null;
  } catch {
    return null;
  }
}
