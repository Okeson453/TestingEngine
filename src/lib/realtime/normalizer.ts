import type { NormalizedRoundEvent, RawSourceEvent, RoundPhase } from "./types";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function unwrapPayload(payload: unknown): Record<string, unknown> | null {
  if (Array.isArray(payload) && payload.length > 0) {
    return unwrapPayload(payload[0]);
  }
  if (!isRecord(payload)) return null;
  if (isRecord(payload.data)) return payload.data;
  if (isRecord(payload.payload)) return payload.payload;
  return payload;
}

function readString(obj: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const value = obj[key];
    if (typeof value === "string" && value.length > 0) return value;
    if (typeof value === "number" && Number.isFinite(value)) return String(value);
  }
  return null;
}

function readNumber(obj: Record<string, unknown>, keys: string[]): number | null {
  for (const key of keys) {
    const value = obj[key];
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (typeof value === "string" && value.trim() !== "") {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) return parsed;
    }
  }
  return null;
}

function toEpochMs(value: number | null): number | null {
  if (value === null) return null;
  if (value > 0 && value < 1_000_000_000_000) return Math.round(value * 1000);
  if (value >= 1_000_000_000_000) return Math.round(value);
  return null;
}

export function mapEventName(event: string): RoundPhase | null {
  const key = event.trim().toLowerCase();
  if (key === "pr" || key === "pg" || key === "prepare" || key === "ready") {
    return key === "pg" ? "progress" : "prepare";
  }
  if (key === "bg" || key === "begin" || key === "start") return "begin";
  if (key === "ed" || key === "st" || key === "end" || key === "crash" || key === "bust") {
    return "end";
  }
  return null;
}

export function normalizeSourceEvent(
  raw: RawSourceEvent,
  options: { backfill?: boolean } = {},
): NormalizedRoundEvent | null {
  const phase = mapEventName(raw.event);
  if (!phase) return null;
  const body = unwrapPayload(raw.payload);
  if (!body) return null;

  const gameId = readString(body, ["gameId", "game_id", "id", "roundId", "round_id"]);
  if (!gameId || !/^\d+$/.test(gameId)) return null;

  let multiplier = readNumber(body, ["multiplier", "rate", "crashPoint", "crash_point", "odds"]);
  if (multiplier !== null && multiplier >= 100 && Number.isInteger(multiplier)) {
    multiplier = multiplier / 100;
  }

  const beganAt = toEpochMs(readNumber(body, ["beganAt", "beginTime", "begin_time", "startTime"]));
  const crashedAt = toEpochMs(readNumber(body, ["crashedAt", "endTime", "end_time", "crashTime"]));

  return {
    sourceId: raw.sourceId,
    sourceKind: raw.sourceKind,
    phase,
    gameId,
    multiplier,
    hash: readString(body, ["hash"]),
    salt: readString(body, ["salt"]),
    beganAt,
    crashedAt,
    receivedAt: raw.receivedAt,
    rawEvent: raw.event,
    backfill: options.backfill === true || raw.backfill === true,
  };
}
