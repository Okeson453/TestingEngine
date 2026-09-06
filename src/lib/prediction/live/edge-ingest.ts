/**
 * Browser-edge crash ingest.
 *
 * Optional low-latency path: a residential browser (userscript/extension)
 * forwards already-decoded crash events to the worker. Same durable pipeline
 * as Socket.IO `ed` / poll recovery — idempotent by game_id.
 *
 * Env:
 *   EDGE_INGEST_TOKEN   — required Bearer token (reject if unset in production)
 *   EDGE_STALE_MS       — poll defers N+1 when last edge event younger than this (default 8000)
 */
import { getSql, type Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";
import { onGameEnd } from "@/lib/prediction/live/validator";

const logger = getLogger("edge-ingest");

export const EDGE_STALE_MS = Number(process.env.EDGE_STALE_MS ?? 8_000) || 8_000;

export type EdgeCrashPayload = {
  gameId: string;
  multiplier: number;
  /** ISO timestamp of crash; defaults to now */
  crashedAt?: string;
  beganAt?: string | null;
  hash?: string | null;
  salt?: string | null;
  /** Client observation time (ms since epoch or ISO) */
  observedAt?: string | number;
  source?: string;
};

export type EdgeBgPayload = {
  gameId: string;
  beganAt: string;
  source?: string;
};

export type EdgeIngestResult =
  | { ok: true; kind: string; gameId: string; lagMs?: number }
  | { ok: false; error: string; status: number };

function requireToken(authHeader: string | null | undefined): EdgeIngestResult | null {
  const expected = process.env.EDGE_INGEST_TOKEN?.trim();
  if (!expected) {
    if (process.env.NODE_ENV === "production") {
      return { ok: false, error: "EDGE_INGEST_TOKEN not configured", status: 503 };
    }
    // Dev: allow without token only if explicitly opted in
    if (process.env.EDGE_INGEST_ALLOW_INSECURE === "1") return null;
    return { ok: false, error: "EDGE_INGEST_TOKEN not configured", status: 503 };
  }
  const raw = (authHeader ?? "").trim();
  const token = raw.toLowerCase().startsWith("bearer ")
    ? raw.slice(7).trim()
    : raw;
  if (!token || token !== expected) {
    return { ok: false, error: "unauthorized", status: 401 };
  }
  return null;
}

function normalizeGameId(v: unknown): string | null {
  if (typeof v === "number" && Number.isFinite(v) && v > 0) return String(Math.trunc(v));
  if (typeof v === "string" && /^\d+$/.test(v.trim())) return v.trim();
  return null;
}

function toIso(v: string | number | Date | null | undefined): string | null {
  if (v == null) return null;
  if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString();
  if (typeof v === "number") {
    const ms = v < 1e12 ? v * 1000 : v;
    const d = new Date(ms);
    return Number.isNaN(d.getTime()) ? null : d.toISOString();
  }
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

/** Record edge heartbeat for poll deferral. */
export async function touchEdgeHealth(
  gameId: string,
  sql?: Sql,
): Promise<void> {
  const s = sql ?? (await getSql());
  const now = new Date().toISOString();
  try {
    await s`
      INSERT INTO worker_state (key, value)
      VALUES
        ('last_edge_event_at', ${now}),
        ('last_edge_game_id', ${gameId})
      ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()
    `;
  } catch (e) {
    logger.debug({ error: String(e) }, "touchEdgeHealth soft-failed");
  }
}

/** True when a browser-edge event arrived recently — poll should defer N+1. */
export async function isEdgeFresh(sql?: Sql): Promise<{
  fresh: boolean;
  ageMs: number | null;
  lastGameId: string | null;
}> {
  const s = sql ?? (await getSql());
  try {
    const rows = await s<{ key: string; value: string }>`
      SELECT key, value FROM worker_state
      WHERE key IN ('last_edge_event_at', 'last_edge_game_id')
    `;
    let at: string | null = null;
    let gameId: string | null = null;
    for (const r of rows) {
      if (r.key === "last_edge_event_at") at = r.value;
      if (r.key === "last_edge_game_id") gameId = r.value;
    }
    if (!at) return { fresh: false, ageMs: null, lastGameId: gameId };
    const ageMs = Date.now() - new Date(at).getTime();
    if (!Number.isFinite(ageMs) || ageMs < 0) {
      return { fresh: false, ageMs: null, lastGameId: gameId };
    }
    return { fresh: ageMs <= EDGE_STALE_MS, ageMs, lastGameId: gameId };
  } catch {
    return { fresh: false, ageMs: null, lastGameId: null };
  }
}

export async function ingestEdgeCrash(
  body: unknown,
  authHeader?: string | null,
): Promise<EdgeIngestResult> {
  const authErr = requireToken(authHeader);
  if (authErr) return authErr;

  const p = (body ?? {}) as Record<string, unknown>;
  const gameId = normalizeGameId(p.gameId ?? p.game_id ?? p.id);
  if (!gameId) return { ok: false, error: "gameId required (numeric string)", status: 400 };

  const multRaw = p.multiplier ?? p.crashPoint ?? p.crash ?? p.rate;
  const multiplier =
    typeof multRaw === "number"
      ? multRaw
      : typeof multRaw === "string"
        ? Number.parseFloat(multRaw)
        : NaN;
  if (!Number.isFinite(multiplier) || multiplier <= 0) {
    return { ok: false, error: "multiplier required (positive number)", status: 400 };
  }

  const crashedAt =
    toIso(p.crashedAt as string | number | undefined) ??
    toIso(p.endTime as string | number | undefined) ??
    toIso(p.observedAt as string | number | undefined) ??
    new Date().toISOString();

  const receivedAt = new Date().toISOString();
  const observedMs = toIso(p.observedAt as string | number | undefined);
  const lagMs = observedMs
    ? Math.max(0, Date.now() - new Date(observedMs).getTime())
    : undefined;

  await touchEdgeHealth(gameId);

  // Durable lifecycle (best-effort)
  try {
    const { markLiveRoundEnded } = await import(
      "@/lib/prediction/live/live-round-state"
    );
    await markLiveRoundEnded(gameId, crashedAt, multiplier, undefined, "browser-edge");
  } catch {
    /* soft */
  }

  try {
    const result = await onGameEnd({
      gameId,
      endTime: crashedAt,
      multiplier,
      receivedAt,
      skipPredict: false,
    });
    logger.info(
      {
        component: "edge-ingest",
        gameId,
        multiplier,
        kind: result.kind,
        lagMs,
        source: String(p.source ?? "browser-edge"),
      },
      `edge crash ingested (${result.kind})`,
    );
    return { ok: true, kind: result.kind, gameId, lagMs };
  } catch (e) {
    logger.error({ gameId, error: String(e) }, "edge crash ingest failed");
    return { ok: false, error: String(e), status: 500 };
  }
}

export async function ingestEdgeBg(
  body: unknown,
  authHeader?: string | null,
): Promise<EdgeIngestResult> {
  const authErr = requireToken(authHeader);
  if (authErr) return authErr;

  const p = (body ?? {}) as Record<string, unknown>;
  const gameId = normalizeGameId(p.gameId ?? p.game_id ?? p.id);
  if (!gameId) return { ok: false, error: "gameId required", status: 400 };
  const beganAt =
    toIso(p.beganAt as string | number | undefined) ??
    toIso(p.beginTime as string | number | undefined);
  if (!beganAt) return { ok: false, error: "beganAt required", status: 400 };

  await touchEdgeHealth(gameId);
  const sql = await getSql();

  try {
    const { markLiveRoundStarted } = await import(
      "@/lib/prediction/live/live-round-state"
    );
    await markLiveRoundStarted(gameId, beganAt, "browser-edge", undefined, sql);
  } catch {
    /* soft */
  }

  try {
    await sql`
      UPDATE pending_predictions
      SET target_round_started_at = ${beganAt}::timestamptz
      WHERE target_game_id = ${gameId}
        AND status = 'PENDING'
        AND target_round_started_at IS NULL
    `;
  } catch (e) {
    logger.warn({ gameId, error: String(e) }, "edge bg backfill failed");
  }

  logger.info({ component: "edge-ingest", gameId, beganAt }, "edge bg ingested");
  return { ok: true, kind: "bg", gameId };
}

export function verifyEdgeAuth(authHeader?: string | null): EdgeIngestResult | null {
  return requireToken(authHeader);
}
