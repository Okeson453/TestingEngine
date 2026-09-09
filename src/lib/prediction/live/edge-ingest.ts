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
import { timingSafeEqual } from "node:crypto";
import { getSql, type Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";
import { onGameEnd } from "@/lib/prediction/live/validator";

function tokensEqual(a: string, b: string): boolean {
  const left = Buffer.from(a, "utf8");
  const right = Buffer.from(b, "utf8");
  const n = Math.max(left.length, right.length, 1);
  const padL = Buffer.alloc(n);
  const padR = Buffer.alloc(n);
  left.copy(padL);
  right.copy(padR);
  return left.length === right.length && timingSafeEqual(padL, padR);
}

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
  | { ok: true; kind: string; gameId?: string; lagMs?: number }
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
  if (!token || !tokensEqual(token, expected)) {
    return { ok: false, error: "unauthorized", status: 401 };
  }
  return null;
}
