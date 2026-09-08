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