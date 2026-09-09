/**
 * Latency-budget instrumentation for the native BC.Game socket feed.
 *
 * Ported from the tested workspace realtime layer (normalizer → validator →
 * EMA metrics). The native socket stays the transport; this layer observes
 * every event and enforces the round-event budget:
 *   - duplicates (same phase/gameId) are dropped
 *   - stale end events (> STALE_MS behind wall clock) are dropped
 *   - missed round detection between consecutive end events
 *   - arrival lag / processing time tracked with EMA smoothing
 */
import type { NativeCrashEvent } from "@/lib/crash/native-socket-client";
import { getLogger } from "@/lib/observability/logger";
import { normalizeSourceEvent } from "./normalizer";
import { RealtimeMetrics } from "./metrics";
import { RoundValidator } from "./validator";
import type { AdapterHealth, MetricsSnapshot } from "./types";

const logger = getLogger("realtime-pipeline");

class RealtimePipeline {
  private readonly metrics = new RealtimeMetrics();
  private readonly validator = new RoundValidator(this.metrics);
  private health: AdapterHealth = {
    sourceId: "native-bc-socket",
    status: "stopped",
    transport: "websocket",
    lastError: null,
    lastEventAt: null,
    lastEventKind: null,
    reconnectAttempts: 0,
    totalReconnects: 0,
    socketId: null,
  };

  getMetrics(): MetricsSnapshot {
    return this.metrics.snapshot();
  }

  getHealth(): AdapterHealth {
    return { ...this.health };
  }

  hydrate(gameIds: string[]): void {
    this.validator.hydrate(gameIds);
  }

  onStatus(status: string, detail?: string): void {
    this.health.status = mapStatus(status);
    if (detail) this.health.lastError = detail;
    if (status === "reconnecting") {
      this.health.reconnectAttempts += 1;
      this.health.totalReconnects += 1;
      this.metrics.markReconnect();
    }
  }

  /**
   * Feed a native socket event through normalize → validate. Returns the
   * normalized event when it passes the budget checks, null when dropped.
   */
  observe(ev: NativeCrashEvent): ReturnType<typeof normalizeSourceEvent> {
    const raw = {
      sourceId: "native-bc-socket",
      sourceKind: "socket" as const,
      event: ev.event,
      payload: {
        gameId: ev.gameId,
        multiplier: ev.multiplier,
        hash: ev.hash ?? null,
        beginTime: ev.beginTime ?? null,
        endTime: ev.endTime ?? null,
        elapsedMs: ev.elapsedMs ?? null,
      },
      receivedAt: ev.receivedAt,
    };

    const normalized = normalizeSourceEvent(raw);
    if (!normalized) {
      this.metrics.markInvalid();
      return null;
    }
    this.metrics.markReceived();
    this.health.lastEventAt = ev.receivedAt;

    const result = this.validator.validate(normalized);
    if (!result.ok) return null;

    if (normalized.phase === "end") this.health.lastEventKind = "end";
    return normalized;
  }

  /** E2E budget sample: event receivedAt → now, at the point of use. */
  markE2e(receivedAt: number): void {
    this.metrics.markE2e(Date.now() - receivedAt);
  }
}

function mapStatus(status: string): AdapterHealth["status"] {
  switch (status) {
    case "connected":
    case "connecting":
    case "degraded":
    case "reconnecting":
    case "waf_blocked":
    case "stopped":
      return status;
    default:
      return "stopped";
  }
}

const globalRef = globalThis as typeof globalThis & {
  __teRealtimePipeline?: RealtimePipeline;
};

export function getRealtimePipeline(): RealtimePipeline {
  globalRef.__teRealtimePipeline ??= new RealtimePipeline();
  return globalRef.__teRealtimePipeline;
}

export function logRealtimeSnapshot(): void {
  const m = getRealtimePipeline().getMetrics();
  logger.info(
    {
      component: "realtime-pipeline",
      received: m.eventsReceived,
      accepted: m.eventsAccepted,
      duplicates: m.duplicates,
      stale: m.stale,
      invalid: m.invalid,
      missed: m.missed,
      reconnects: m.reconnects,
      lastArrivalLagMs: m.lastArrivalLagMs,
      avgProcessingMs: m.avgProcessingMs,
      lastE2eMs: m.lastE2eMs,
    },
    "realtime latency budget snapshot",
  );
}
