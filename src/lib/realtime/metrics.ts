import type { MetricsSnapshot } from "./types";

function ema(prev: number | null, sample: number, alpha = 0.25): number {
  if (prev === null) return sample;
  return prev * (1 - alpha) + sample * alpha;
}

export class RealtimeMetrics {
  private eventsReceived = 0;
  private eventsAccepted = 0;
  private duplicates = 0;
  private stale = 0;
  private invalid = 0;
  private missed = 0;
  private reconnects = 0;
  private lastArrivalLagMs: number | null = null;
  private lastProcessingMs: number | null = null;
  private lastPredictionMs: number | null = null;
  private lastDeliveryMs: number | null = null;
  private lastE2eMs: number | null = null;
  private avgProcessingMs: number | null = null;
  private avgPredictionMs: number | null = null;
  private avgDeliveryMs: number | null = null;

  markReceived(): void {
    this.eventsReceived += 1;
  }

  markAccepted(arrivalLagMs: number, processingMs: number): void {
    this.eventsAccepted += 1;
    this.lastArrivalLagMs = arrivalLagMs;
    this.lastProcessingMs = processingMs;
    this.avgProcessingMs = ema(this.avgProcessingMs, processingMs);
  }

  markDuplicate(): void {
    this.duplicates += 1;
  }

  markStale(): void {
    this.stale += 1;
  }

  markInvalid(): void {
    this.invalid += 1;
  }

  markMissed(count: number): void {
    if (count > 0) this.missed += count;
  }

  markReconnect(): void {
    this.reconnects += 1;
  }

  markPrediction(ms: number): void {
    this.lastPredictionMs = ms;
    this.avgPredictionMs = ema(this.avgPredictionMs, ms);
  }

  markDelivery(ms: number): void {
    this.lastDeliveryMs = ms;
    this.avgDeliveryMs = ema(this.avgDeliveryMs, ms);
  }

  markE2e(ms: number): void {
    this.lastE2eMs = ms;
  }

  snapshot(): MetricsSnapshot {
    return {
      eventsReceived: this.eventsReceived,
      eventsAccepted: this.eventsAccepted,
      duplicates: this.duplicates,
      stale: this.stale,
      invalid: this.invalid,
      missed: this.missed,
      reconnects: this.reconnects,
      lastArrivalLagMs: this.lastArrivalLagMs,
      lastProcessingMs: this.lastProcessingMs,
      lastPredictionMs: this.lastPredictionMs,
      lastDeliveryMs: this.lastDeliveryMs,
      avgProcessingMs: this.avgProcessingMs,
      avgPredictionMs: this.avgPredictionMs,
      avgDeliveryMs: this.avgDeliveryMs,
      lastE2eMs: this.lastE2eMs,
    };
  }
}
