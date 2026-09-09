import type { NormalizedRoundEvent, ValidatedRoundEvent } from "./types";
import { RealtimeMetrics } from "./metrics";

const STALE_MS = 90_000;

export type ValidateResult =
  | { ok: true; event: ValidatedRoundEvent; missed: number }
  | { ok: false; reason: "duplicate" | "stale" | "invalid" | "out_of_order" };

export class RoundValidator {
  private readonly seen = new Map<string, number>();
  private lastEndId: number | null = null;

  constructor(private readonly metrics: RealtimeMetrics) {}

  hydrate(gameIds: string[]): void {
    for (const id of gameIds) {
      if (/^\d+$/.test(id)) this.seen.set(`end:${id}`, Date.now());
    }
    const numeric = gameIds.map((id) => Number(id)).filter((n) => Number.isFinite(n));
    if (numeric.length > 0) this.lastEndId = Math.max(...numeric);
  }

  validate(event: NormalizedRoundEvent): ValidateResult {
    if (!/^\d+$/.test(event.gameId)) {
      this.metrics.markInvalid();
      return { ok: false, reason: "invalid" };
    }

    if (event.phase === "end") {
      if (event.multiplier === null || event.multiplier < 1 || event.multiplier > 1_000_000) {
        this.metrics.markInvalid();
        return { ok: false, reason: "invalid" };
      }
      const crashedAt = event.crashedAt ?? event.receivedAt;
      if (!event.backfill && event.receivedAt - crashedAt > STALE_MS) {
        this.metrics.markStale();
        return { ok: false, reason: "stale" };
      }
    }

    const key = `${event.phase}:${event.gameId}`;
    if (this.seen.has(key)) {
      this.metrics.markDuplicate();
      return { ok: false, reason: "duplicate" };
    }

    let missed = 0;
    if (event.phase === "end") {
      const id = Number(event.gameId);
      if (this.lastEndId !== null && id > this.lastEndId + 1) {
        missed = id - this.lastEndId - 1;
        this.metrics.markMissed(missed);
      }
      if (this.lastEndId === null || id > this.lastEndId) this.lastEndId = id;
    }

    this.seen.set(key, event.receivedAt);
    if (this.seen.size > 20_000) {
      const keys = [...this.seen.keys()].slice(0, 5_000);
      for (const k of keys) this.seen.delete(k);
    }

    const processingMs = Date.now() - event.receivedAt;
    const arrivalLagMs = Math.max(
      0,
      event.receivedAt - (event.crashedAt ?? event.beganAt ?? event.receivedAt),
    );
    this.metrics.markAccepted(arrivalLagMs, processingMs);

    return {
      ok: true,
      missed,
      event: { ...event, acceptedAt: Date.now() },
    };
  }
}
