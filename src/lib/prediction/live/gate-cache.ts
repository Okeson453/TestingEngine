/**
 * In-process cache for infrequently-changing gate values.
 * Avoids a worker_state SQL round-trip on every onGameEndPredict hot path.
 *
 * Writers (poll-worker, clock-skew-monitor) call set* when they refresh values.
 * Readers use get* with soft fallbacks.
 */
let medianInterRoundGapMs: number | null = null;
let wallClockSkewMs: number | null = null;
let effectiveSkipBelowMs: number | null = null;
let updatedAt = 0;

const STALE_MS = 60_000;

export function setMedianInterRoundGapMs(ms: number): void {
  if (!Number.isFinite(ms) || ms <= 0) return;
  medianInterRoundGapMs = ms;
  updatedAt = Date.now();
}

export function setWallClockSkewMs(ms: number): void {
  if (!Number.isFinite(ms)) return;
  wallClockSkewMs = ms;
  updatedAt = Date.now();
}

export function setEffectiveSkipBelowMs(ms: number): void {
  if (!Number.isFinite(ms) || ms < 0) return;
  effectiveSkipBelowMs = ms;
  updatedAt = Date.now();
}

export function getMedianInterRoundGapMs(fallback = 4_000): number {
  if (medianInterRoundGapMs != null && Date.now() - updatedAt < STALE_MS * 5) {
    return medianInterRoundGapMs;
  }
  return fallback;
}

export function getWallClockSkewMs(fallback = 0): number {
  if (wallClockSkewMs != null && Date.now() - updatedAt < STALE_MS * 5) {
    return wallClockSkewMs;
  }
  return fallback;
}

export function getEffectiveSkipBelowMs(): number | null {
  if (effectiveSkipBelowMs != null && Date.now() - updatedAt < STALE_MS * 5) {
    return effectiveSkipBelowMs;
  }
  return null;
}

export function isGateCacheWarm(): boolean {
  return medianInterRoundGapMs != null || wallClockSkewMs != null;
}

/** Test helper */
export function _resetGateCacheForTests(): void {
  medianInterRoundGapMs = null;
  wallClockSkewMs = null;
  effectiveSkipBelowMs = null;
  updatedAt = 0;
}
