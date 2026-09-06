/**
 * Phase 14 — Raise PG_POOL_MAX only from measured pool wait pressure.
 *
 * Does not auto-mutate process.env mid-flight (Pool is already constructed).
 * Samples waitingCount + idle pressure; exposes p95/p99 guidance for operators
 * and logs when measured pressure justifies a higher PG_POOL_MAX on next boot.
 */
import { getPoolStats, getPgPool } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";
import { poolWaitMs } from "@/lib/observability/performance/latency";

const logger = getLogger("pool-sizing");

const samples: number[] = [];
const MAX_SAMPLES = 500;

/** Hard ceiling — never recommend above this without explicit env override. */
const HARD_CAP = Math.max(3, Number(process.env.PG_POOL_HARD_CAP ?? 10) || 10);

/** Soft floor used when env is unset. */
const DEFAULT_MAX = 3;

/**
 * Observe current pool pressure. Call from heartbeat / after heavy DB work.
 * waitingCount > 0 is treated as waitMs proxy (connection queue depth * 50ms).
 */
export function observePoolPressure(): void {
  const stats = getPoolStats();
  if (!stats) return;
  // Proxy: each waiting client ≈ at least connectionTimeout / queue depth pressure.
  // Prefer real wait if we ever instrument acquire; until then use waitingCount * 25ms.
  const proxyWaitMs = Math.max(0, stats.waitingCount) * 25 +
    (stats.totalCount >= stats.max && stats.idleCount === 0 ? 40 : 0);
  samples.push(proxyWaitMs);
  if (samples.length > MAX_SAMPLES) samples.shift();
  try {
    poolWaitMs.observe(proxyWaitMs);
  } catch {
    /* optional metrics */
  }
}

function percentile(sorted: number[], p: number): number | null {
  if (sorted.length === 0) return null;
  const idx = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil((p / 100) * sorted.length) - 1),
  );
  return sorted[idx]!;
}

export interface PoolSizingAdvice {
  currentMax: number;
  recommendedMax: number;
  p50WaitMs: number | null;
  p95WaitMs: number | null;
  p99WaitMs: number | null;
  sampleCount: number;
  shouldRaise: boolean;
  reason: string;
}

/**
 * Recommendation rules (Phase 14):
 * - p95 wait proxy < 25ms → keep current (or DEFAULT_MAX)
 * - p95 in [25, 100) → +1 (capped)
 * - p95 >= 100 or p99 >= 200 → +2 (capped)
 * Never raise above HARD_CAP. Never lower below env PG_POOL_MAX if set.
 */
export function getPoolSizingAdvice(): PoolSizingAdvice {
  const stats = getPoolStats();
  const currentMax = stats?.max ?? Number(process.env.PG_POOL_MAX ?? DEFAULT_MAX) || DEFAULT_MAX;
  const sorted = samples.slice().sort((a, b) => a - b);
  const p50 = percentile(sorted, 50);
  const p95 = percentile(sorted, 95);
  const p99 = percentile(sorted, 99);

  let recommended = currentMax;
  let reason = "stable";
  let shouldRaise = false;

  if (samples.length >= 20 && p95 != null) {
    if (p95 >= 100 || (p99 != null && p99 >= 200)) {
      recommended = Math.min(HARD_CAP, currentMax + 2);
      reason = `p95=${p95}ms p99=${p99}ms → raise by 2`;
      shouldRaise = recommended > currentMax;
    } else if (p95 >= 25) {
      recommended = Math.min(HARD_CAP, currentMax + 1);
      reason = `p95=${p95}ms → raise by 1`;
      shouldRaise = recommended > currentMax;
    } else {
      reason = `p95=${p95}ms within budget; keep max=${currentMax}`;
    }
  } else {
    reason = `insufficient samples (${samples.length}); keep max=${currentMax}`;
  }

  return {
    currentMax,
    recommendedMax: recommended,
    p50WaitMs: p50,
    p95WaitMs: p95,
    p99WaitMs: p99,
    sampleCount: samples.length,
    shouldRaise,
    reason,
  };
}

/** Log advice when raise is warranted (operators set PG_POOL_MAX on next deploy). */
export function logPoolSizingAdvice(): void {
  observePoolPressure();
  const advice = getPoolSizingAdvice();
  if (advice.shouldRaise) {
    logger.warn(
      {
        component: "pool-sizing",
        ...advice,
        action: "set PG_POOL_MAX on next boot — live pool.max is fixed after construct",
      },
      `Phase 14: measured pool wait justifies PG_POOL_MAX=${advice.recommendedMax}`,
    );
  } else if (advice.sampleCount > 0 && advice.sampleCount % 30 === 0) {
    logger.info(
      { component: "pool-sizing", ...advice },
      "pool sizing sample",
    );
  }
}

/** Test helper */
export function resetPoolSizingSamples(): void {
  samples.length = 0;
}

/**
 * Optional: if pool exists and env PG_POOL_MAX_DYNAMIC=1, we cannot grow
 * node-pg Pool.max at runtime safely — only log. Kept for documentation.
 */
export function noteRuntimePoolImmutable(): void {
  if (getPgPool() && process.env.PG_POOL_MAX_DYNAMIC === "1") {
    const advice = getPoolSizingAdvice();
    if (advice.shouldRaise) {
      logger.warn(
        { component: "pool-sizing", advice },
        "PG_POOL_MAX_DYNAMIC requested but node-pg Pool.max is fixed after construction",
      );
    }
  }
}
