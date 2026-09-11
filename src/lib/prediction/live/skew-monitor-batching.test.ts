/**
 * Forensic pass 11 (sep 11) — DB-layer audit regressions.
 *
 * clock-skew-monitor.measureOnce issued 2-5 SEQUENTIAL worker_state upserts
 * (p95, p50, plus wall_clock_skew_ms / clock_skew_action /
 * effective_skip_below_ms / sheath_force_warn on high skew) — 5-6 sequential
 * Neon RTTs (~150ms each) every 5 minutes on the general pool: the same
 * multi-RTT pattern the BG reconcile CTE fix eliminated. Now ONE batched
 * multi-row upsert via sql.query with numbered placeholders (the Sql
 * wrapper is template-only — arrays would serialize as a single JSON param).
 *
 * The exact generated SQL was PGLite-verified pre-push (2-row and 5-row
 * cases, on-conflict overwrite) and the outbox claim's use of the 0041
 * partial index was EXPLAIN-proven (Index Scan + Limit on the ordered
 * partial index) in the same repro.
 */
import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const src = readFileSync(join(here, "clock-skew-monitor.ts"), "utf8");

describe("clock-skew-monitor batched state write", () => {
  it("measureOnce issues ONE batched upsert, not sequential per-key statements", () => {
    const body = src.slice(src.indexOf("async measureOnce"));
    expect(body).toContain("sql.query(");
    expect(body).toContain("on conflict (key) do update set value = excluded.value");
    // The old sequential per-key upserts must be gone.
    expect(body).not.toContain("values ('last_bg_to_recv_lag_ms_p95'");
    expect(body).not.toContain("values ('last_bg_to_recv_lag_ms_p50'");
  });

  it("uses numbered-placeholder rows, not an array param (wrapper is template-only)", () => {
    const body = src.slice(src.indexOf("async measureOnce"));
    expect(body).toContain("flatParams.push(r.key, r.value)");
    // A raw array passed through a template would JSON-serialize — regression guard:
    expect(body).not.toContain("${sql(stateRows");
  });

  it("preserves the gate-cache side effects from the corrective action", () => {
    const body = src.slice(src.indexOf("async measureOnce"));
    expect(body).toContain("setWallClockSkewMs(wallClockSkewMs)");
    expect(body).toContain("setEffectiveSkipBelowMs(Number(actionRow.value))");
  });

  it("cadence unchanged (5 min default — no realtime impact either way)", () => {
    expect(src).toContain("CLOCK_SKEW_INTERVAL_MS ?? 5 * 60 * 1_000");
  });
});
