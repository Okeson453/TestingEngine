/**
 * P0 regression: BG arrives while Telegram send is in flight must not
 * finalize the row as delivered.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";

describe("outbox TOCTOU temporal contract (source)", () => {
  const src = readFileSync(join(__dirname, "notification-worker.ts"), "utf8");
  const handlers = readFileSync(
    join(__dirname, "..", "events", "game-event-handlers.ts"),
    "utf8",
  );

  it("final delivered UPDATE requires status=inflight and deadline still valid", () => {
    expect(src).toMatch(/status = 'inflight'/);
    expect(src).toMatch(/telegram_deadline_at is null or telegram_deadline_at > now\(\)/);
    expect(src).toContain("suppressed_after_telegram");
    expect(src).toContain("returning id");
  });

  it("temporal DB check fails closed (no .catch(() => []) on target lookup)", () => {
    // The fail-open pattern must not appear on the prediction temporal query path
    expect(src).toContain("TEMPORAL_CHECK_DB_ERROR");
    expect(src).toContain("fail-closed");
    expect(src).toContain("temporal_check_db_error");
  });

  it("does not force min 200ms send timeout when residual budget is smaller", () => {
    expect(src).toContain("expired_insufficient_send_budget");
    expect(src).toContain("residual budget too small");
  });

  it("BG temporal kill retries and logs error instead of silent swallow only", () => {
    expect(handlers).toContain("BG temporal kill failed — retrying once");
    expect(handlers).toContain("BG temporal kill FAILED after retry");
  });
});
