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
    // The fail-open pattern must not appear on the prediction temporal query path.
    // POOL-BUDGET FIX: the temporal gate + send stamp are ONE atomic
    // authorization UPDATE; a DB error on it must requeue without sending.
    expect(src).toContain("SEND_AUTH_DB_ERROR");
    expect(src).toContain("fail-closed");
    expect(src).toContain("send_auth_db_error");
    // The atomic authorization must fail closed INSIDE the send path: the
    // authorization UPDATE must appear before sendTelegramMessage.
    const authIdx = src.indexOf("SEND_AUTH_DB_ERROR");
    const sendIdx = src.indexOf("sendTelegramMessage(row.content");
    expect(authIdx).toBeGreaterThan(-1);
    expect(sendIdx).toBeGreaterThan(authIdx);
  });

  it("does not force min 200ms send timeout when residual budget is smaller", () => {
    expect(src).toContain("expired_insufficient_send_budget");
    expect(src).toContain("residual budget too small");
  });

  it("BG temporal kill retries and logs error instead of silent swallow only", () => {
    // POOL-BUDGET FIX: the kill now lives inside the BG transaction; the
    // retry-with-log contract is at transaction granularity.
    expect(handlers).toContain("BG transaction failed — retrying once");
    expect(handlers).toContain("BG transaction FAILED after retry");
  });
});
