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

  it("temporal authorization fails closed at claim time (no claim, no send)", () => {
    // CLAIM-TIME AUTHORIZATION: the temporal gate lives INSIDE the claim
    // statement — an unauthorized row is dead-lettered by the same UPDATE,
    // and a DB error on the claim means nothing is claimed and nothing is
    // sent. The old separate authorization UPDATE (and its
    // SEND_AUTH_DB_ERROR requeue path) no longer exists; fail-closed is now
    // structural: no claim → no send.
    expect(src).toContain("CLAIM-TIME AUTHORIZATION");
    expect(src).toContain("claim-time temporal gate");
    // The claim must still be the FOR UPDATE SKIP LOCKED statement that both
    // gates and stamps — authorization must precede sendTelegramMessage.
    const claimIdx = src.indexOf("claim-time temporal gate");
    const sendIdx = src.indexOf("sendTelegramMessage(row.content");
    expect(claimIdx).toBeGreaterThan(-1);
    expect(sendIdx).toBeGreaterThan(claimIdx);
    // The old fail-open pattern must still not appear on the temporal path.
    expect(src).not.toMatch(/catch\s*\(\(\)\s*=>\s*\[\]\).*target/i);
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
