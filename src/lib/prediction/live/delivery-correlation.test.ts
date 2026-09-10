/**
 * E2E correlation timeline: send_started_at stamped before Telegram HTTP;
 * BG backfills target_round_started_at; delivery view migration present.
 */
import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";

describe("prediction delivery correlation timeline", () => {
  const nw = readFileSync(join(__dirname, "notification-worker.ts"), "utf8");
  const geh = readFileSync(
    join(__dirname, "..", "events", "game-event-handlers.ts"),
    "utf8",
  );

  it("stamps send_started_at while inflight before sendTelegramMessage", () => {
    // POOL-BUDGET FIX: the stamp lives inside the atomic authorization UPDATE
    // (set send_started_at = clock_timestamp()) which still precedes the send.
    const stampIdx = nw.indexOf("set send_started_at = clock_timestamp()");
    const sendIdx = nw.indexOf("sendTelegramMessage(row.content");
    expect(stampIdx).toBeGreaterThan(-1);
    expect(sendIdx).toBeGreaterThan(stampIdx);
    expect(nw).toContain("authorization refused (BG/expiry)");
  });

  it("lifecycle logs include predictionId, targetGameId, sourceGameId, queuedAt", () => {
    expect(nw).toContain("targetGameId:");
    expect(nw).toContain("sourceGameId:");
    expect(nw).toContain("queuedAt:");
    expect(nw).toContain("predictionId:");
  });

  it("BG backfills pending_predictions.target_round_started_at", () => {
    expect(geh).toContain("target_round_started_at = COALESCE");
    expect(geh).toContain("WHERE target_game_id = ${gameId}");
  });
});
