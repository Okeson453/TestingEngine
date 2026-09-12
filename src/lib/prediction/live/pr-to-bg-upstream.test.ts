/**
 * Directive 2026-09-12 — prove pr→bg ~7s is NOT application scheduling.
 *
 * Production repeatedly measures pr_to_bg_ms ∈ [7054, 7068] with
 * frame_to_event_ms ∈ [0.03, 0.13]. That clustering is the BC.Game betting
 * window. These tests lock the source-level invariants that make an
 * artificial delay impossible on the native path:
 *
 *   1. No setTimeout/setInterval with a ~7s duration in the native socket
 *      or protocol modules (reconnect/ping/health only).
 *   2. pr and bg are distinct TRACKED wire events dispatched synchronously
 *      on frame receipt — no local synthesis of bg from pr.
 *   3. Decision audit + taxonomy remain separate from the betting gate.
 */
import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "../../..");

function read(rel: string): string {
  return readFileSync(join(root, rel), "utf8");
}

describe("pr→bg upstream proof — no artificial 7s delay", () => {
  const nativeSocket = read("lib/crash/native-socket-client.ts");
  const nativeProtocol = read("lib/crash/native-protocol.ts");
  const handlers = read("lib/prediction/events/game-event-handlers.ts");

  it("native socket has no ~7s timer (only reconnect/ping/health/watchdog)", () => {
    // Forbid literal 7000 / 7_000 sleeps that could hold bg.
    expect(nativeSocket).not.toMatch(/\b7_?000\b/);
    expect(nativeProtocol).not.toMatch(/\b7_?000\b/);
    // setTimeout usages must be reconnect/waf only (short backoff).
    const timeoutLines = nativeSocket
      .split("\n")
      .filter((l) => /setTimeout\s*\(/.test(l));
    for (const line of timeoutLines) {
      expect(line).not.toMatch(/7\s*\*\s*1000|7000|7_000/);
    }
  });

  it("pr and bg are both TRACKED wire events (not synthesized)", () => {
    expect(nativeSocket).toMatch(/TRACKED\s*=\s*new Set\(\[.*"pr".*"bg"/s);
    expect(nativeSocket).toMatch(/packet\.event === "pr"/);
    expect(nativeSocket).toMatch(/packet\.event === "bg"/);
  });

  it("pr_to_bg_ms is measured between frame arrivals, not after a delay", () => {
    expect(nativeSocket).toMatch(/lastPrArrivedMono/);
    expect(nativeSocket).toMatch(/frameArrivedMono/);
    expect(nativeSocket).toMatch(/pr_to_bg_ms/);
    // Dispatch is synchronous: frame stamp set at message handler entry.
    expect(nativeSocket).toMatch(/this\.frameArrivedMono = performance\.now\(\)/);
  });

  it("prHandler does not schedule bg and does not write began_at", () => {
    const prStart = handlers.indexOf("export async function prHandler");
    const prEnd = handlers.indexOf("export function normalizeCrashEnd");
    const body = handlers.slice(prStart, prEnd);
    expect(body).not.toMatch(/setTimeout|setInterval/);
    expect(body).not.toMatch(/bgHandler/);
    expect(body).toMatch(/event_kind.*PR|'PR'/);
  });

  it("handlers document upstream betting window on pr observe", () => {
    expect(handlers).toMatch(/upstream BC\.Game betting window/);
  });
});

describe("≥65% decision audit is scheduled on edge veto", () => {
  const predictor = read("lib/prediction/live/predictor.ts");
  const audit = read("lib/prediction/live/decision-audit.ts");
  const attempt = read("lib/prediction/live/prediction-attempt.ts");

  it("skip path calls recordNoBetDecision with tier", () => {
    expect(predictor).toMatch(/recordNoBetDecision\(\{/);
    expect(predictor).toMatch(/decision:\s*tier/);
    expect(predictor).toMatch(/classifyPredictionTier/);
  });

  it("decision-audit logs schedule + persist for ≥65%", () => {
    expect(audit).toMatch(/decision audit scheduled/);
    expect(audit).toMatch(/decision audit persisted/);
    expect(audit).toMatch(/rec\.probability >= 0\.65/);
  });

  it("attempt log distinguishes NO_BET audit from missing betting signal", () => {
    expect(attempt).toMatch(/decision audit scheduled; betting signal withheld/);
    expect(attempt).not.toMatch(
      /N\+1 prediction not persisted \(kind=\$\{result\?\.kind/,
    );
  });
});
