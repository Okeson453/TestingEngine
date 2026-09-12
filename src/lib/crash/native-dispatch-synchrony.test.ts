/**
 * Native WS dispatch synchrony — directive 17:10Z regression tests.
 *
 * Proves the pr→bg latency is NOT introduced by application scheduling:
 * the entire chain raw frame arrival → EIO/protobuf parse → event
 * classification → state update → handler dispatch must be synchronous
 * (single tick, no timers, no awaits, no queues). The prod-evidence
 * counterpart is frame_to_event_ms=0.04-0.13 and pr_to_bg_ms≈6900-6970 in
 * the raw logs: BC.Game's own betting-open→begin window (6.972s per the
 * public gameDetail timeline), identical upstream of this socket.
 */

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const SRC = readFileSync(
  fileURLToPath(new URL("./native-socket-client.ts", import.meta.url)),
  "utf8",
);

/** Slice between two source landmarks. */
function between(start: string, end: string): string {
  const a = SRC.indexOf(start);
  expect(a, `landmark not found: ${start}`).toBeGreaterThan(-1);
  const b = SRC.indexOf(end, a);
  expect(b, `end landmark not found: ${end}`).toBeGreaterThan(-1);
  return SRC.slice(a, b);
}

describe("native WS dispatch synchrony (no app-side pr→bg delay)", () => {
  it("frame-arrival → handler dispatch contains no await / timers / queues", () => {
    // onText through the handlers loop covers: EIO parse, packet parse,
    // event classification, state updates, and BG/ED dispatch.
    const region = between("private onText(", "for (const h of this.handlers)");
    expect(region).not.toMatch(/\bawait /);
    expect(region).not.toContain("setTimeout(");
    expect(region).not.toContain("setInterval(");
    expect(region).not.toContain("queueMicrotask(");
    expect(region).not.toContain("process.nextTick(");
    expect(region).not.toContain(".then(");
  });

  it("handlers are dispatched in a synchronous for-loop (no async boundary)", () => {
    const loop = between("for (const h of this.handlers)", "private startPing(");
    expect(loop).toMatch(/h\(ev\)/); // direct synchronous call
    expect(loop).not.toMatch(/\bawait /);
  });

  it("bg log carries the monotonic wire-gap instrumentation", () => {
    // The instrumentation that proves the ~6.9s is game-side must stay:
    // frame_to_event_ms (our dispatch cost) and pr_to_bg_ms (the game's
    // betting-open→begin interval measured between wire frames).
    expect(SRC).toContain("frame_to_event_ms=");
    expect(SRC).toContain("pr_to_bg_ms=");
    expect(SRC).toContain("lastPrArrivedMono");
  });
});
