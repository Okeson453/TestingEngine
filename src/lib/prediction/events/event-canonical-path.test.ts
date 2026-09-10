/**
 * P1 — canonical round-processing path regression tests.
 *
 * Invariants being locked:
 *   1. Fix 11: `ed` and `st` are the SAME semantic event — both normalize to
 *      ONE canonical crash-final shape and exactly one edHandler invocation
 *      per round. (Native WS emits BOTH packets per round; the 350ms–1.2s
 *      duplicate pairs in production logs are this ed/st sequence.)
 *   2. The game-ID dedup ledger classifies the second event as
 *      duplicate_event BEFORE any claim/N+1 compute.
 *   3. Malformed payloads (no gameId) normalize to null and never enter the
 *      pipeline.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  normalizeCrashEnd,
  classifyEdReentry,
  recordEdRoundProcessedForTests,
  _resetEdDedupForTests,
} from "@/lib/prediction/events/game-event-handlers";

test("ed and st for the same round normalize to ONE canonical shape", () => {
  const base = {
    gameId: "424242",
    multiplier: 1.97,
    endTime: 1725900000000,
    hash: "abc",
  };
  const asEd = normalizeCrashEnd(base, "ed");
  const asSt = normalizeCrashEnd(base, "st");
  assert.ok(asEd && asSt, "both must normalize");
  // sourceEvent is telemetry-only; everything semantically meaningful matches
  assert.equal(asEd.gameId, asSt.gameId);
  assert.equal(asEd.multiplier, asSt.multiplier);
  assert.equal(asEd.crashedAt, asSt.crashedAt);
  assert.equal(asEd.hash, asSt.hash);
});

test("st arriving after ed for the same round is classified duplicate_event before compute", () => {
  _resetEdDedupForTests();
  const gameId = `canon-${Date.now()}`;
  assert.equal(classifyEdReentry(gameId), "new", "ed passes");
  recordEdRoundProcessedForTests(gameId);
  assert.equal(
    classifyEdReentry(gameId),
    "duplicate_event",
    "st for the same round must be stopped before claim/N+1 compute",
  );
  _resetEdDedupForTests();
});

test("payload without gameId normalizes to null — never enters the pipeline", () => {
  assert.equal(normalizeCrashEnd({ gameId: "", multiplier: 2 }, "ed"), null);
  assert.equal(normalizeCrashEnd({ gameId: "", multiplier: 2 }, "st"), null);
});
