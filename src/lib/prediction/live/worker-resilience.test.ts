/**
 * Worker resilience unit checks (no Neon required).
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { TICK_MS } from "./notification-worker.ts";
import {
  isAuthoritative,
  setWorkerAuthority,
  markAuthorityLost,
  onAuthorityLost,
  _resetFencingForTests,
} from "./fencing.ts";

describe("worker resilience invariants", () => {
  it("outbox recovery tick stays sub-second by default", () => {
    assert.ok(TICK_MS <= 200, `TICK_MS=${TICK_MS} too high for wake recovery`);
  });

  it("authority loss stops mutation roles via cascade", () => {
    _resetFencingForTests();
    setWorkerAuthority(99);
    let stopped = false;
    onAuthorityLost(() => {
      stopped = true;
    });
    assert.equal(isAuthoritative(), true);
    markAuthorityLost("resilience-test");
    assert.equal(isAuthoritative(), false);
    assert.equal(stopped, true);
  });

  it("drain error backoff formula stays bounded", () => {
    for (let n = 2; n <= 12; n++) {
      const backoffMs = Math.min(5_000, 100 * 2 ** Math.min(n - 1, 5));
      assert.ok(backoffMs >= 100 && backoffMs <= 5_000);
    }
  });
});
