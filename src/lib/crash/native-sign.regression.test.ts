/**
 * P1 — wr_utils / native-sign regression monitoring.
 *
 * Locks the failure contract of signSocketQuery (the only interface the WS
 * reconnect path depends on):
 *
 *   1. BCGAME_SOCKET_P/T env override → immediate sign, ZERO network (lets
 *      operators bypass a broken bundle fetch entirely).
 *   2. Bundle fetch failure → rejects with the structured
 *      "socket sign unavailable" error and NEVER throws synchronously /
 *      leaves a hanging inflight — callers (prewarmSign / prefetchSign)
 *      swallow it, and Fix 1's top-of-boot handlers catch anything that
 *      escapes. Sign failure must degrade the WS, never kill the worker.
 *
 * The full sandboxing of the wr_utils bundle (node:vm worker) is deliberately
 * deferred (FIX_PLAN.md Fix 2) until the bundle format is verified live.
 */
import { test } from "node:test";
import assert from "node:assert/strict";

const signModule = await import("@/lib/crash/native-sign");

test("env override returns a sign without any network activity", async () => {
  const prevP = process.env.BCGAME_SOCKET_P;
  const prevT = process.env.BCGAME_SOCKET_T;
  process.env.BCGAME_SOCKET_P = "env-p-token";
  process.env.BCGAME_SOCKET_T = "env-t-token";
  let fetched = 0;
  const realFetch = globalThis.fetch;
  globalThis.fetch = (async () => {
    fetched += 1;
    throw new Error("network must not be touched under env override");
  }) as typeof fetch;
  try {
    const sign = await signModule.signSocketQuery();
    assert.equal(sign.p, "env-p-token");
    assert.equal(sign.t, "env-t-token");
    assert.equal(fetched, 0, "env override must not fetch");
  } finally {
    globalThis.fetch = realFetch;
    if (prevP === undefined) delete process.env.BCGAME_SOCKET_P;
    else process.env.BCGAME_SOCKET_P = prevP;
    if (prevT === undefined) delete process.env.BCGAME_SOCKET_T;
    else process.env.BCGAME_SOCKET_T = prevT;
  }
});

test("bundle fetch failure rejects with structured error — no hang, no sync throw", async () => {
  const prevP = process.env.BCGAME_SOCKET_P;
  const prevT = process.env.BCGAME_SOCKET_T;
  delete process.env.BCGAME_SOCKET_P;
  delete process.env.BCGAME_SOCKET_T;
  const realFetch = globalThis.fetch;
  globalThis.fetch = (async () => {
    throw new Error("ECONNREFUSED simulated");
  }) as typeof fetch;
  try {
    await assert.rejects(
      () => signModule.signSocketQuery(),
      /socket sign unavailable/,
      "failure must surface as a structured rejection, not a crash",
    );
    // Inflight must be cleared — a second attempt gets its own rejection
    // (proves the module doesn't wedge on a dead promise).
    await assert.rejects(() => signModule.signSocketQuery(), /socket sign unavailable/);
  } finally {
    globalThis.fetch = realFetch;
    if (prevP !== undefined) process.env.BCGAME_SOCKET_P = prevP;
    if (prevT !== undefined) process.env.BCGAME_SOCKET_T = prevT;
  }
});
