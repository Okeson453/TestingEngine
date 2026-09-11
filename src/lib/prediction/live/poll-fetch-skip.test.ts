/**
 * Healthy-WS poll fetch-skip tests (audit 2026-09-11, bandwidth).
 *
 * Contract:
 *   - WS healthy + ED fresh → tick does NOT call the REST fetch (redundant
 *     page), reports fetched=0, and does not count as failure.
 *   - WS degraded/dead → fetch runs (recovery behavior untouched).
 *   - POLL_SKIP_FETCH_WHEN_WS_HEALTHY=0 disables the skip entirely.
 *   - After MAX_CONSECUTIVE_SKIPS consecutive skips, the next decision
 *     forces a fetch (REST parity verification).
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { PollWorker } from "./poll-worker";
import { setWorkerAuthority } from "./fencing";

function freshWorker(opts: {
  health: "healthy" | "degraded" | "dead";
  edAgeMs: number | null;
}): { worker: PollWorker; fetchCount: () => number } {
  const worker = new PollWorker();
  worker.wsHealthFn = () => opts.health;
  worker.lastEdAgeMsFn = () => opts.edAgeMs;
  let calls = 0;
  (worker as unknown as { fetchImpl: (pages: number) => Promise<never[]> }).fetchImpl =
    async () => {
      calls += 1;
      return [];
    };
  return { worker, fetchCount: () => calls };
}

test("healthy WS + fresh ED skips the REST fetch", async () => {
  setWorkerAuthority(7);
  const { worker, fetchCount } = freshWorker({ health: "healthy", edAgeMs: 2_000 });
  const r = await worker.tickOnce();
  assert.equal(r.fetched, 0, "no rounds fetched on skipped tick");
  assert.equal(r.error, null, "skip is not an error");
  assert.equal(fetchCount(), 0, "fetchImpl never called");
});

test("degraded WS never skips — recovery fetch runs", async () => {
  setWorkerAuthority(7);
  const { worker, fetchCount } = freshWorker({ health: "degraded", edAgeMs: 2_000 });
  await worker.tickOnce();
  assert.equal(fetchCount(), 1, "fetch called exactly once");
});

test("kill switch POLL_SKIP_FETCH_WHEN_WS_HEALTHY=0 restores always-fetch", async () => {
  setWorkerAuthority(7);
  const prev = process.env.POLL_SKIP_FETCH_WHEN_WS_HEALTHY;
  process.env.POLL_SKIP_FETCH_WHEN_WS_HEALTHY = "0";
  try {
    const { worker, fetchCount } = freshWorker({ health: "healthy", edAgeMs: 1_000 });
    await worker.tickOnce();
    assert.equal(fetchCount(), 1, "fetch ran despite healthy WS");
  } finally {
    if (prev === undefined) delete process.env.POLL_SKIP_FETCH_WHEN_WS_HEALTHY;
    else process.env.POLL_SKIP_FETCH_WHEN_WS_HEALTHY = prev;
  }
});

test("skip cap: after MAX_CONSECUTIVE_SKIPS the decision forces a fetch", async () => {
  setWorkerAuthority(7);
  const { worker } = freshWorker({ health: "healthy", edAgeMs: 1_000 });
  const decide =
    (): boolean => (worker as unknown as { shouldSkipFetchForHealthyWs: () => boolean }).shouldSkipFetchForHealthyWs();
  const cap =
    (PollWorker as unknown as { MAX_CONSECUTIVE_SKIPS: number }).MAX_CONSECUTIVE_SKIPS;
  for (let i = 0; i < cap; i += 1) {
    assert.equal(decide(), true, `skip ${i + 1} allowed`);
  }
  assert.equal(decide(), false, "cap forces a verification fetch");
  // After the forced fetch the cycle restarts.
  assert.equal(decide(), true, "skip cycle restarts after verification");
});

test("stale ED (age over window) never skips even when status reads healthy", async () => {
  const { worker, fetchCount } = freshWorker({ health: "healthy", edAgeMs: 20_000 });
  const decide =
    (): boolean => (worker as unknown as { shouldSkipFetchForHealthyWs: () => boolean }).shouldSkipFetchForHealthyWs();
  assert.equal(decide(), false, "stale ED forces fetch");
  assert.equal(fetchCount(), 0, "decision alone does not fetch");
});
