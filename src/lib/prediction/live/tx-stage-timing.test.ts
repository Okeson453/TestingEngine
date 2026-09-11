/**
 * P0 forensic-instrumentation regression.
 *
 * `runInTransaction` (tx.ts) has always computed a per-stage breakdown
 * (acquireMs/beginMs/bodyMs/commitMs) via its optional `onStage` callback,
 * but no caller in the app (predictor.ts, notification-worker.ts,
 * validator.ts) ever supplied it — every slow transaction on the live path
 * logged, at best, an undifferentiated total, with no way to tell whether
 * the delay was connection acquisition (pool/network), BEGIN/COMMIT round-
 * trip overhead, or the caller's own statement. That ambiguity is exactly
 * what makes a slow transaction get reflexively blamed on "pool contention"
 * without evidence. These tests cover:
 *   1. `logSlowTxStages` correctly identifies the dominant stage and
 *      respects its logging threshold (pure function, deterministic).
 *   2. `runInTransaction` actually invokes `onStage` with timings that
 *      correctly attribute delay to the stage it happened in, using a fake
 *      pool/client with independently controllable per-stage latency — the
 *      same mechanism a real slow acquire vs a real slow query would
 *      produce, but deterministic instead of depending on live DB timing.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

const generalConnect = vi.fn();

vi.mock("@/lib/db", () => ({
  dbSource: "neon",
  getPgPool: () => ({ connect: generalConnect }),
  getTaggedPool: (sql: unknown) => (sql as { __pgPool?: unknown }).__pgPool,
}));

import { runInTransaction, logSlowTxStages, type TxStageTimings } from "@/lib/prediction/live/tx";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** A fake pg client whose connect/BEGIN/query/COMMIT each take a controllable, independent amount of time. */
function makeFakeClient(opts: {
  connectMs?: number;
  beginMs?: number;
  bodyQueryMs?: number;
  commitMs?: number;
}) {
  const pool = {
    connect: vi.fn(async () => {
      if (opts.connectMs) await delay(opts.connectMs);
      return {
        query: vi.fn(async (text: string) => {
          if (text === "BEGIN") {
            if (opts.beginMs) await delay(opts.beginMs);
          } else if (text === "COMMIT") {
            if (opts.commitMs) await delay(opts.commitMs);
          } else {
            if (opts.bodyQueryMs) await delay(opts.bodyQueryMs);
          }
          return { rows: [] };
        }),
        release: vi.fn(),
      };
    }),
  };
  return pool;
}

function taggedSql(pool: { connect: (...a: unknown[]) => unknown }) {
  const fn = (async () => []) as unknown as { query: (...a: unknown[]) => Promise<unknown[]> } & {
    __pgPool: unknown;
  };
  fn.query = async () => [];
  fn.__pgPool = pool;
  return fn as unknown as Parameters<typeof runInTransaction>[0];
}

describe("runInTransaction onStage — distinguishes acquire vs begin vs body vs commit", () => {
  beforeEach(() => {
    generalConnect.mockReset();
  });

  it("attributes delay to ACQUIRE when connect() is slow but the transaction itself is fast", async () => {
    const pool = makeFakeClient({ connectMs: 250, bodyQueryMs: 5 });
    let stages: TxStageTimings | undefined;

    await runInTransaction(
      taggedSql(pool),
      async (tx) => {
        await tx.query("select 1");
        return null;
      },
      (t) => {
        stages = t;
      },
    );

    expect(stages).toBeDefined();
    expect(stages!.acquireMs).toBeGreaterThanOrEqual(200);
    expect(stages!.bodyMs).toBeLessThan(100);
    // The dominant stage a human would look at first must be acquire, not body.
    expect(stages!.acquireMs).toBeGreaterThan(stages!.bodyMs);
  });

  it("attributes delay to BODY when the caller's own statement is slow but connect/begin/commit are fast", async () => {
    const pool = makeFakeClient({ connectMs: 5, bodyQueryMs: 250, beginMs: 2, commitMs: 2 });
    let stages: TxStageTimings | undefined;

    await runInTransaction(
      taggedSql(pool),
      async (tx) => {
        await tx.query("insert into pending_predictions (...) values (...)");
        return null;
      },
      (t) => {
        stages = t;
      },
    );

    expect(stages!.bodyMs).toBeGreaterThanOrEqual(200);
    expect(stages!.acquireMs).toBeLessThan(100);
    expect(stages!.bodyMs).toBeGreaterThan(stages!.acquireMs);
    expect(stages!.bodyMs).toBeGreaterThan(stages!.commitMs);
  });

  it("attributes delay to COMMIT when only the commit round trip is slow", async () => {
    const pool = makeFakeClient({ connectMs: 5, bodyQueryMs: 5, commitMs: 250 });
    let stages: TxStageTimings | undefined;

    await runInTransaction(
      taggedSql(pool),
      async (tx) => {
        await tx.query("update notification_outbox set status = 'inflight'");
        return null;
      },
      (t) => {
        stages = t;
      },
    );

    expect(stages!.commitMs).toBeGreaterThanOrEqual(200);
    expect(stages!.commitMs).toBeGreaterThan(stages!.bodyMs);
    expect(stages!.commitMs).toBeGreaterThan(stages!.acquireMs);
  });
});

describe("logSlowTxStages", () => {
  it("does not log below the threshold", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const reporter = logSlowTxStages("test.fast", 300);
    reporter({ acquireMs: 10, beginMs: 5, bodyMs: 20, commitMs: 5, totalMs: 40 });
    expect(warn).not.toHaveBeenCalled();
    warn.mockRestore();
  });

  it("logs above the threshold and names the correct dominant stage", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const reporter = logSlowTxStages("test.acquireDominant", 300);
    reporter({ acquireMs: 900, beginMs: 5, bodyMs: 20, commitMs: 5, totalMs: 930 });
    expect(warn).toHaveBeenCalledTimes(1);
    const [line] = warn.mock.calls[0] as [string];
    expect(line).toContain("test.acquireDominant");
    expect(line).toContain("dominant=acquire");
    expect(line).toContain("acquire_ms=900");
    warn.mockRestore();
  });

  it("identifies body as dominant when the caller's statement is the slow part", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const reporter = logSlowTxStages("test.bodyDominant", 300);
    reporter({ acquireMs: 20, beginMs: 5, bodyMs: 850, commitMs: 15, totalMs: 890 });
    expect(warn).toHaveBeenCalledTimes(1);
    const [line] = warn.mock.calls[0] as [string];
    expect(line).toContain("dominant=body");
    warn.mockRestore();
  });
});
