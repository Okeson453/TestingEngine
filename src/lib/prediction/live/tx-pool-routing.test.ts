/**
 * P0 regression — dual-pool routing must actually be honored by
 * runInTransaction.
 *
 * Root cause (forensic timing investigation): `runInTransaction` called
 * `getPgPool()` unconditionally on the Neon path, which always resolves to
 * the GENERAL pool (db.ts: `globalRef.__pgPool__ = generalPool`), regardless
 * of whether the caller obtained its `sql` handle via `getCriticalSql()` or
 * `getSql()`. Every transactional write in the app — prediction persist
 * (predictor.ts `onGameEndPredict`), outbox claim (notification-worker.ts
 * `tickOnce`), validator persist, crash ingest — routes through this
 * function, so the critical/general pool split (designed specifically to
 * keep the prediction → outbox → dispatch path off the pool shared with
 * dashboard/analytics/forensics traffic) never applied to a single
 * transaction. `getCriticalPool()` (db.ts) existed but was dead code —
 * confirming this was an oversight, not an intentional design.
 *
 * This test does not touch a real database: it mocks `@/lib/db` and asserts
 * `runInTransaction` acquires its client from the pool tagged on the `sql`
 * argument, never silently substituting the general pool for a critical
 * caller.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

const generalConnect = vi.fn();
const criticalConnect = vi.fn();

function makeFakeClient() {
  return {
    query: vi.fn(async (_text: string, _params?: unknown[]) => ({ rows: [] })),
    release: vi.fn(),
  };
}

vi.mock("@/lib/db", () => ({
  dbSource: "neon",
  // The general pool is what the old buggy implementation always reached
  // for via getPgPool() — kept here so a regression back to that behavior
  // is caught even though the module is mocked.
  getPgPool: () => ({ connect: generalConnect }),
  getTaggedPool: (sql: unknown) =>
    (sql as { __pgPool?: unknown }).__pgPool,
}));

// Imported AFTER the mock so tx.ts picks up the mocked db module.
import { runInTransaction } from "@/lib/prediction/live/tx";

function taggedSql(pool: { connect: typeof generalConnect }) {
  const fn = (async () => []) as unknown as { query: (...a: unknown[]) => Promise<unknown[]> } & {
    __pgPool: unknown;
  };
  fn.query = async () => [];
  fn.__pgPool = pool;
  return fn as unknown as Parameters<typeof runInTransaction>[0];
}

describe("runInTransaction pool routing (P0 regression)", () => {
  beforeEach(() => {
    generalConnect.mockReset();
    criticalConnect.mockReset();
  });

  it("pins a client from the CRITICAL pool when the caller's sql is tagged critical", async () => {
    const criticalPool = { connect: criticalConnect };
    criticalConnect.mockResolvedValue(makeFakeClient());
    generalConnect.mockResolvedValue(makeFakeClient());

    await runInTransaction(taggedSql(criticalPool), async (tx) => {
      await tx.query("select 1");
      return null;
    });

    expect(criticalConnect).toHaveBeenCalledTimes(1);
    expect(generalConnect).not.toHaveBeenCalled();
  });

  it("pins a client from the GENERAL pool when the caller's sql is tagged general", async () => {
    const generalPool = { connect: generalConnect };
    generalConnect.mockResolvedValue(makeFakeClient());

    await runInTransaction(taggedSql(generalPool), async (tx) => {
      await tx.query("select 1");
      return null;
    });

    expect(generalConnect).toHaveBeenCalledTimes(1);
    expect(criticalConnect).not.toHaveBeenCalled();
  });

  it("falls back to getPgPool() only when the sql carries no pool tag", async () => {
    generalConnect.mockResolvedValue(makeFakeClient());
    const untaggedSql = Object.assign(async () => [], {
      query: async () => [],
    }) as unknown as Parameters<typeof runInTransaction>[0];

    await runInTransaction(untaggedSql, async (tx) => {
      await tx.query("select 1");
      return null;
    });

    expect(generalConnect).toHaveBeenCalledTimes(1);
  });
});
