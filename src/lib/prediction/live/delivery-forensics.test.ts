import { describe, it, expect } from "vitest";
import { classifyDelivery, reconcileForensicOutcomes } from "./delivery-forensics.ts";

describe("classifyDelivery", () => {
  it("ON_TIME when accepted before target start", () => {
    const r = classifyDelivery({
      telegramAcceptedAtMs: 1_000,
      targetStartedAtMs: 2_000,
      outboxStatus: "delivered",
    });
    expect(r.outcome).toBe("ON_TIME");
    expect(r.leadTimeMs).toBe(1000);
  });

  it("LATE when accepted after target start", () => {
    const r = classifyDelivery({
      telegramAcceptedAtMs: 3_000,
      targetStartedAtMs: 2_000,
      outboxStatus: "delivered",
    });
    expect(r.outcome).toBe("LATE");
    expect(r.leadTimeMs).toBe(-1000);
  });

  it("EARLY at the boundary (lead >= 4s) and ON_TIME just below it", () => {
    const atBoundary = classifyDelivery({
      telegramAcceptedAtMs: 1_000,
      targetStartedAtMs: 1_000 + 4_000,
      outboxStatus: "delivered",
    });
    expect(atBoundary.outcome).toBe("EARLY");
    expect(atBoundary.leadTimeMs).toBe(4_000);

    const justBelow = classifyDelivery({
      telegramAcceptedAtMs: 1_000,
      targetStartedAtMs: 1_000 + 3_999,
      outboxStatus: "delivered",
    });
    expect(justBelow.outcome).toBe("ON_TIME");
    expect(justBelow.leadTimeMs).toBe(3_999);
  });

  it("EXPIRED for dead_letter", () => {
    expect(
      classifyDelivery({
        telegramAcceptedAtMs: null,
        targetStartedAtMs: 1,
        outboxStatus: "dead_letter",
      }).outcome,
    ).toBe("EXPIRED");
  });

  it("UNKNOWN when delivered and target start not yet known (documented semantics)", () => {
    // Was ON_TIME pre-remediation: that silently converted "not yet
    // determinable" into a healthy count and stuck if BG reclassification
    // failed. UNKNOWN is truthful; the reconcile sweep upgrades it later.
    expect(
      classifyDelivery({
        telegramAcceptedAtMs: 1_000,
        targetStartedAtMs: null,
        outboxStatus: "delivered",
      }).outcome,
    ).toBe("UNKNOWN");
  });
});

describe("reconcileForensicOutcomes (durable forensic retry)", () => {
  /** Minimal tagged-template Sql stub: SELECT returns the seeded rows, the
   *  forensic UPDATE is captured for assertions. No DB required. */
  function makeFakeSql(seedRows: Array<Record<string, unknown>>) {
    const updates: Array<{ id: string; outcome: string; lead: number | null }> = [];
    const sql = ((strings: TemplateStringsArray, ...values: unknown[]) => {
      const text = strings.join("?");
      if (text.includes("FROM notification_outbox o")) {
        return Promise.resolve(seedRows);
      }
      if (text.includes("UPDATE notification_outbox")) {
        updates.push({
          outcome: String(values[0]),
          lead: (values[1] as number | null) ?? null,
          id: String(values[2]),
        });
        return Promise.resolve([]);
      }
      return Promise.resolve([]);
    }) as unknown as Parameters<typeof reconcileForensicOutcomes>[0];
    return { sql, updates };
  }

  it("repairs a NULL outcome into LATE from raw timestamps and reports the masked late", async () => {
    const { sql, updates } = makeFakeSql([
      {
        notification_id: "n-late-1",
        telegram_accepted_at: new Date(1_000),
        delivery_outcome: null, // forensic write failed — the sweep must repair it
        target_started_at: new Date(0), // accepted 1000ms AFTER start → LATE
      },
    ]);
    const r = await reconcileForensicOutcomes(sql);
    expect(r.scanned).toBe(1);
    expect(r.reclassified).toBe(1);
    expect(r.maskedLate).toBe(1);
    expect(r.mismatches).toBe(0);
    expect(updates).toEqual([
      { id: "n-late-1", outcome: "LATE", lead: -1000 },
    ]);
  });

  it("downgrades the old optimistic ON_TIME to UNKNOWN when target start is unresolvable", async () => {
    const { sql, updates } = makeFakeSql([
      {
        notification_id: "n-stale",
        telegram_accepted_at: new Date(1_000),
        delivery_outcome: "ON_TIME", // legacy optimistic classification, target never resolved
        target_started_at: null,
      },
    ]);
    const r = await reconcileForensicOutcomes(sql);
    expect(r.reclassified).toBe(1);
    expect(r.mismatches).toBe(1); // stored ON_TIME vs derived UNKNOWN
    expect(updates).toEqual([
      { id: "n-stale", outcome: "UNKNOWN", lead: null },
    ]);
  });

  it("is idempotent: a second pass over repaired rows changes nothing", async () => {
    const { sql, updates } = makeFakeSql([
      {
        notification_id: "n-ok",
        telegram_accepted_at: new Date(0),
        delivery_outcome: "LATE", // already correct
        target_started_at: new Date(0),
      },
    ]);
    const r = await reconcileForensicOutcomes(sql);
    expect(r.scanned).toBe(1);
    expect(r.reclassified).toBe(0);
    expect(r.maskedLate).toBe(0);
    expect(r.mismatches).toBe(0);
    expect(updates).toEqual([]);
  });
});
