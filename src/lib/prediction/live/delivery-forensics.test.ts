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
    // The split scans (REPAIR/AUDIT) and the batched reclassify write use
    // sql.query(text, params) — the tagged wrapper does not support nested
    // fragments. The stub routes BOTH shapes through the same matcher.
    const runQuery = (text: string, values: unknown[]) => {
      if (text.includes("FROM notification_outbox o")) {
        // pg Result is an array subclass (iterable + rowCount) — spread-consumed
        // by the sweep ([...repairRows, ...auditRows]). Route by scan semantics
        // so a row is returned by exactly one scan, like the real partial
        // indexes: REPAIR targets NULL/UNKNOWN, AUDIT targets ON_TIME/EARLY.
        const isAudit = text.includes("IN ('ON_TIME', 'EARLY')");
        const matched = seedRows.filter((r) =>
          isAudit
            ? r.delivery_outcome === "ON_TIME" || r.delivery_outcome === "EARLY"
            : r.delivery_outcome == null || r.delivery_outcome === "UNKNOWN",
        );
        return Promise.resolve(Object.assign([...matched], { rowCount: matched.length }));
      }
      if (text.includes("UPDATE notification_outbox")) {
        if (text.includes("AS v(notification_id, outcome, lead_time_ms)")) {
          // Batched reclassify: params are (id, outcome, lead) tuples.
          for (let i = 0; i < values.length; i += 3) {
            updates.push({
              id: String(values[i]),
              outcome: String(values[i + 1]),
              lead: (values[i + 2] as number | null) ?? null,
            });
          }
        } else {
          updates.push({
            outcome: String(values[0]),
            lead: (values[1] as number | null) ?? null,
            id: String(values[2]),
          });
        }
        return Promise.resolve(Object.assign([], { rowCount: 0 }));
      }
      return Promise.resolve(Object.assign([], { rowCount: 0 }));
    };
    const sql = Object.assign(
      (strings: TemplateStringsArray, ...values: unknown[]) =>
        runQuery(strings.join("?"), values),
      { query: <R>(text: string, values?: unknown[]) => runQuery(text, values ?? []) as Promise<R[] & { rowCount: number }> },
    ) as unknown as Parameters<typeof reconcileForensicOutcomes>[0];
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

  it("is idempotent: a second pass over stable rows changes nothing", async () => {
    // Split-sweep semantics (5a6ac90): REPAIR covers NULL/UNKNOWN, AUDIT
    // covers ON_TIME/EARLY — a stable row is scanned exactly once and, being
    // already correct, produces no write.
    const { sql, updates } = makeFakeSql([
      {
        notification_id: "n-ok",
        telegram_accepted_at: new Date(0),
        delivery_outcome: "ON_TIME", // already correct, AUDIT window
        target_started_at: new Date(1_000), // accepted strictly before start → ON_TIME
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
