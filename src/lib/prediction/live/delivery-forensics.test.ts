import { describe, it, expect } from "vitest";
import { classifyDelivery } from "./delivery-forensics.ts";

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

  it("ON_TIME when delivered and target start not yet known", () => {
    expect(
      classifyDelivery({
        telegramAcceptedAtMs: 1_000,
        targetStartedAtMs: null,
        outboxStatus: "delivered",
      }).outcome,
    ).toBe("ON_TIME");
  });
});
