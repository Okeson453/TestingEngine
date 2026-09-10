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

  it("EXPIRED for dead_letter", () => {
    expect(
      classifyDelivery({
        telegramAcceptedAtMs: null,
        targetStartedAtMs: 1,
        outboxStatus: "dead_letter",
      }).outcome,
    ).toBe("EXPIRED");
  });

  it("UNKNOWN when target start missing", () => {
    expect(
      classifyDelivery({
        telegramAcceptedAtMs: 1_000,
        targetStartedAtMs: null,
        outboxStatus: "delivered",
      }).outcome,
    ).toBe("UNKNOWN");
  });
});
