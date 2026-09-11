import { describe, it, expect, beforeEach } from "vitest";
import {
  noteRoundStarted,
  noteRoundEnded,
  isTargetPastBettingWindow,
  getRoundPhase,
  getMedianBettingWindowMs,
  resetRoundRegistry,
} from "./live-round-registry";

describe("live-round-registry (zero-RTT temporal gate)", () => {
  beforeEach(() => resetRoundRegistry());

  it("unknown round is not past the betting window (fail open to DB gates)", () => {
    expect(isTargetPastBettingWindow("round-x")).toBe(false);
    expect(getRoundPhase("round-x")).toBeUndefined();
  });

  it("a real BG (round start) closes the betting window", () => {
    noteRoundStarted("r1", 1000);
    expect(isTargetPastBettingWindow("r1")).toBe(true);
    expect(getRoundPhase("r1")?.startedAt).toBe(1000);
    expect(getRoundPhase("r1")?.endedAt).toBeUndefined();
  });

  it("an ED (crash) closes the betting window", () => {
    noteRoundEnded("r2", 2000);
    expect(isTargetPastBettingWindow("r2")).toBe(true);
    expect(getRoundPhase("r2")?.endedAt).toBe(2000);
    expect(getRoundPhase("r2")?.startedAt).toBeUndefined();
  });

  it("merges started+ended phases for the same round", () => {
    noteRoundStarted("r3", 1000);
    noteRoundEnded("r3", 5000);
    const phase = getRoundPhase("r3");
    expect(phase?.startedAt).toBe(1000);
    expect(phase?.endedAt).toBe(5000);
  });

  it("prunes entries past retention", () => {
    noteRoundStarted("old", Date.now() - 11 * 60_000);
    noteRoundEnded("fresh");
    // The write that inserted 'fresh' prunes 'old'.
    expect(getRoundPhase("old")).toBeUndefined();
    expect(isTargetPastBettingWindow("fresh")).toBe(true);
  });

  it("measures the betting window from bg(N) − ed(N−1) and returns its median", () => {
    expect(getMedianBettingWindowMs()).toBe(4_000); // cold fallback
    noteRoundEnded("r10", 1_000);
    noteRoundStarted("r11", 11_000); // window 10s
    noteRoundEnded("r11", 16_000);
    noteRoundStarted("r12", 21_000); // window 5s
    noteRoundEnded("r12", 25_000);
    noteRoundStarted("r13", 31_000); // window 6s
    expect(getMedianBettingWindowMs()).toBe(6_000); // median(10s,5s,6s)
  });

  it("ignores duplicate bg events and absurd windows", () => {
    noteRoundEnded("r20", 1_000);
    noteRoundStarted("r21", 2_000); // 1s
    noteRoundStarted("r21", 2_500); // duplicate — must not sample
    noteRoundEnded("r21", 3_000);
    noteRoundStarted("r22", 3_000 + 120_000); // 120s gap — absurd, ignored
    // Only ONE valid sample: below the 3-sample median floor, so the cold
    // fallback is returned.
    expect(getMedianBettingWindowMs()).toBe(4_000);
  });
});
