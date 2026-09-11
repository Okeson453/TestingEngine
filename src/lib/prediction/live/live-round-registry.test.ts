import { describe, it, expect, beforeEach } from "vitest";
import {
  noteRoundStarted,
  noteRoundEnded,
  isTargetPastBettingWindow,
  getRoundPhase,
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
});
