import { describe, it, expect, beforeEach } from "vitest";
import {
  recordCandidateRound,
  recordEligibleRound,
  recordPredictionGenerated,
  recordCalibratedPrediction,
  recordGateResults,
  recordNoBet,
  recordSignalCreated,
  recordSignalPersisted,
  recordSignalDispatched,
  getFunnelSnapshot,
  logFunnelLine,
  _resetFunnelForTests,
} from "@/lib/prediction/live/funnel-metrics";

describe("signal funnel telemetry", () => {
  beforeEach(() => {
    _resetFunnelForTests();
  });

  it("counts every stage in directive order", () => {
    recordCandidateRound();
    recordCandidateRound();
    recordEligibleRound();
    recordPredictionGenerated();
    recordCalibratedPrediction();
    recordGateResults({
      edge: true,
      confidence: true,
      quality: true,
      risk: true,
      temporal: true,
    });
    recordSignalCreated();
    recordSignalPersisted();
    recordSignalDispatched();

    const s = getFunnelSnapshot();
    expect(s.candidate_rounds).toBe(2);
    expect(s.eligible_rounds).toBe(1);
    expect(s.predictions_generated).toBe(1);
    expect(s.calibrated_predictions).toBe(1);
    expect(s.edge_pass).toBe(1);
    expect(s.signals_created).toBe(1);
    expect(s.signals_persisted).toBe(1);
    expect(s.signals_dispatched).toBe(1);
    expect(s.no_bet_total).toBe(0);
  });

  it("records the terminal veto reason per NO_BET", () => {
    recordPredictionGenerated();
    recordGateResults({
      edge: false,
      confidence: true,
      quality: true,
      risk: true,
      temporal: true,
    });
    recordNoBet("edge_below_threshold");
    recordNoBet("edge_below_threshold");
    recordNoBet("strategy_veto");

    const s = getFunnelSnapshot();
    expect(s.no_bet_total).toBe(3);
    expect(s.no_bet_by_reason.edge_below_threshold).toBe(2);
    expect(s.no_bet_by_reason.strategy_veto).toBe(1);
    expect(s.edge_pass).toBe(0);
    expect(s.confidence_pass).toBe(1);
  });

  it("funnel is monotone: signals ≤ persisted ≤ created ≤ generated ≤ candidate", () => {
    for (let i = 0; i < 10; i += 1) {
      recordCandidateRound();
      if (i % 2 === 0) recordPredictionGenerated();
      if (i % 4 === 0) {
        recordSignalCreated();
        recordSignalPersisted();
        recordSignalDispatched();
      }
    }
    const s = getFunnelSnapshot();
    expect(s.signals_dispatched).toBeLessThanOrEqual(s.signals_persisted);
    expect(s.signals_persisted).toBeLessThanOrEqual(s.signals_created);
    expect(s.signals_created).toBeLessThanOrEqual(s.predictions_generated);
    expect(s.predictions_generated).toBeLessThanOrEqual(s.candidate_rounds);
  });

  it("plain-text log line carries counters (Railway strips JSON)", () => {
    recordCandidateRound();
    recordNoBet("edge_below_threshold");
    const logged: string[] = [];
    const orig = console.log;
    console.log = (m: string) => logged.push(m);
    try {
      logFunnelLine("test");
    } finally {
      console.log = orig;
    }
    expect(logged[0]).toContain("[funnel]");
    expect(logged[0]).toContain("candidate_rounds=1");
    expect(logged[0]).toContain("edge_below_threshold=1");
  });
});
