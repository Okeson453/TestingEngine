/**
 * Directive 17:27Z — five-state prediction taxonomy.
 *
 * Coverage (persistence) is fully separated from betting eligibility:
 * every prediction >= PREDICTION_FLOOR (0.65) is durably recorded in
 * prediction_decisions via the decision column, in one of four recorded
 * tiers; only NO_BET (below floor) is excluded by construction. 0.7692
 * (= 1/1.30) stays the mathematical break-even under the 1.30x payout
 * convention; BET_CANDIDATE (76.92%-needP) is EXPLICITLY not a bet when
 * needP is raised above break-even via MIN_SIGNAL_EDGE>0.
 * Default runtime needP is now 0.65 (absolute probability gate).
 */
import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  classifyPredictionTier,
  PREDICTION_FLOOR,
  MIN_SIGNAL_PROBABILITY,
} from "@/lib/prediction/live/predictor";

// Default absolute gate (edge=0): needP = 0.65.
const NEEDP = 0.65;
// Elevated gate for BET_CANDIDATE band coverage (fair + 0.03).
const NEEDP_ELEVATED = 1 / 1.3 + 0.03;

describe("classifyPredictionTier — band boundaries", () => {
  it("below floor is NO_BET (not persisted to band stats)", () => {
    expect(classifyPredictionTier(0.0, NEEDP)).toBe("NO_BET");
    expect(classifyPredictionTier(0.64, NEEDP)).toBe("NO_BET");
    expect(classifyPredictionTier(0.6499, NEEDP)).toBe("NO_BET");
  });

  it("default needP=0.65 makes all >=65% BET_ELIGIBLE", () => {
    expect(classifyPredictionTier(0.65, NEEDP)).toBe("BET_ELIGIBLE");
    expect(classifyPredictionTier(0.70, NEEDP)).toBe("BET_ELIGIBLE");
    expect(classifyPredictionTier(0.75, NEEDP)).toBe("BET_ELIGIBLE");
    expect(classifyPredictionTier(0.80, NEEDP)).toBe("BET_ELIGIBLE");
    expect(classifyPredictionTier(0.85, NEEDP)).toBe("BET_ELIGIBLE");
  });

  it("elevated needP preserves PREDICTION_65_PLUS / WATCH / BREAK_EVEN / BET_CANDIDATE bands", () => {
    expect(classifyPredictionTier(0.65, NEEDP_ELEVATED)).toBe("PREDICTION_65_PLUS");
    expect(classifyPredictionTier(0.6776, NEEDP_ELEVATED)).toBe("PREDICTION_65_PLUS");
    expect(classifyPredictionTier(0.70, NEEDP_ELEVATED)).toBe("WATCH");
    expect(classifyPredictionTier(0.7499, NEEDP_ELEVATED)).toBe("WATCH");
    expect(classifyPredictionTier(0.75, NEEDP_ELEVATED)).toBe("BREAK_EVEN_ZONE");
    expect(classifyPredictionTier(0.7691, NEEDP_ELEVATED)).toBe("BREAK_EVEN_ZONE");
    expect(classifyPredictionTier(1 / 1.3, NEEDP_ELEVATED)).toBe("BET_CANDIDATE");
    expect(classifyPredictionTier(0.7899, NEEDP_ELEVATED)).toBe("BET_CANDIDATE");
    expect(classifyPredictionTier(NEEDP_ELEVATED, NEEDP_ELEVATED)).toBe("BET_ELIGIBLE");
  });
});

describe("taxonomy invariants locked in source", () => {
  const predictorSrc = readFileSync(
    fileURLToPath(new URL("./predictor.ts", import.meta.url)),
    "utf8",
  );

  it("floor and default probability gate are 0.65", () => {
    expect(PREDICTION_FLOOR).toBe(0.65);
    expect(MIN_SIGNAL_PROBABILITY).toBe(0.65);
  });

  it("recordWatch counts ALL recorded rounds (p >= floor), not just WATCH band", () => {
    // Coverage counter must track persistence coverage (>= 0.65), so the
    // funnel denominator matches prediction_decisions row coverage.
    expect(predictorSrc).toContain("if (p >= PREDICTION_FLOOR) recordWatch()");
  });

  it("decision audit records the classifier tier, not a binary label", () => {
    expect(predictorSrc).toContain("decision: tier");
  });

  it("skip-log carries the full tier inline (Railway strips JSON fields)", () => {
    expect(predictorSrc).toContain("[tier=${tier}]");
  });

  it("BET_CANDIDATE is documentation-only — no path from tier to emission", () => {
    // The only emission path remains the full gate chain; the classifier is
    // a pure labeler. Assert the classifier body contains no side effects.
    const fn = predictorSrc.slice(
      predictorSrc.indexOf("export function classifyPredictionTier"),
      predictorSrc.indexOf("}", predictorSrc.indexOf("return \"PREDICTION_65_PLUS\"")),
    );
    expect(fn).not.toMatch(/await |record|emit|insert|getSql/);
  });
});
