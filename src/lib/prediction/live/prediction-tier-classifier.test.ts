/**
 * Directive 17:27Z — five-state prediction taxonomy.
 *
 * Coverage (persistence) is fully separated from betting eligibility:
 * every prediction >= PREDICTION_FLOOR (0.65) is durably recorded in
 * prediction_decisions via the decision column, in one of four recorded
 * tiers; only NO_BET (below floor) is excluded by construction. 0.7692
 * (= 1/1.30) stays the mathematical break-even under the 1.30x payout
 * convention; BET_CANDIDATE (76.92%-needP) is EXPLICITLY not a bet —
 * the runtime edge gate (needP) is unchanged.
 */
import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  classifyPredictionTier,
  PREDICTION_FLOOR,
} from "@/lib/prediction/live/predictor";

// Runtime needP with BASE_EDGE=0.03: 0.7692 + 0.03 = 0.7992 (prod value).
const NEEDP = 1 / 1.3 + 0.03;

describe("classifyPredictionTier — band boundaries", () => {
  it("below floor is NO_BET (not persisted to band stats)", () => {
    expect(classifyPredictionTier(0.0, NEEDP)).toBe("NO_BET");
    expect(classifyPredictionTier(0.64, NEEDP)).toBe("NO_BET");
    expect(classifyPredictionTier(0.6499, NEEDP)).toBe("NO_BET");
  });

  it("65-69.99 is PREDICTION_65_PLUS", () => {
    expect(classifyPredictionTier(0.65, NEEDP)).toBe("PREDICTION_65_PLUS");
    expect(classifyPredictionTier(0.6776, NEEDP)).toBe("PREDICTION_65_PLUS");
    expect(classifyPredictionTier(0.6999, NEEDP)).toBe("PREDICTION_65_PLUS");
  });

  it("70-74.99 is WATCH", () => {
    expect(classifyPredictionTier(0.70, NEEDP)).toBe("WATCH");
    expect(classifyPredictionTier(0.7176, NEEDP)).toBe("WATCH");
    expect(classifyPredictionTier(0.7499, NEEDP)).toBe("WATCH");
  });

  it("75-76.91 is BREAK_EVEN_ZONE (just under mathematical break-even)", () => {
    expect(classifyPredictionTier(0.75, NEEDP)).toBe("BREAK_EVEN_ZONE");
    expect(classifyPredictionTier(0.7691, NEEDP)).toBe("BREAK_EVEN_ZONE");
  });

  it("76.92-needP is BET_CANDIDATE — above break-even, NOT a bet", () => {
    // 1/1.3 = 0.769230... — the mathematical break-even at 1.30x payout.
    expect(classifyPredictionTier(1 / 1.3, NEEDP)).toBe("BET_CANDIDATE");
    // The directive's discarded predictions live here:
    expect(classifyPredictionTier(0.7699, NEEDP)).toBe("BET_CANDIDATE");
    expect(classifyPredictionTier(0.7776, NEEDP)).toBe("BET_CANDIDATE");
    expect(classifyPredictionTier(0.7830, NEEDP)).toBe("BET_CANDIDATE");
    expect(classifyPredictionTier(0.7866, NEEDP)).toBe("BET_CANDIDATE");
    expect(classifyPredictionTier(0.7899, NEEDP)).toBe("BET_CANDIDATE");
  });

  it("p >= needP is BET_ELIGIBLE (full gate chain still applies)", () => {
    expect(classifyPredictionTier(NEEDP, NEEDP)).toBe("BET_ELIGIBLE");
    expect(classifyPredictionTier(0.85, NEEDP)).toBe("BET_ELIGIBLE");
  });
});

describe("taxonomy invariants locked in source", () => {
  const predictorSrc = readFileSync(
    fileURLToPath(new URL("./predictor.ts", import.meta.url)),
    "utf8",
  );

  it("floor unchanged at 0.65", () => {
    expect(PREDICTION_FLOOR).toBe(0.65);
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
