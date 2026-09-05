/**
 * Opportunity ranker bridge.
 *
 * DEFERRED (non-goal for live prediction hardening pass):
 * Always returns rank 0. Real ranking is a product decision (issues A6 / B3).
 */
import type { OpportunityRanker } from "./ranker";

export function bridgeOpportunityToDecisionRanker(
  _ranker: unknown,
): OpportunityRanker {
  return { rank: () => 0 };
}
