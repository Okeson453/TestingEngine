import type { OpportunityRanker } from './ranker';
export function bridgeOpportunityToDecisionRanker(_ranker: unknown): OpportunityRanker {
  return { rank: () => 0 };
}
