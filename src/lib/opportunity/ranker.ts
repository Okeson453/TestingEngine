export interface OpportunityRanker {
  rank(signal: unknown): number;
}
