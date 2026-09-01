export interface RiskEvaluationInput {
  signal: unknown;
  stake: number;
  bankroll: number;
}
export interface RiskEvaluationResult {
  approved: boolean;
  reason: string;
  maxStake?: number;
}
