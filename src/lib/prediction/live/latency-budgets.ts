/**
 * Production latency budgets and classification.
 *
 * Durable handoff is dominated by Neon RTT (~180–220ms measured with
 * pool_wait_ms=0). That is not pool contention and cannot be fixed by
 * larger pools. Budgets distinguish:
 *   - neon_rtt_floor: pool_wait≈0, single-statement persist in [150, 280]
 *   - violation: pool_wait>50 OR persist>350 OR unexplained checkout
 */

export const BUDGET = {
  /** Model compute only (excludes DB). */
  predictionComputeMs: 50,
  /** PR ownership / claim. */
  prOwnershipMs: 25,
  /** Ideal durable handoff (single RTT may exceed on remote Neon). */
  durableHandoffIdealMs: 150,
  /** Acceptable Neon single-statement RTT with pool_wait≈0. */
  durableHandoffNeonFloorMs: 280,
  /** Hard fail: something other than one RTT is wrong. */
  durableHandoffHardMs: 350,
  poolWaitWarnMs: 50,
  frameToEventMs: 10,
  claimToSendMs: 100,
  sendToAcceptP95Ms: 1000,
  eventLoopLagRuntimeMs: 50,
  bgKillQueryWhenNeededMs: 300,
} as const;

export type PersistBudgetClass =
  | "ok"
  | "neon_rtt_floor"
  | "pool_contention"
  | "violation";

export function classifyPersistBudget(args: {
  persistMs: number;
  poolWaitMs: number;
  txMs: number;
}): PersistBudgetClass {
  if (args.poolWaitMs > BUDGET.poolWaitWarnMs) return "pool_contention";
  if (args.persistMs <= BUDGET.durableHandoffIdealMs) return "ok";
  if (
    args.poolWaitMs <= BUDGET.poolWaitWarnMs &&
    args.persistMs <= BUDGET.durableHandoffNeonFloorMs
  ) {
    return "neon_rtt_floor";
  }
  if (args.persistMs > BUDGET.durableHandoffHardMs) return "violation";
  return "neon_rtt_floor";
}
