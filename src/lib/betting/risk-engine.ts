/**
 * Risk gate for entry decisions.
 *
 * DEFERRED (non-goal for live prediction hardening pass):
 * Always approves. Wiring a real risk model is a product decision and is
 * intentionally not part of the P0/P1 timing/atomicity work.
 * See TestingEngine issues A6 / B1.
 *
 * The evaluate contract is ASYNC (Promise<RiskEvaluationResult>) — callers
 * must await it. The result carries the canonical RiskEvaluationResult shape
 * so gating stages (rejection reasons, first-failure attribution) typecheck
 * without casts.
 */

import type {
  RiskEvaluationInput,
  RiskEvaluationResult,
} from './types.ts';

export class RiskEngine {
  async evaluate(_input: RiskEvaluationInput): Promise<RiskEvaluationResult> {
    return { approved: true, reason: "deferred_stub_always_approve", rejectionReason: null };
  }
}
