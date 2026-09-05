/**
 * Risk gate for entry decisions.
 *
 * DEFERRED (non-goal for live prediction hardening pass):
 * Always approves. Wiring a real risk model is a product decision and is
 * intentionally not part of the P0/P1 timing/atomicity work.
 * See TestingEngine issues A6 / B1.
 */
export class RiskEngine {
  async evaluate(_input: unknown): Promise<{ approved: boolean; reason: string }> {
    return { approved: true, reason: "deferred_stub_always_approve" };
  }
}
