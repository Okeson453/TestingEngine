/**
 * Temporal Motif Gate — additional quality filter on ACIE ENTRY signals.
 *
 * Rationale (Report 3 validation suite):
 *   On 15,878 walk-forward rounds, ACIE alone has WR ≈ 75.50%.
 *   Motif gate (prior 6 binary outcomes match `001111` or `011011`) lifts
 *   retrospective WR to ~80.4% and frozen-OOS WR to ~79.5%.
 *
 *   0 = previous crash < 1.30x
 *   1 = previous crash >= 1.30x
 *
 *   Motif A `001111`: loss, loss, hit, hit, hit, hit
 *   Motif B `011011`: loss, hit, hit, loss, hit, hit
 *
 * Selector only — opt-in via ACIE_MOTIF_GATE=1 (default off / shadow).
 */

import type { SOLRecord } from './types.ts';

export type BinaryOutcome = 0 | 1;

export interface MotifGateResult {
  passes: boolean;
  matchedMotif: '001111' | '011011' | null;
  historyDepth: number;
}

export const APPROVED_MOTIFS = ['001111', '011011'] as const;
export type ApprovedMotif = (typeof APPROVED_MOTIFS)[number];

/**
 * Evaluate motif gate against SOL history.
 * Prior 6 outcomes are taken from indices [target-6, target-1].
 */
export function evaluateMotifGate(
  history: readonly SOLRecord[],
  target: number = history.length,
): MotifGateResult {
  const depth = target;
  if (depth < 6) {
    return { passes: false, matchedMotif: null, historyDepth: depth };
  }

  const last6: BinaryOutcome[] = [];
  for (let i = target - 6; i < target; i++) {
    last6.push(history[i].reached130 ? 1 : 0);
  }
  const motif = last6.join('');

  if (motif === '001111' || motif === '011011') {
    return {
      passes: true,
      matchedMotif: motif as ApprovedMotif,
      historyDepth: depth,
    };
  }

  return { passes: false, matchedMotif: null, historyDepth: depth };
}

export function isMotifApproved(history: readonly SOLRecord[]): boolean {
  return evaluateMotifGate(history).passes;
}
