/**
 * Stale ACIE state enforcement.
 *
 * Before emitting N+1, the prediction path must prove that ACIE observed
 * the source crash (observation count advanced for that source_game_id).
 * Silent reuse of a previous evaluation is a hard reject.
 */

import { getLogger } from '../../observability/logger.ts';

const logger = getLogger('acie-stale-guard');

/** Last source game id that advanced shared ACIE observation count. */
let lastObservedGameId: string | null = null;
let lastObservationCount = 0;

export function recordAcieObservation(gameId: string, observationCount: number): void {
  lastObservedGameId = String(gameId);
  lastObservationCount = observationCount;
}

/**
 * Seed the in-process last-observation from the persisted ACIE snapshot on
 * boot. Without this, the first BG prediction after a restart is rejected
 * with "no ACIE observation recorded in this process" (production
 * 14:45:32) even though ACIE state itself was restored — the observation
 * PROVENANCE lived only in process memory.
 */
export function seedLastAcieObservation(gameId: string, observationCount: number): void {
  if (lastObservedGameId) return; // never clobber a live observation
  lastObservedGameId = String(gameId);
  lastObservationCount = observationCount;
}

export function getLastAcieObservation(): {
  gameId: string | null;
  observationCount: number;
} {
  return { gameId: lastObservedGameId, observationCount: lastObservationCount };
}

export type StaleCheckResult =
  | { ok: true; observationCount: number; sourceGameId: string }
  | { ok: false; reason: string; expectedSource: string; actualSource: string | null; observationCount: number };

/**
 * Assert that ACIE has observed `sourceGameId` and state advanced.
 * Call immediately before persisting N+1.
 */
export function assertFreshAcieState(sourceGameId: string, minObservationCount = 1): StaleCheckResult {
  const src = String(sourceGameId);
  if (!lastObservedGameId) {
    logger.error(
      { component: 'acie-stale-guard', sourceGameId: src },
      'STALE_REJECTED: no ACIE observation recorded in this process',
    );
    return {
      ok: false,
      reason: 'no_acie_observation',
      expectedSource: src,
      actualSource: null,
      observationCount: lastObservationCount,
    };
  }
  if (lastObservedGameId !== src) {
    logger.error(
      {
        component: 'acie-stale-guard',
        expectedSource: src,
        actualSource: lastObservedGameId,
        observationCount: lastObservationCount,
      },
      'STALE_REJECTED: ACIE last observed a different source game',
    );
    return {
      ok: false,
      reason: 'source_mismatch',
      expectedSource: src,
      actualSource: lastObservedGameId,
      observationCount: lastObservationCount,
    };
  }
  if (lastObservationCount < minObservationCount) {
    logger.error(
      {
        component: 'acie-stale-guard',
        sourceGameId: src,
        observationCount: lastObservationCount,
        minObservationCount,
      },
      'STALE_REJECTED: observation count below minimum',
    );
    return {
      ok: false,
      reason: 'observation_count_low',
      expectedSource: src,
      actualSource: lastObservedGameId,
      observationCount: lastObservationCount,
    };
  }
  return { ok: true, observationCount: lastObservationCount, sourceGameId: src };
}

/** Test helper */
export function resetStaleGuardForTests(): void {
  lastObservedGameId = null;
  lastObservationCount = 0;
}
