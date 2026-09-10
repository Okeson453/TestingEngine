/**
 * Authoritative process-wide ACIEEngine singleton.
 *
 * P0 fix: one shared instance for:
 *   - live crash observation
 *   - learning / online state updates
 *   - next-round evaluation
 *   - EntryDecisionService
 *   - persistence
 *   - prediction emission
 *
 * Do NOT construct ACIEEngine in secondary services. Always use
 * getSharedACIEEngine().
 */

import { ACIEEngine, type ACIEEngineOptions } from './engine.ts';
import { getLogger } from '../../observability/logger.ts';

const logger = getLogger('acie-shared');

let shared: ACIEEngine | null = null;
let instanceId: string = 'acie-uninitialized';

/**
 * Returns the sole ACIEEngine for this process.
 * First call constructs; subsequent calls return the same object.
 */
export function getSharedACIEEngine(opts?: ACIEEngineOptions): ACIEEngine {
  if (!shared) {
    shared = new ACIEEngine(opts);
    instanceId = `acie-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    logger.info(
      { component: 'acie-shared', instanceId },
      'Shared ACIEEngine constructed',
    );
  }
  return shared;
}

/** Stable id for provenance (does not change after first construction). */
export function getSharedACIEInstanceId(): string {
  if (!shared) getSharedACIEEngine();
  return instanceId;
}

/**
 * Replace the shared instance (tests / controlled re-init only).
 * Production boot should call getSharedACIEEngine() then loadAcieStateFromDb.
 */
export function setSharedACIEEngineForTests(engine: ACIEEngine | null): void {
  shared = engine;
  instanceId = engine
    ? `acie-test-${Date.now().toString(36)}`
    : 'acie-uninitialized';
}

/** True once the singleton has been constructed. */
export function isSharedACIEInitialized(): boolean {
  return shared != null;
}
