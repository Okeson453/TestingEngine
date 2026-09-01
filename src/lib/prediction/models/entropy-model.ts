import type { IncrementalStateEngine } from '../state/incremental-state-engine.ts';
import { entropyModel as impl } from './candidate-models.ts';

export function scoreEntropy(engine: IncrementalStateEngine): number {
  return impl(engine).probability;
}
