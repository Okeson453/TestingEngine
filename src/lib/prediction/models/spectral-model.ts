import type { IncrementalStateEngine } from '../state/incremental-state-engine.ts';
import { spectralModel as impl } from './candidate-models.ts';

export function scoreSpectral(engine: IncrementalStateEngine): number {
  return impl(engine).probability;
}
