import type { IncrementalStateEngine } from '../state/incremental-state-engine.ts';
import { autocorrelationModel as impl } from './candidate-models.ts';

export function scoreAutocorrelation(engine: IncrementalStateEngine): number {
  return impl(engine).probability;
}
