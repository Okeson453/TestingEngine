import type { IncrementalStateEngine } from '../state/incremental-state-engine.ts';
import { markovChainModel } from './candidate-models.ts';

export function scoreMarkov(engine: IncrementalStateEngine): number {
  return markovChainModel(engine).probability;
}
