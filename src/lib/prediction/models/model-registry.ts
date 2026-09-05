import type { PredictiveModel } from './baseline-model.ts';
import { globalBaselineModel } from './baseline-model.ts';
import type { ModelIdentity } from '../types.ts';
import { getLogger } from '../../observability/logger.ts';

export class ModelRegistry {
  private readonly logger = getLogger();
  private readonly models = new Map<string, PredictiveModel>();
  private defaultKey: string;
  constructor() {
    // Use adaptive singleton so observeOutcome learning is shared
    this.register(globalBaselineModel);
    this.defaultKey = this.keyOf(globalBaselineModel.identity);
  }
  private keyOf(id: ModelIdentity): string { return `${id.name}@${id.version}`; }
  register(model: PredictiveModel): void {
    const key = this.keyOf(model.identity);
    this.models.set(key, model);
    this.logger.info({ component: 'ModelRegistry', model: key }, 'Model registered');
  }
  get(name: string, version?: string): PredictiveModel | undefined {
    if (version) return this.models.get(`${name}@${version}`);
    const candidates = [...this.models.entries()].filter(([k]) => k.startsWith(`${name}@`));
    return candidates.length ? candidates[candidates.length - 1][1] : undefined;
  }
  getDefault(): PredictiveModel {
    const m = this.models.get(this.defaultKey);
    if (!m) throw new Error('No default model registered');
    return m;
  }
  list(): ModelIdentity[] { return [...this.models.values()].map((m) => m.identity); }
}
