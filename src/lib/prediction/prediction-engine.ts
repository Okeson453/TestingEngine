import { HistoricalRound, ThresholdTarget, PredictionSignal, FeatureVector, Regime } from './types.ts';
import { FeatureEngine } from './features/feature-engine.ts';
import { RegimeDetector } from './regimes/regime-detector.ts';
import { ModelRegistry } from './models/model-registry.ts';
import { toSignal } from './signals/signal.ts';
import { getLogger } from '../observability/logger.ts';

export interface PredictRequest {
  priorRounds: HistoricalRound[];
  targetRoundId: string;
  timestamp: string;
  target?: ThresholdTarget;
  modelName?: string;
  modelVersion?: string;
}

export class PredictionEngine {
  private readonly logger = getLogger();
  private readonly featureEngine: FeatureEngine;
  private readonly regimeDetector: RegimeDetector;
  private readonly registry: ModelRegistry;

  constructor(featureEngine?: FeatureEngine, regimeDetector?: RegimeDetector, registry?: ModelRegistry) {
    this.featureEngine = featureEngine ?? new FeatureEngine();
    this.regimeDetector = regimeDetector ?? new RegimeDetector();
    this.registry = registry ?? new ModelRegistry();
  }

  predict(req: PredictRequest): PredictionSignal {
    const target = req.target ?? 1.3;
    const features = this.featureEngine.buildVector(req.priorRounds, req.targetRoundId, req.timestamp);
    const regime = this.regimeDetector.detect(req.priorRounds, req.timestamp);
    const model = req.modelName
      ? this.registry.get(req.modelName, req.modelVersion) ?? this.registry.getDefault()
      : this.registry.getDefault();
    const output = model.predict(features, target, regime);
    const signal = toSignal(output);
    this.logger.info({
      component: 'PredictionEngine', predictionId: signal.predictionId, target: signal.target,
      probability: signal.probability, confidence: signal.confidence, model: signal.modelVersion, regime: regime.name,
    }, 'Prediction generated');
    return signal;
  }

  buildFeatures(priorRounds: HistoricalRound[], targetRoundId: string, timestamp: string): FeatureVector {
    return this.featureEngine.buildVector(priorRounds, targetRoundId, timestamp);
  }

  detectRegime(priorRounds: HistoricalRound[], at?: string): Regime {
    return this.regimeDetector.detect(priorRounds, at);
  }

  getRegistry(): ModelRegistry {
    return this.registry;
  }
}
