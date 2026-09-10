import type { HistoricalRound, ThresholdTarget, PredictionSignal, FeatureVector, Regime } from './types.ts';
import { FeatureEngine } from './features/feature-engine.ts';
import { FeatureEngineV2 } from './features/feature-engine-v2.ts';
import { globalIncrementalState } from './state/incremental-state-engine.ts';
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
  private readonly featureEngineV2: FeatureEngineV2;
  private readonly regimeDetector: RegimeDetector;
  private readonly registry: ModelRegistry;

  constructor(featureEngine?: FeatureEngine, regimeDetector?: RegimeDetector, registry?: ModelRegistry) {
    this.featureEngine = featureEngine ?? new FeatureEngine();
    // P2.1: FeatureEngineV2 uses IncrementalStateEngine for O(1) snapshots
    // instead of O(n) full-history scans. Falls back to V1 on cold start.
    this.featureEngineV2 = new FeatureEngineV2();
    this.regimeDetector = regimeDetector ?? new RegimeDetector();
    this.registry = registry ?? new ModelRegistry();
  }

  
  predict(req: PredictRequest): PredictionSignal {
    const target = req.target ?? 1.3;

    // PHASE 3: Remove silent V1 fallback from live prediction path
    // V2 is now the only supported feature engine for live predictions.
    // Any V2 failure will bubble up as an explicit error rather than silently degrading.
    let features: FeatureVector;
    let featurePath: 'V2_INCREMENTAL' = 'V2_INCREMENTAL';
    
    // Always require warm incremental state for live predictions
    if (!globalIncrementalState.isWarm(20)) {
      throw new Error('Incremental state not warm - V2 features require at least 20 observations');
    }
    
    try {
      features = this.featureEngineV2.snapshotFromState(
        req.targetRoundId,
        req.timestamp,
      );
    } catch (err) {
      this.logger.error(
        { component: 'PredictionEngine', error: String(err), targetRoundId: req.targetRoundId },
        'V2 feature computation failed — no V1 fallback available in live mode',
      );
      throw new Error('V2 feature computation failed: ' + String(err));
    }

    const regime = this.regimeDetector.detect(req.priorRounds, req.timestamp);
    const model = req.modelName
      ? this.registry.get(req.modelName, req.modelVersion) ?? this.registry.getDefault()
      : this.registry.getDefault();
    const output = model.predict(features, target, regime);
    const signal = toSignal(output);
    (signal as unknown as Record<string, unknown>).featurePath = featurePath;
    (signal as unknown as Record<string, unknown>).featureVersion = signal.featureVersion ?? 'v2-incremental';

    if (Math.random() < Number(process.env.PRED_LOG_SAMPLE_RATE ?? 0.05)) {
      this.logger.info({
        component: 'PredictionEngine', predictionId: signal.predictionId, target: signal.target,
        probability: signal.probability, confidence: signal.confidence, model: signal.modelVersion,
        regime: regime.name, featurePath,
      }, 'Prediction generated');
    }
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
