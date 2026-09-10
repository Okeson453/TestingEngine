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

/** Stage names for N+1 diagnosis (must match telemetry contracts). */
export type PredictionStage =
  | 'history'
  | 'feature_generation'
  | 'regime_detection'
  | 'model_resolution'
  | 'model_prediction'
  | 'signal_conversion'
  | 'signal_validation';

function stageError(
  stage: PredictionStage,
  err: unknown,
  ctx: Record<string, unknown>,
): Error {
  const base = err instanceof Error ? err : new Error(String(err));
  const e = new Error(`[${stage}] ${base.message}`);
  e.name = base.name || 'PredictionStageError';
  (e as Error & { stage?: string; cause?: unknown }).stage = stage;
  (e as Error & { stage?: string; cause?: unknown }).cause = base;
  Object.assign(e, ctx);
  return e;
}

export class PredictionEngine {
  private readonly logger = getLogger();
  private readonly featureEngine: FeatureEngine;
  private readonly featureEngineV2: FeatureEngineV2;
  private readonly regimeDetector: RegimeDetector;
  private readonly registry: ModelRegistry;

  constructor(featureEngine?: FeatureEngine, regimeDetector?: RegimeDetector, registry?: ModelRegistry) {
    this.featureEngine = featureEngine ?? new FeatureEngine();
    // FeatureEngineV2 uses IncrementalStateEngine for O(1) snapshots
    // instead of O(n) full-history scans. Falls back to V1 only when explicitly allowed.
    this.featureEngineV2 = new FeatureEngineV2();
    this.regimeDetector = regimeDetector ?? new RegimeDetector();
    this.registry = registry ?? new ModelRegistry();
  }

  predict(req: PredictRequest): PredictionSignal {
    const target = req.target ?? 1.3;
    const targetRoundId = req.targetRoundId;
    const historySize = req.priorRounds?.length ?? 0;

    // ── stage: feature_generation ──────────────────────────────────────────
    let features: FeatureVector;
    let featurePath: 'V2_INCREMENTAL' | 'V1_FALLBACK' = 'V1_FALLBACK';
    // Production recommendation: set BLOCK_V1_LIVE_FEATURES=1 after V2 is validated.
    // Default remains permissive for cold-start / tests unless the env is set.
    const blockV1Live = process.env.BLOCK_V1_LIVE_FEATURES === '1';

    try {
      if (globalIncrementalState.isWarm(20)) {
        try {
          features = this.featureEngineV2.snapshotFromState(
            targetRoundId,
            req.timestamp,
          );
          featurePath = 'V2_INCREMENTAL';
        } catch (err) {
          this.logger.error(
            {
              component: 'PredictionEngine',
              stage: 'feature_generation',
              featurePath: 'V2_INCREMENTAL',
              targetRoundId,
              historySize,
              errorName: err instanceof Error ? err.name : 'Error',
              errorMessage: err instanceof Error ? err.message : String(err),
              errorStack: err instanceof Error ? err.stack?.slice(0, 1500) : null,
            },
            'FeatureEngineV2 failed — will not silently switch to V1 when blocked',
          );
          if (blockV1Live) {
            throw stageError('feature_generation', err, {
              featurePath: 'V2_INCREMENTAL',
              targetRoundId,
              historySize,
            });
          }
          features = this.featureEngine.buildVector(req.priorRounds, targetRoundId, req.timestamp);
          featurePath = 'V1_FALLBACK';
          this.logger.warn(
            {
              component: 'PredictionEngine',
              stage: 'feature_generation',
              featurePath,
              targetRoundId,
            },
            'Fell back to V1 full-history features after V2 exception',
          );
        }
      } else {
        // Cold start: use V1 full-history scan (explicitly labeled — never silent)
        if (blockV1Live) {
          throw stageError(
            'feature_generation',
            new Error('Incremental state cold and BLOCK_V1_LIVE_FEATURES=1'),
            { featurePath: 'cold', targetRoundId, historySize },
          );
        }
        features = this.featureEngine.buildVector(req.priorRounds, targetRoundId, req.timestamp);
        featurePath = 'V1_FALLBACK';
        this.logger.info(
          { component: 'PredictionEngine', stage: 'feature_generation', targetRoundId, historySize },
          'featurePath=V1_FALLBACK (cold incremental state)',
        );
      }
    } catch (err) {
      if ((err as { stage?: string }).stage) throw err;
      throw stageError('feature_generation', err, { targetRoundId, historySize, featurePath });
    }

    // ── stage: regime_detection ────────────────────────────────────────────
    let regime: Regime;
    try {
      regime = this.regimeDetector.detect(req.priorRounds, req.timestamp);
    } catch (err) {
      this.logger.error(
        {
          component: 'PredictionEngine',
          stage: 'regime_detection',
          targetRoundId,
          historySize,
          errorName: err instanceof Error ? err.name : 'Error',
          errorMessage: err instanceof Error ? err.message : String(err),
          errorStack: err instanceof Error ? err.stack?.slice(0, 1500) : null,
        },
        'RegimeDetector failed',
      );
      throw stageError('regime_detection', err, { targetRoundId, historySize });
    }

    // ── stage: model_resolution ────────────────────────────────────────────
    let model;
    try {
      model = req.modelName
        ? this.registry.get(req.modelName, req.modelVersion) ?? this.registry.getDefault()
        : this.registry.getDefault();
      if (!model) {
        throw new Error('ModelRegistry returned no model (corrupted registry?)');
      }
    } catch (err) {
      this.logger.error(
        {
          component: 'PredictionEngine',
          stage: 'model_resolution',
          targetRoundId,
          modelName: req.modelName ?? 'default',
          errorName: err instanceof Error ? err.name : 'Error',
          errorMessage: err instanceof Error ? err.message : String(err),
        },
        'Model resolution failed',
      );
      throw stageError('model_resolution', err, { targetRoundId, modelName: req.modelName });
    }

    // ── stage: model_prediction ────────────────────────────────────────────
    let output;
    try {
      output = model.predict(features, target, regime);
    } catch (err) {
      this.logger.error(
        {
          component: 'PredictionEngine',
          stage: 'model_prediction',
          targetRoundId,
          model: (model as { name?: string; version?: string })?.name ?? (model as { version?: string })?.version ?? 'unknown',
          featurePath,
          historySize,
          errorName: err instanceof Error ? err.name : 'Error',
          errorMessage: err instanceof Error ? err.message : String(err),
          errorStack: err instanceof Error ? err.stack?.slice(0, 1500) : null,
        },
        'Model.predict failed',
      );
      throw stageError('model_prediction', err, {
        targetRoundId,
        featurePath,
        model: (model as { name?: string })?.name ?? 'unknown',
      });
    }

    // ── stage: signal_conversion ───────────────────────────────────────────
    let signal: PredictionSignal;
    try {
      signal = toSignal(output);
      (signal as unknown as Record<string, unknown>).featurePath = featurePath;
      (signal as unknown as Record<string, unknown>).featureVersion =
        featurePath === 'V2_INCREMENTAL'
          ? (signal.featureVersion ?? 'v2-incremental')
          : (signal.featureVersion ?? 'v1-fallback');
    } catch (err) {
      this.logger.error(
        {
          component: 'PredictionEngine',
          stage: 'signal_conversion',
          targetRoundId,
          errorName: err instanceof Error ? err.name : 'Error',
          errorMessage: err instanceof Error ? err.message : String(err),
        },
        'toSignal conversion failed',
      );
      throw stageError('signal_conversion', err, { targetRoundId });
    }

    if (Math.random() < Number(process.env.PRED_LOG_SAMPLE_RATE ?? 0.05)) {
      this.logger.info({
        component: 'PredictionEngine',
        predictionId: signal.predictionId,
        target: signal.target,
        probability: signal.probability,
        confidence: signal.confidence,
        model: signal.modelVersion,
        regime: regime.name,
        featurePath,
        historySize,
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
