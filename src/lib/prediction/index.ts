export * from './types.ts';
export { HistoricalDataService } from './historical-data-service.ts';
export { RollingHistoryBuffer } from './rolling-history-buffer.ts';
export { FeatureEngine, CURRENT_FEATURE_VERSION } from './features/feature-engine.ts';
export { LabelGenerator, CURRENT_TARGET_VERSION } from './labels/label-generator.ts';
export { DatasetBuilder } from './datasets/dataset-builder.ts';
export { RegimeDetector } from './regimes/regime-detector.ts';
export { BaselineStatisticalModel, type PredictiveModel } from './models/baseline-model.ts';
export { ModelRegistry } from './models/model-registry.ts';
export { PredictionEngine } from './prediction-engine.ts';
export { toSignal, isSignalExpired, isSignalFresh } from './signals/signal.ts';
export { StatisticalValidator } from './validation/statistical-validator.ts';
export { BacktestEngine } from './backtesting/backtest-engine.ts';
export { WalkForwardValidator } from './backtesting/walk-forward.ts';
export { EntryDecisionService } from './entry-decision-service.ts';
export type { EntryDecisionContext, EntryDecisionResult } from './entry-decision-service.ts';

// ACIE v3 — 1.30× threshold-probability intelligence
export * from './acie/index.ts';

// Phase 1–4 upgrades
export { IncrementalStateEngine, globalIncrementalState } from './state/incremental-state-engine.ts';
export { FeatureEngineV2, globalFeatureEngineV2 } from './features/feature-engine-v2.ts';
export { FEATURE_VERSION_V2 } from './features/feature-meta.ts';
export { CalibrationState, globalCalibrationState } from './calibration/calibration-state.ts';
export { EnsembleOrchestrator, globalEnsemble, DEFAULT_ENSEMBLE_FLAGS } from './ensemble/ensemble-orchestrator.ts';
export { prewarmPredictionStack, assertPredictionWarmForLive } from './prewarm.ts';
export { globalIncrementalFeatures } from './features/incremental-features.ts';

// Phases 4–8
export { MetaLogisticModel, globalMetaModel } from './models/meta-logistic-model.ts';
export { MultiTargetEngine, globalMultiTargetEngine, MULTI_TARGETS } from './multi-target/multi-target-engine.ts';
export { OpportunityRanker, globalOpportunityRanker } from './opportunity/opportunity-ranker.ts';
export { computeOpportunityScore } from './opportunity/opportunity-score.ts';
export { computeDynamicThreshold } from './strategy/dynamic-threshold.ts';
export { fractionalKellyStake } from './stake/kelly-sizer.ts';
export { LiveDivergenceMonitor, globalLiveDivergence } from './validation/live-divergence-monitor.ts';
export { FeatureDriftMonitor, globalFeatureDrift } from './drift/feature-drift.ts';
export { PredictionDriftMonitor, globalPredictionDrift } from './drift/prediction-drift.ts';
export { ConceptDriftMonitor, globalConceptDrift } from './drift/concept-drift.ts';
export { ModelLifecycleManager, globalModelLifecycle } from './lifecycle/model-lifecycle.ts';
export { ProductionController, globalProductionController } from './lifecycle/production-controller.ts';
export { runPredictionPipeline, feedbackPredictionPipeline } from './prediction-pipeline.ts';

export { LearningScheduler, globalLearningScheduler } from './learning/learning-scheduler.ts';
export type { PredictionGeneratedEvent } from './events/prediction-event.ts';
export { buildPredictionGeneratedEvent } from './events/prediction-event.ts';

export { runRandomnessGate, applyRandomnessGateToFlags } from './validation/randomness-gate.ts';
export { validateCalibration } from './validation/calibration-validator.ts';
export { evaluateModelGate } from './validation/model-gate.ts';
export { runValidationProtocol } from './validation/walk-forward-protocol.ts';
export { LearnedRegimeClustering, globalLearnedRegimes } from './regimes/learned-clustering.ts';
export { LookaheadEngine, globalLookaheadEngine } from './lookahead/lookahead-engine.ts';
export { OpportunityWindow, globalOpportunityWindow } from './opportunity/opportunity-window.ts';
export { runDesignAcceptance } from './validation/design-acceptance.ts';
export { tickLearningWithHooks, installLearningHooks } from './learning/learning-bootstrap.ts';
export {
  PredictionProvenanceRepository,
  InMemoryPredictionProvenanceRepository,
} from '../persistence/repositories/prediction-provenance-repo.ts';
export { runRegimeFitJob, featureRowsFromCrashPoints } from './regimes/regime-fit-job.ts';
export { computeGroupImportance, DEFAULT_FEATURE_GROUPS } from './features/feature-importance.ts';
export { fitRegimesOffline } from './workers/regime-fit-offload.ts';

export { runAcieWalkForward } from './backtesting/acie-walk-forward.ts';
export {
  snapshotPredictionStack,
  loadPredictionStackOnBoot,
  saveSnapshotToFile,
  loadSnapshotFromFile,
} from './state/state-persistence.ts';
export { BrowserWorkerHttpClient, shouldUseRemoteBrowserWorker } from '../browser/worker/http-client.ts';
export type { RiskInputProvider } from '../betting/risk-input-provider.ts';
