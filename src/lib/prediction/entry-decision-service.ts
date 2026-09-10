/**
 * EntryDecisionService — live bridge:
 *   Round context → Rolling history + ACIE continuous learning → Signal → RiskEngine
 *
 * Critical path has NO database I/O.
 * ACIE onCrash must run on every completed crash (event loop) before the next entry.
 */

import { getLogger } from '../observability/logger.ts';
import { RiskEngine } from '../betting/risk-engine.ts';
import { RiskEvaluationInput, RiskEvaluationResult } from '../betting/types.ts';
import { HistoricalDataService } from './historical-data-service.ts';
import { PredictionEngine } from './prediction-engine.ts';
import { PredictionSignal, ThresholdTarget } from './types.ts';
import { isSignalFresh } from './signals/signal.ts';
import { PredictionRepository } from '../persistence/repositories/prediction-repo.ts';
import { PredictionProvenanceRepository } from '../persistence/repositories/prediction-provenance-repo.ts';
import type { OpportunityRanker as DecisionOpportunityRanker } from '../opportunity/ranker.ts';
import { globalCalibrationState } from './calibration/calibration-state.ts';

import { RoundRepository } from '../persistence/repositories/round-repo.ts';
import { ACIEEngine } from './acie/engine.ts';
// P0 FIX: was require()-based ("lazy require to avoid circular init") but the
// standalone worker is pure ESM — require threw ReferenceError and every
// construction silently fell back to a private cold engine, diverging from
// live observation state. shared-engine has no import back into this file.
import { getSharedACIEEngine } from './acie/shared-engine.ts';
import { globalLiveDivergence } from './validation/live-divergence-monitor.ts';
import { isReadyForLiveSync as isReadyForLive } from '../observability/readiness.ts';
import { saveSnapshotToFile } from './state/state-persistence.ts';
import { loadAcieStateFromDb, scheduleAcieStateSave } from './acie/state-persistence.ts';
import { onlineMeanCalibrationError } from './acie/online-state.ts';
import type { CrashLearningResult } from './acie/engine.ts';
import type { StrategyRiskState } from './acie/types.ts';
import { randomUUID } from 'crypto';
import {
  LatencyTimer,
  entryDecisionMs,
} from '../observability/performance/latency.ts';
import { predictionHotCache } from '../observability/performance/hot-cache.ts';
import { globalIncrementalFeatures } from './features/incremental-features.ts';
import { globalIncrementalState } from './state/incremental-state-engine.ts';
import {
  runPredictionPipeline,
  feedbackPredictionPipeline,
} from './prediction-pipeline.ts';
import { globalProductionController } from './lifecycle/production-controller.ts';
import { tickLearningWithHooks } from './learning/learning-bootstrap.ts';
import { assertPredictionWarmForLive } from './prewarm.ts';
import { assertFeatureVersionMatch } from './features/feature-version-assert.ts';
import { FEATURE_VERSION_V2 } from './features/feature-meta.ts';
import { evaluateSheath, type SheathMode } from '../core/sheath-mode/index.ts';

import { PredictionStateRegistry, type PredictionStateSnapshot } from './state-snapshot.ts';
// Diagnosis fixes (prediction-stack batch): identity registry, rolling
// performance windows and drift controller.
import {
  globalPredictionRegistry,
  globalRollingPerformance,
  type FeaturePath,
} from './identity/prediction-registry.ts';

export interface EntryDecisionContext {
  roundId: string;
  externalRoundId?: string | null;
  sessionId?: string | null;
  decisionTimestamp: string;
  riskInput: RiskEvaluationInput;
  target?: ThresholdTarget;
  historyLimit?: number;
  minHistory?: number;
}

export interface EntryDecisionResult {
  signal: PredictionSignal | null;
  riskResult: RiskEvaluationResult;
  predictionPersisted: boolean;
  acie?: CrashLearningResult['evaluation'] | null;
}


/** Soft adapters for sheath API drift — never throw if methods missing. */
function sheathReport(sheath: unknown, triggers: unknown[]): void {
  const s = sheath as { reportTriggers?: (t: unknown[]) => void } | null | undefined;
  s?.reportTriggers?.(triggers);
}
function sheathState(sheath: unknown): unknown {
  const s = sheath as { getState?: () => unknown } | null | undefined;
  return typeof s?.getState === "function" ? s.getState() : undefined;
}

export class EntryDecisionService {
  private readonly logger = getLogger();
  private readonly predictionEngine: PredictionEngine;
  private readonly historicalData: HistoricalDataService;
  private readonly riskEngine: RiskEngine;
  private readonly predictionRepo: PredictionRepository;
  private readonly acie: ACIEEngine;
  /** When true, ACIE probability drives the signal (legacy model still runs for features/log). */
  private readonly preferAcie: boolean;
  private lastSignal: PredictionSignal | null = null;
  private acieSeeded = false;
  private readonly stateRegistry: PredictionStateRegistry;
  private lastStateSnapshot: PredictionStateSnapshot;
  private lastEmittedProbability: number | null = null;
  private crashCountForSnapshot = 0;
  private sheathMode: SheathMode | null = null;
  private usePipeline = true;
  private provenanceRepo: PredictionProvenanceRepository;
  private decisionRanker: DecisionOpportunityRanker | null = null;

  constructor(opts?: {
    predictionEngine?: PredictionEngine;
    historicalData?: HistoricalDataService;
    riskEngine?: RiskEngine;
    predictionRepo?: PredictionRepository;
    roundRepo?: RoundRepository;
    acie?: ACIEEngine;
    preferAcie?: boolean;
    sheathMode?: SheathMode | null;
    usePipeline?: boolean;
    provenanceRepo?: PredictionProvenanceRepository;
    decisionRanker?: DecisionOpportunityRanker | null;
  }) {
    this.predictionEngine = opts?.predictionEngine ?? new PredictionEngine();
    this.historicalData =
      opts?.historicalData ??
      new HistoricalDataService(opts?.roundRepo ?? new RoundRepository());
    this.riskEngine = opts?.riskEngine ?? new RiskEngine();
    this.predictionRepo = opts?.predictionRepo ?? new PredictionRepository();
    // P0: always prefer the process-wide shared ACIE unless an explicit
    // instance is injected (tests). Never construct a second private engine
    // that diverges from live observation state.
    if (opts?.acie) {
      this.acie = opts.acie;
    } else {
      try {
        // P0 FIX: was require() — ReferenceError under the ESM worker meant
        // this ALWAYS took the fallback branch, constructing a private cold
        // engine that diverged from live observation state.
        this.acie = getSharedACIEEngine();
      } catch {
        this.acie = new ACIEEngine();
        void loadAcieStateFromDb(this.acie).catch(() => undefined);
      }
    }
    this.preferAcie = opts?.preferAcie ?? true;
    this.sheathMode = opts?.sheathMode ?? null;
    this.usePipeline = opts?.usePipeline ?? true;
    this.provenanceRepo = opts?.provenanceRepo ?? new PredictionProvenanceRepository();
    this.decisionRanker = opts?.decisionRanker ?? null;
    this.stateRegistry = new PredictionStateRegistry();
    this.lastStateSnapshot = this.stateRegistry.snapshot();
  }

  getHistoricalDataService(): HistoricalDataService {
    return this.historicalData;
  }

  /**
   * Canonical entries-blocked gate: production controller verdict OR the
   * sheath late-rate evaluator at its halt threshold.
   */
  private entriesBlocked(): boolean {
    if (!globalProductionController.status().entriesAllowed) return true;
    if (!this.sheathMode) return false;
    return evaluateSheath(this.sheathMode).decision === 'halt';
  }

  setDecisionRanker(ranker: DecisionOpportunityRanker | null): void {
    this.decisionRanker = ranker;
  }

  setProvenanceRepo(repo: PredictionProvenanceRepository): void {
    this.provenanceRepo = repo;
  }

  getACIE(): ACIEEngine {
    return this.acie;
  }

  getLastEmittedProbability(): number | null {
    return this.lastEmittedProbability;
  }

  /** Diagnosis P1/P2: rolling windows (25/50/100/250) + drift controller state. */
  getRollingPerformance() {
    return {
      windows: globalRollingPerformance.stats(),
      drift: globalRollingPerformance.getDriftState(),
      registry: globalPredictionRegistry.stats(),
    };
  }

  getStateSnapshot(): PredictionStateSnapshot {
    return this.stateRegistry.snapshot();
  }

  publishLearningState(update: Partial<Omit<PredictionStateSnapshot, 'version' | 'publishedAt'>> = {}): PredictionStateSnapshot {
    this.lastStateSnapshot = this.stateRegistry.publish(update);
    return this.lastStateSnapshot;
  }

  /**
   * Event-loop hook: every completed crash must call this so ACIE learns continuously.
   * Safe to call multiple times for the same roundId (engine is idempotent).
   */
  observeCrash(
    roundId: string,
    crashPoint: number,
    riskState?: Partial<StrategyRiskState>
  ): CrashLearningResult {
    this.ensureAcieSeeded();
    // O(1) incremental feature update (feeds hot feature cache)
    globalIncrementalFeatures.onCrash(crashPoint);
    // P0 fix (Problem 1): feedback authority is the EXACT prediction record
    // registered for the round that just ended — never a "last emitted"
    // scalar, which can belong to the wrong round/context.
    const resolution = globalPredictionRegistry.resolve(roundId, crashPoint, 1.3);
    const fbProbability = resolution?.probability ?? this.lastEmittedProbability;
    // P1 fix (Problem 3): calibration observations are keyed by the
    // prediction's actual regime, not hardcoded 'global'.
    const fbRegime = resolution?.record.regime ?? 'global';
    const feedbackInvalid =
      resolution != null && resolution.record.temporalValidity === 'TEMPORALLY_INVALID';
    // Phase 4: feed the resolved probability into calibration (if present)
    try {
      const actual: 0 | 1 = crashPoint >= 1.3 ? 1 : 0;
      if (fbProbability != null && fbProbability > 0) {
        // P0 fix (Problem 6): temporally invalid predictions NEVER enter
        // calibration/learning/performance statistics.
        // P2 fix (Problem 2): when the drift controller has restricted or
        // frozen learning, outcomes are observed but calibration/pipeline
        // learning updates are held back.
        if (
          !feedbackInvalid &&
          globalRollingPerformance.shouldAllowLearning()
        ) {
          globalCalibrationState.observe(fbProbability, actual, fbRegime);
          feedbackPredictionPipeline(fbProbability, actual);
        }
        const div = globalLiveDivergence.observe(fbProbability, actual);
        if (div.actions.fullSheathHaltEntries) {
          this.logger.warn(
            { component: 'EntryDecisionService', level: div.level, reason: div.reason },
            'Live divergence full sheath — halt entries'
          );
          try {
            sheathReport(this.sheathMode, [
              {
                id: 'prediction_divergence',
                severity: 'critical',
                message: div.reason ?? `divergence level ${div.level}`,
                detectedAt: new Date().toISOString(),
                metadata: { level: div.level },
              },
            ]);
          } catch { /* */ }
          this.publishLearningState({
            divergenceLevel: div.level,
            divergenceReason: div.reason ?? undefined,
          } as never);
        } else if (div.actions.lockConservativeBaseline) {
          try {
            sheathReport(this.sheathMode, [
              {
                id: 'prediction_calibration_degraded',
                severity: 'high',
                message: div.reason ?? 'conservative baseline lock',
                detectedAt: new Date().toISOString(),
                metadata: { level: div.level },
              },
            ]);
          } catch { /* */ }
        }

      }
      // P2 fix (Problem 2): learning hooks run only while the drift controller
      // allows learning — outcome observation above is unconditional.
      if (globalRollingPerformance.shouldAllowLearning()) {
        tickLearningWithHooks(this.sheathMode);
      }
      const prod = globalProductionController.status();
      this.logger.debug(
        {
          component: 'EntryDecisionService',
          divergenceLevel: prod.divergence.level,
          ece: prod.divergence.eceProxy,
          reason: prod.divergence.reason,
          coldState: !globalIncrementalState.isWarm(30),
        },
        'Prediction health snapshot'
      );
    } catch { /* non-critical */ }
    // Rolling performance windows (Problem 5): observe EVERY outcome —
    // this is outcome observation, independent of any learning gate.
    try {
      if (fbProbability != null && fbProbability > 0) {
        const resolution2 = resolution;
        globalRollingPerformance.observe(
          fbProbability,
          crashPoint >= 1.3,
          resolution2?.record.confidence ?? null,
        );
      }
    } catch { /* non-critical */ }
    this.crashCountForSnapshot += 1;
    if (this.crashCountForSnapshot % 25 === 0) {
      void saveSnapshotToFile(undefined, this.acie).catch(() => undefined);
    }
    const result = this.acie.onCrash(
      {
        roundId,
        crashPoint,
        timestamp: new Date().toISOString(),
      },
      riskState,
      // Problem 2: separate outcome observation from model update.
      { learn: globalRollingPerformance.shouldAllowLearning() }
    );
    this.logger.debug(
      {
        component: 'EntryDecisionService',
        roundId,
        crashPoint,
        reached130: result.reached130,
        heavy: result.heavyValidationRan,
        action: result.evaluation.strategy.action,
        drift: globalRollingPerformance.getDriftState(),
      },
      'ACIE onCrash learning tick'
    );

    // Consecutive Loss Streak Sheath Trigger
    try {
      const cl = this.acie.getConsecutiveLosses?.() ?? 0;
      if (cl >= 4) {
        this.logger.warn(
          { component: 'EntryDecisionService', streak: cl },
          'Consecutive loss streak sheath — forcing conservative mode'
        );
        sheathReport(this.sheathMode, [
          {
            id: 'consecutive_loss_streak',
            severity: 'high',
            message: `${cl} consecutive losses — conservative lock`,
            detectedAt: new Date().toISOString(),
            metadata: { streak: cl },
          },
        ]);
      }
      // Problem 5 fix: rolling-window deterioration trigger — catches
      // W/L oscillation and slow decay that a 4-streak check misses.
      const drift = globalRollingPerformance.getDriftState();
      if (drift === 'DEGRADED' || drift === 'LEARNING_RESTRICTED' || drift === 'FROZEN') {
        const s100 = globalRollingPerformance.primaryStats();
        this.logger.warn(
          {
            component: 'EntryDecisionService',
            drift,
            winRate100: s100?.winRate,
            brier100: s100?.brier,
            n100: s100?.n,
          },
          'Rolling performance deterioration — sheath trigger'
        );
        sheathReport(this.sheathMode, [
          {
            id: 'rolling_performance_degradation',
            severity: drift === 'FROZEN' ? 'critical' : 'high',
            message: `rolling deterioration (${drift}) — winRate100=${s100?.winRate?.toFixed(3)} brier100=${s100?.brier?.toFixed(3)}`,
            detectedAt: new Date().toISOString(),
            metadata: { drift, winRate100: s100?.winRate ?? null, brier100: s100?.brier ?? null, n100: s100?.n ?? 0 },
          },
        ]);
      }
    } catch { /* non-critical */ }

    return result;
  }

  async evaluateEntry(ctx: EntryDecisionContext): Promise<EntryDecisionResult> {
    if (ctx.riskInput?.mode === 'live' && !isReadyForLive()) {
      this.logger.warn({ component: 'EntryDecisionService' }, 'Live entry blocked — prediction not ready');
      await this.riskEngine.evaluate(ctx.riskInput);
      return {
        signal: null,
        riskResult: {
          approved: false,
          reason: 'PREDICTION_NOT_READY',
          rejectionReason: 'PREDICTION_NOT_READY',
          firstFailure: 'prediction_not_ready',
        },
        predictionPersisted: false,
        acie: null,
      };
    }

    const timer = new LatencyTimer();
    const target = ctx.target ?? 1.3;
    const historyLimit = ctx.historyLimit ?? 100;
    const minHistory = ctx.minHistory ?? 20;

    // Never perform database warm-up on the latency-critical decision path.
    // Startup prewarm is mandatory; if it is unavailable, fail closed for this decision.
    if (!this.historicalData.getBuffer().isWarmed()) {
      this.logger.warn(
        { component: 'EntryDecisionService', roundId: ctx.roundId },
        'Prediction history is not warm; rejecting decision without DB I/O'
      );
      const riskResult = await this.riskEngine.evaluate({ ...ctx.riskInput, predictionSignal: undefined });
      return { signal: null, riskResult, predictionPersisted: false, acie: null };
    }
    this.ensureAcieSeeded();
    const stateSnapshot = this.stateRegistry.snapshot();
    timer.record('history');
    timer.mark('post_history');

    const prior = this.historicalData.getRecentRoundsSync(
      historyLimit,
      ctx.roundId,
      ctx.externalRoundId
    );

    let signal: PredictionSignal | null = null;
    let acieEval: CrashLearningResult['evaluation'] | null = null;
    // Diagnosis Problem 4: expose the full probability transformation chain.
    let pRawAcie: number | null = null;        // P_raw (ACIE psi estimate)
    let pCalibratedPre: number | null = null;  // P_calibrated (shrinkage, pre-pipeline)
    let pPipeline: number | null = null;       // P_pipeline (post-pipeline, pre-sheath)

    const riskPartial: Partial<StrategyRiskState> = {
      balance: ctx.riskInput.currentBalance ?? 0,
      consecutiveLosses:
        ctx.riskInput.consecutiveErrors ??
        this.acie.getConsecutiveLosses?.() ??
        0,
      dailyEntriesUsed: ctx.riskInput.dailyEntriesConfirmed,
      dailyEntriesLimit: ctx.riskInput.maxDailyEntries,
    };

    if (prior.length >= minHistory || this.acie.historySize() >= minHistory) {
      // Continuous ACIE decision for *next* opportunity (does not learn; learn on crash)
      acieEval = this.acie.evaluateNext(riskPartial);
      timer.record('prediction', 'post_history');

      // V1.1 fast path: skip legacy model on critical path when ACIE is preferred.
      let legacy: PredictionSignal | null = null;
      if (!this.preferAcie && prior.length >= minHistory) {
        try {
          legacy = this.predictionEngine.predict({
            priorRounds: prior,
            targetRoundId: ctx.roundId,
            timestamp: ctx.decisionTimestamp,
            target,
          });
        } catch (err) {
          this.logger.warn(
            { component: 'EntryDecisionService', error: String(err) },
            'Legacy prediction failed'
          );
        }
      }

      if (this.preferAcie && acieEval) {
        const p = acieEval.psi.estimatedProbability;
        const conf = Math.max(0, Math.min(1, 1 - acieEval.psi.modelUncertainty));
        const expires = new Date(new Date(ctx.decisionTimestamp).getTime() + 45_000).toISOString();
        const calibrated = globalCalibrationState.calibrateWithShrinkage(
          p,
          String(acieEval.regime ?? 'global'),
          p,
          this.acie.historySize()
        );
        // Problem 4: record the transformation chain stages
        pRawAcie = p;
        pCalibratedPre = calibrated;
        signal = {
          predictionId: randomUUID(),
          timestamp: ctx.decisionTimestamp,
          modelVersion: stateSnapshot.modelVersion,
          featureVersion: stateSnapshot.featureVersion,
          featurePath: 'ACIE_STATE',
          targetRoundId: ctx.roundId,
          target,
          score: calibrated,
          probability: calibrated,
          confidence: conf,
          regimeId: acieEval.regime,
          dataQuality: Math.min(1, this.acie.historySize() / 200),
          reasoning: Object.freeze([
            acieEval.strategy.reason,
            `evidence=${acieEval.evidence.status}`,
            `regime=${acieEval.regime}`,
          ]),
          expiresAt: expires,
          featureSummary: Object.freeze({
            stateVersion: stateSnapshot.version,
            regimeVersion: stateSnapshot.regimeVersion,
            calibrationVersion: stateSnapshot.calibrationVersion,
            psiProbability: p,
            modelUncertainty: acieEval.psi.modelUncertainty,
            dataUncertainty: acieEval.psi.dataUncertainty,
          }),
        };
        if (!acieEval.signal) {
          this.logger.info(
            {
              component: 'EntryDecisionService',
              reason: acieEval.strategy.reason,
              action: acieEval.strategy.action,
            },
            'ACIE strategy: no entry opportunity'
          );
        }

        // Phase 4–8 pipeline: calibration, meta, multi-target, opportunity, thresholds, sheath
        if (this.usePipeline && signal) {
          signal = this.applyPipelineToSignal(signal, acieEval, ctx);
        }
      } else if (legacy) {
        signal = legacy;
        if (this.usePipeline && signal) {
          signal = this.applyPipelineToSignal(signal, null, ctx);
        }
      }

      if (
        signal &&
        (!Number.isFinite(signal.probability) ||
          signal.probability < 0 ||
          signal.probability > 1 ||
          !Number.isFinite(signal.confidence) ||
          signal.confidence < 0 ||
          signal.confidence > 1)
      ) {
        this.logger.warn(
          { component: 'EntryDecisionService', predictionId: signal.predictionId },
          'Invalid signal bounds — discarding'
        );
        signal = null;
      } else if (signal && !isSignalFresh(signal, 60_000, new Date(ctx.decisionTimestamp))) {
        this.logger.warn(
          { component: 'EntryDecisionService', predictionId: signal.predictionId },
          'Signal already stale — discarding'
        );
        signal = null;
      } else if (signal) {
        this.lastSignal = signal;
        this.lastEmittedProbability = signal.probability;
        // Problem 7: feature path provenance — never silently mix V2/V1.
        const featurePath: FeaturePath = globalIncrementalState.isWarm(20)
          ? 'V2_INCREMENTAL'
          : 'V1_FALLBACK';
        // Opt-in hard gate: in live mode, a V1 fallback prediction comes from
        // a different feature distribution — block instead of silently firing.
        if (
          featurePath === 'V1_FALLBACK' &&
          process.env.PREDICT_BLOCK_V1_FALLBACK === '1' &&
          ctx.riskInput.mode === 'live'
        ) {
          this.logger.warn(
            { component: 'EntryDecisionService', predictionId: signal.predictionId },
            'V1 feature fallback in live mode — signal blocked (PREDICT_BLOCK_V1_FALLBACK)'
          );
          signal = null;
        } else {
        // Honest prediction quality labels (P1)
        ((signal as unknown) as Record<string, unknown>).modelFamily = 'acie-heuristic-ensemble';
        ((signal as unknown) as Record<string, unknown>).heuristic = true;
        ((signal as unknown) as Record<string, unknown>).trainable = false;
        ((signal as unknown) as Record<string, unknown>).modelScope = 'global';
        ((signal as unknown) as Record<string, unknown>).modelVersion = 'acie-v3';
        try {
          ((signal as unknown) as Record<string, unknown>).calibrationError = onlineMeanCalibrationError(
            this.acie.getOnlineState()
          );
          ((signal as unknown) as Record<string, unknown>).ewmaBrier = this.acie.getOnlineState().ewmaBrier;
        } catch { /* */ }
        }
        // Problem 4/8/9: register the immutable prediction record with the
        // full probability chain, provenance versions and decision stages.
        if (signal) {
          pPipeline = signal.probability; // post-pipeline, pre-sheath/risk
          const fs = signal.featureSummary as Record<string, unknown>;
          try {
            globalPredictionRegistry.register({
              predictionId: signal.predictionId,
              sourceRoundId: null,
              targetRoundId: ctx.roundId,
              createdAt: ctx.decisionTimestamp,
              targetStartedAt: null,
              targetEndedAt: null,
              rawProbability: pRawAcie ?? signal.probability,
              calibratedProbability: pCalibratedPre,
              pipelineProbability: pPipeline,
              finalProbability: signal.probability,
              confidence: signal.confidence,
              target,
              regime: signal.regimeId ?? null,
              modelVersion: signal.modelVersion,
              featureVersion: signal.featureVersion,
              featurePath,
              temporalValidity: 'TEMPORALLY_UNVERIFIED',
              provenance: {
                stateVersion: (stateSnapshot.version as string | number | null | undefined) ?? null,
                acieStateVersion: (fs?.stateVersion as string | number | undefined) ?? null,
                calibrationVersion: stateSnapshot.calibrationVersion,
                pipelineVersion: 'v1',
                regimeVersion: (stateSnapshot.regimeVersion as string | number | null | undefined) ?? null,
              },
              stages: {
                acieSignal: acieEval ? acieEval.signal != null : null,
                calibrationApplied: pCalibratedPre != null,
                pipelineApplied: this.usePipeline === true,
                opportunityScore:
                  typeof fs?.opportunityScore === 'number' ? fs.opportunityScore : null,
                riskApproved: null,
                riskRejectionReason: null,
                sheathBlocked: null,
                finalSignal: true,
              },
              resolved: false,
            });
          } catch { /* registry is best-effort */ }
        }
      }
    } else {
      this.logger.info(
        { component: 'EntryDecisionService', priorCount: prior.length, minHistory },
        'Insufficient history for prediction'
      );
    }

    // Risk remains final authority — attach prediction signal when present
    const riskInput: RiskEvaluationInput = {
      ...ctx.riskInput,
      predictionSignal: signal
        ? {
            predictionId: signal.predictionId,
            probability: signal.probability,
            confidence: signal.confidence,
            target: signal.target,
            dataQuality: signal.dataQuality,
            expiresAt: signal.expiresAt,
          }
        : ctx.riskInput.predictionSignal,
      minPredictionProbability: ctx.riskInput.minPredictionProbability,
      minPredictionConfidence: ctx.riskInput.minPredictionConfidence,
    };

    // ACIE SKIP → do not present signal as acceptable opportunity
    if (acieEval && !acieEval.signal && this.preferAcie) {
      riskInput.predictionSignal = undefined;
    }

    // Feature version consistency (Phase 2.7)
    if (signal) {
      try {
        assertFeatureVersionMatch(signal.featureVersion);
      } catch (err) {
        this.logger.warn(
          { component: 'EntryDecisionService', error: String(err), featureVersion: signal.featureVersion, engine: FEATURE_VERSION_V2 },
          'Feature version mismatch — discarding signal for live decision'
        );
        if (ctx.riskInput.mode === 'live') signal = null;
      }
    }

    // Live warm-state gate (design §21)
    if (ctx.riskInput.mode === 'live') {
      try {
        assertPredictionWarmForLive(40);
      } catch (err) {
        this.logger.warn(
          { component: 'EntryDecisionService', error: String(err) },
          'LIVE blocked — prediction stack cold'
        );
        riskInput.predictionSignal = undefined;
        signal = null;
      }
    }

    // Production / divergence sheath may block entries (design §25–26)
    {
      if (this.entriesBlocked()) {
        this.logger.info(
          {
            component: 'EntryDecisionService',
            divergenceLevel: globalProductionController.status().divergence.level,
            sheath: sheathState(this.sheathMode),
          },
          'Entries blocked by prediction sheath / divergence'
        );
        riskInput.predictionSignal = undefined;
      }
    }

    timer.mark('pre_risk');
    const riskResult = await this.riskEngine.evaluate(riskInput);
    timer.record('risk', 'pre_risk');

    // Problem 9: record the risk + sheath stage outcomes on the prediction record
    try {
      const rec = globalPredictionRegistry.getByTarget(ctx.roundId);
      if (rec && !rec.resolved) {
        rec.stages.riskApproved = riskResult.approved;
        rec.stages.riskRejectionReason = riskResult.rejectionReason ?? null;
        rec.stages.sheathBlocked = this.entriesBlocked();
      }
    } catch { /* best-effort */ }

    if (signal) {
      this.persistAsync(signal, ctx, riskResult, target);
      // Hot cache for subsequent ranking / workers within same round window
      predictionHotCache.set(ctx.roundId, {
        probability: signal.probability,
        confidence: signal.confidence,
        regimeId: signal.regimeId ?? null,
        modelVersion: signal.modelVersion,
        reasoning: signal.reasoning,
      });
    }

    const totalMs = timer.record('entry_total');
    entryDecisionMs.observe(totalMs);
    const entryP99Raw = entryDecisionMs.percentile(99);
    const entryP99 = entryP99Raw != null ? Math.round(entryP99Raw * 100) / 100 : null;

    this.logger.info(
      {
        component: 'EntryDecisionService',
        roundId: ctx.roundId,
        approved: riskResult.approved,
        probability: signal?.probability,
        model: signal?.modelVersion,
        acieAction: acieEval?.strategy.action,
        latencyMs: Math.round(totalMs * 100) / 100,
        entryP99Estimate: entryP99,
      },
      riskResult.approved ? 'Entry APPROVED' : 'Entry REJECTED'
    );

    return { signal, riskResult, predictionPersisted: false, acie: acieEval };
  }

  private ensureAcieSeeded(): void {
    if (this.acieSeeded) return;
    try {
      const recent = this.historicalData.getRecentRoundsSync(500);
      if (recent.length >= 20) {
        this.acie.seedHistory(
          recent.map((r) => ({
            roundId: r.id || r.externalRoundId || randomUUID(),
            crashPoint: r.crashPoint,
            timestamp: r.crashedAt ?? r.createdAt,
          }))
        );
        this.logger.info(
          { component: 'EntryDecisionService', seeded: recent.length },
          'ACIE seeded from rolling history'
        );
      }
      this.acieSeeded = true;
    } catch (err) {
      this.logger.warn(
        { component: 'EntryDecisionService', error: String(err) },
        'ACIE seed skipped'
      );
      this.acieSeeded = true;
    }
  }


  /**
   * Apply Phase 4–8 pipeline on top of ACIE/legacy signal:
   * ensemble + meta + calibration + multi-target + opportunity + dynamic threshold.
   */
  private applyPipelineToSignal(
    signal: PredictionSignal,
    acieEval: CrashLearningResult['evaluation'] | null,
    ctx: EntryDecisionContext
  ): PredictionSignal {
    const regime = String(acieEval?.regime ?? signal.regimeId ?? 'normal');
    const pipeline = runPredictionPipeline({
      baseProbability: signal.probability,
      regime,
      regimeConfidence: Math.min(1, this.acie.historySize() / 200),
      dataQuality: signal.dataQuality,
      bankroll: ctx.riskInput.currentBalance ?? 0,
      baseThreshold: 0.58,
      predictionId: signal.predictionId,
      featureVersion: signal.featureVersion,
      modelVersion: signal.modelVersion,
    });

    const target = pipeline.targetSelection.selected.target as ThresholdTarget;
    const expires = signal.expiresAt;
    const reasoning = Object.freeze([
      ...signal.reasoning,
      pipeline.reason,
      pipeline.targetSelection.reason,
      `threshold=${pipeline.threshold.toFixed(3)} (${pipeline.thresholdReason})`,
      `metaP=${pipeline.metaProbability.toFixed(3)}`,
      `oppScore=${pipeline.opportunity.score.toFixed(4)}`,
    ]);

    const probability = pipeline.calibratedProbability;
    const confidence = pipeline.opportunity.confidence;

    // Unify prediction opportunity with decision-layer ranker: rank the
    // pipeline's opportunity through the decision ranker when one is wired.
    try {
      this.decisionRanker?.rank(pipeline.opportunity);
    } catch {
      /* non-critical */
    }

    return {
      predictionId: pipeline.predictionId || signal.predictionId,
      timestamp: signal.timestamp,
      modelVersion: signal.modelVersion,
      featureVersion: signal.featureVersion,
      featurePath: signal.featurePath,
      targetRoundId: signal.targetRoundId,
      target,
      score: probability,
      probability,
      confidence,
      regimeId: regime,
      dataQuality: signal.dataQuality,
      reasoning,
      expiresAt: expires,
      featureSummary: Object.freeze({
        ...signal.featureSummary,
        metaProbability: pipeline.metaProbability,
        rawPipelineProbability: pipeline.rawProbability,
        opportunityScore: pipeline.opportunity.score,
        opportunityRank: pipeline.opportunity.rank,
        pipelineThreshold: pipeline.threshold,
        selectedTarget: target,
        shrunkEV: pipeline.targetSelection.selected.shrunkEV,
        divergenceLevel: pipeline.production.divergence.level,
      }),
    };
  }

  private persistAsync(
    signal: PredictionSignal,
    ctx: EntryDecisionContext,
    riskResult: RiskEvaluationResult,
    target: ThresholdTarget
  ): void {
    void (async () => {
      try {
        await this.predictionRepo.create({
          signal,
          sessionId: ctx.sessionId,
          roundId: ctx.roundId,
          externalRoundId: ctx.externalRoundId,
          regimeName: signal.regimeId,
        });
        await this.predictionRepo.resolveOutcome({
          predictionId: signal.predictionId,
          roundId: ctx.roundId,
          riskApproved: riskResult.approved,
          riskRejectionReason: riskResult.rejectionReason,
          betExecuted: false,
          targetThreshold: target,
        });

        // Full provenance (migration 025) — best-effort
        const fs = signal.featureSummary as Record<string, number>;
        const raw = Number(fs.rawPipelineProbability ?? signal.probability);
        const meta = Number(fs.metaProbability ?? signal.probability);
        const opp = Number(fs.opportunityScore ?? 0);
        try {
          await this.provenanceRepo.enrichPrediction({
            predictionId: signal.predictionId,
            calibratedProbability: signal.probability,
            rawProbability: raw,
            opportunityScore: opp,
            metaProbability: meta,
            calibrationVersion: globalCalibrationState.version,
          });
          await this.provenanceRepo.recordCalibration({
            predictionId: signal.predictionId,
            rawProbability: raw,
            calibratedProbability: signal.probability,
            calibrationVersion: globalCalibrationState.version,
            regime: signal.regimeId ?? undefined,
          });
          await this.provenanceRepo.recordOpportunity({
            opportunityId: `opp-${signal.predictionId}`,
            predictionId: signal.predictionId,
            target: signal.target,
            score: opp,
            rank: Number(fs.opportunityRank ?? 0) || undefined,
            calibratedProbability: signal.probability,
            regime: signal.regimeId ?? undefined,
          });
          await this.provenanceRepo.recordModelScores(signal.predictionId, [
            {
              modelName: 'pipeline',
              modelVersion: signal.modelVersion,
              probability: signal.probability,
              weight: 1,
            },
            {
              modelName: 'meta',
              modelVersion: 'lr-v1',
              probability: meta,
              weight: 0.5,
            },
          ]);
        } catch {
          /* provenance tables may not exist yet */
        }
      } catch (err) {
        this.logger.error(
          {
            component: 'EntryDecisionService',
            predictionId: signal.predictionId,
            error: err instanceof Error ? err.message : String(err),
          },
          'Async prediction persistence failed'
        );
      }
    })();
  }

  async resolveActualOutcome(opts: {
    predictionId: string;
    roundId?: string;
    actualCrashPoint: number;
    targetThreshold: number;
    betExecuted?: boolean;
  }): Promise<void> {
    await this.predictionRepo.resolveOutcome({
      predictionId: opts.predictionId,
      roundId: opts.roundId,
      actualCrashPoint: opts.actualCrashPoint,
      targetThreshold: opts.targetThreshold,
      betExecuted: opts.betExecuted ?? false,
    });
  }

  resolveActualOutcomeAsync(opts: {
    predictionId: string;
    roundId?: string;
    actualCrashPoint: number;
    targetThreshold: number;
    betExecuted?: boolean;
  }): void {
    void this.resolveActualOutcome(opts).catch((err) => {
      this.logger.error(
        {
          component: 'EntryDecisionService',
          predictionId: opts.predictionId,
          error: err instanceof Error ? err.message : String(err),
        },
        'Async outcome resolution failed'
      );
    });
  }

  getLastSignal(): PredictionSignal | null {
    return this.lastSignal;
  }

  getPredictionEngine(): PredictionEngine {
    return this.predictionEngine;
  }
}
