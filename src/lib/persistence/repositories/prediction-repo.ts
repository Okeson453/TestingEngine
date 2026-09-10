/**
 * PredictionRepository — decision-layer prediction bookkeeping.
 *
 * The live pipeline's durable prediction persistence lives in
 * `predictor.onGameEndPredict` (pending_predictions + notification_outbox,
 * one atomic transaction). This repository is the decision-layer record store
 * (in-memory, process-local) used by EntryDecisionService for signal
 * bookkeeping and outcome resolution — it deliberately performs no DB I/O on
 * the critical path and does not double-write the live pipeline's tables.
 */

import type { PredictionSignal } from '../../prediction/types.ts';

export interface PersistedPredictionInput {
  signal: PredictionSignal;
  sessionId?: string | null;
  roundId: string;
  externalRoundId?: string | null;
  regimeName?: string | null;
}

export interface PredictionOutcomeInput {
  predictionId: string;
  roundId?: string | null;
  /** Actual crash point of the target round, when known. */
  actualCrashPoint?: number;
  riskApproved?: boolean | null;
  riskRejectionReason?: string | null;
  betExecuted?: boolean;
  targetThreshold?: number;
}

export interface PersistedPrediction {
  predictionId: string;
  signal: PredictionSignal;
  sessionId: string | null;
  roundId: string;
  externalRoundId: string | null;
  regimeName: string | null;
  createdAt: string;
  outcome: {
    roundId: string | null;
    actualCrashPoint: number | null;
    riskApproved: boolean | null;
    riskRejectionReason: string | null;
    betExecuted: boolean;
    targetThreshold: number | null;
    resolvedAt: string;
  } | null;
}

export class PredictionRepository {
  protected readonly predictions = new Map<string, PersistedPrediction>();

  async create(input: PersistedPredictionInput): Promise<void> {
    this.predictions.set(input.signal.predictionId, {
      predictionId: input.signal.predictionId,
      signal: input.signal,
      sessionId: input.sessionId ?? null,
      roundId: input.roundId,
      externalRoundId: input.externalRoundId ?? null,
      regimeName: input.regimeName ?? null,
      createdAt: new Date().toISOString(),
      outcome: null,
    });
  }

  async resolveOutcome(input: PredictionOutcomeInput): Promise<void> {
    const existing = this.predictions.get(input.predictionId);
    if (!existing) return;
    existing.outcome = {
      roundId: input.roundId ?? existing.roundId,
      actualCrashPoint: input.actualCrashPoint ?? null,
      riskApproved: input.riskApproved ?? null,
      riskRejectionReason: input.riskRejectionReason ?? null,
      betExecuted: input.betExecuted ?? false,
      targetThreshold: input.targetThreshold ?? null,
      resolvedAt: new Date().toISOString(),
    };
  }

  async findById(id: string): Promise<PersistedPrediction | null> {
    return this.predictions.get(id) ?? null;
  }

  async listRecent(limit = 100): Promise<PersistedPrediction[]> {
    return [...this.predictions.values()]
      .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))
      .slice(0, limit);
  }
}
