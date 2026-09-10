/**
 * HistoricalDataService mapping + repository contract tests (node:test).
 * Pure unit — no DB. Verifies the canonical RoundRecord → HistoricalRound
 * mapping and the decision-layer repository contracts.
 */
import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { HistoricalDataService } from './historical-data-service.ts';
import type { RoundRecord } from '../persistence/repositories/round-repo.ts';
import { PredictionRepository } from '../persistence/repositories/prediction-repo.ts';
import { PredictionProvenanceRepository } from '../persistence/repositories/prediction-provenance-repo.ts';
import { RiskEngine } from '../betting/risk-engine.ts';
import type { RiskEvaluationInput } from '../betting/types.ts';
import type { PredictionSignal } from './types.ts';

function rec(over: Partial<RoundRecord> = {}): RoundRecord {
  return {
    id: '1001',
    externalRoundId: '1001',
    crashPoint: 2.5,
    crashedAt: '2026-01-01T00:00:04.000Z',
    startedAt: '2026-01-01T00:00:01.000Z',
    createdAt: '2026-01-01T00:00:05.000Z',
    ...over,
  };
}

describe('HistoricalDataService.toHistorical', () => {
  const svc = new HistoricalDataService();

  test('maps the canonical RoundRecord fields', () => {
    const h = svc.toHistorical(rec());
    assert.ok(h);
    assert.equal(h.id, '1001');
    assert.equal(h.externalRoundId, '1001');
    assert.equal(h.crashPoint, 2.5);
    assert.equal(h.startedAt, '2026-01-01T00:00:01.000Z');
    assert.equal(h.crashedAt, '2026-01-01T00:00:04.000Z');
    assert.equal(h.createdAt, '2026-01-01T00:00:05.000Z');
  });

  test('dimensions absent from the durable schema stay null (never fabricated)', () => {
    const h = svc.toHistorical(rec());
    assert.ok(h);
    assert.equal(h.sessionId, null);
    assert.equal(h.observationSource, null);
    assert.equal(h.dataQuality, null);
  });

  test('sequenceIndex passes through', () => {
    assert.equal(svc.toHistorical(rec(), 7)?.sequenceIndex, 7);
  });

  test('rejects invalid crash points', () => {
    assert.equal(svc.toHistorical(rec({ crashPoint: 0 })), null);
    assert.equal(svc.toHistorical(rec({ crashPoint: -1.2 })), null);
    assert.equal(svc.toHistorical(rec({ crashPoint: Number.NaN })), null);
  });

  test('null startedAt survives (ED-before-BG rounds)', () => {
    const h = svc.toHistorical(rec({ startedAt: null }));
    assert.ok(h);
    assert.equal(h.startedAt, null);
  });
});

function makeSignal(id = 'p1'): PredictionSignal {
  return {
    predictionId: id,
    timestamp: new Date().toISOString(),
    modelVersion: 'acie-v3',
    featureVersion: 'fv',
    featurePath: 'ACIE_STATE',
    targetRoundId: '2002',
    target: 1.3,
    score: 0.7,
    probability: 0.7,
    confidence: 0.8,
    regimeId: 'global',
    dataQuality: 1,
    reasoning: Object.freeze(['test']),
    expiresAt: new Date(Date.now() + 45_000).toISOString(),
    featureSummary: Object.freeze({}),
  } as PredictionSignal;
}

describe('PredictionRepository (decision layer)', () => {
  test('create → findById → resolveOutcome lifecycle', async () => {
    const repo = new PredictionRepository();
    const sig = makeSignal();
    await repo.create({
      signal: sig,
      roundId: '1001',
      externalRoundId: '1001',
      sessionId: 's1',
      regimeName: 'global',
    });
    const found = await repo.findById(sig.predictionId);
    assert.ok(found);
    assert.equal(found.outcome, null);
    assert.equal(found.roundId, '1001');

    await repo.resolveOutcome({
      predictionId: sig.predictionId,
      roundId: '1001',
      actualCrashPoint: 2.0,
      riskApproved: true,
      betExecuted: false,
      targetThreshold: 1.3,
    });
    const resolved = await repo.findById(sig.predictionId);
    assert.ok(resolved?.outcome);
    assert.equal(resolved.outcome.actualCrashPoint, 2.0);
    assert.equal(resolved.outcome.riskApproved, true);
    assert.ok(resolved.outcome.resolvedAt);
  });

  test('resolveOutcome for an unknown prediction is a no-op, not a throw', async () => {
    const repo = new PredictionRepository();
    await repo.resolveOutcome({ predictionId: 'nope', roundId: 'r' });
    assert.equal(await repo.findById('nope'), null);
  });
});

describe('PredictionProvenanceRepository (decision layer)', () => {
  test('enrich + calibrations + opportunities + model scores accumulate', async () => {
    const repo = new PredictionProvenanceRepository();
    await repo.enrichPrediction({
      predictionId: 'p1',
      calibratedProbability: 0.7,
      rawProbability: 0.65,
      opportunityScore: 0.5,
      metaProbability: 0.68,
      calibrationVersion: 'cal-v1',
    });
    await repo.recordCalibration({
      predictionId: 'p1',
      rawProbability: 0.65,
      calibratedProbability: 0.7,
      calibrationVersion: 'cal-v1',
      regime: 'global',
    });
    await repo.recordOpportunity({
      opportunityId: 'opp-p1',
      predictionId: 'p1',
      target: 1.3,
      score: 0.5,
      calibratedProbability: 0.7,
    });
    await repo.recordModelScores('p1', [
      { modelName: 'pipeline', modelVersion: 'acie-v3', probability: 0.7, weight: 1 },
    ]);
    const p = await repo.findById('p1');
    assert.ok(p);
    assert.equal(p.rawProbability, 0.65);
    assert.equal(p.calibrations.length, 1);
    assert.equal(p.opportunities.length, 1);
    assert.equal(p.modelScores.length, 1);
  });
});

describe('RiskEngine async contract', () => {
  test('evaluate is awaitable and returns the canonical result shape', async () => {
    const engine = new RiskEngine();
    const input: RiskEvaluationInput = {
      signal: null,
      stake: 10,
      bankroll: 1000,
      mode: 'paper',
      currentBalance: 1000,
      consecutiveErrors: 0,
      dailyEntriesConfirmed: 0,
      maxDailyEntries: 10,
      minPredictionProbability: 0.58,
      minPredictionConfidence: 0.5,
      predictionSignal: undefined,
    };
    const result = await engine.evaluate(input);
    assert.equal(typeof result.approved, 'boolean');
    assert.equal(typeof result.reason, 'string');
    assert.equal(result.approved, true);
  });
});
