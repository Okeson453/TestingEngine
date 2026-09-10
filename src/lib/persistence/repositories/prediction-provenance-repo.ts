/**
 * PredictionProvenanceRepository — decision-layer provenance record store.
 *
 * Records the probability-transformation chain (raw → calibrated → pipeline),
 * calibration observations, opportunity snapshots and model scores for each
 * decision-layer prediction. Process-local in-memory store: no provenance
 * tables exist in the current schema, so nothing here claims durability.
 */

export interface EnrichPredictionInput {
  predictionId: string;
  calibratedProbability: number;
  rawProbability: number;
  opportunityScore: number;
  metaProbability: number;
  calibrationVersion: string | number;
}

export interface CalibrationObservationInput {
  predictionId: string;
  rawProbability: number;
  calibratedProbability: number;
  calibrationVersion: string | number;
  regime?: string;
}

export interface OpportunityRecordInput {
  opportunityId: string;
  predictionId: string;
  target: number;
  score: number;
  rank?: number;
  calibratedProbability: number;
  regime?: string;
}

export interface ModelScoreInput {
  modelName: string;
  modelVersion: string;
  probability: number;
  weight: number;
}

export interface PredictionProvenance {
  predictionId: string;
  enrichedAt: string;
  calibratedProbability: number;
  rawProbability: number;
  opportunityScore: number;
  metaProbability: number;
  calibrationVersion: string | number;
  calibrations: CalibrationObservationInput[];
  opportunities: OpportunityRecordInput[];
  modelScores: ModelScoreInput[];
}

export class PredictionProvenanceRepository {
  protected readonly byPredictionId = new Map<string, PredictionProvenance>();

  private entry(predictionId: string): PredictionProvenance {
    let e = this.byPredictionId.get(predictionId);
    if (!e) {
      e = {
        predictionId,
        enrichedAt: new Date().toISOString(),
        calibratedProbability: 0,
        rawProbability: 0,
        opportunityScore: 0,
        metaProbability: 0,
        calibrationVersion: '',
        calibrations: [],
        opportunities: [],
        modelScores: [],
      };
      this.byPredictionId.set(predictionId, e);
    }
    return e;
  }

  async enrichPrediction(input: EnrichPredictionInput): Promise<void> {
    const e = this.entry(input.predictionId);
    e.calibratedProbability = input.calibratedProbability;
    e.rawProbability = input.rawProbability;
    e.opportunityScore = input.opportunityScore;
    e.metaProbability = input.metaProbability;
    e.calibrationVersion = input.calibrationVersion;
    e.enrichedAt = new Date().toISOString();
  }

  async recordCalibration(input: CalibrationObservationInput): Promise<void> {
    this.entry(input.predictionId).calibrations.push({ ...input });
  }

  async recordOpportunity(input: OpportunityRecordInput): Promise<void> {
    this.entry(input.predictionId).opportunities.push({ ...input });
  }

  async recordModelScores(predictionId: string, scores: ModelScoreInput[]): Promise<void> {
    this.entry(predictionId).modelScores.push(...scores.map((s) => ({ ...s })));
  }

  async findById(predictionId: string): Promise<PredictionProvenance | null> {
    return this.byPredictionId.get(predictionId) ?? null;
  }
}
