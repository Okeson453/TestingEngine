export class PredictionProvenanceRepository {
  async findById(_id: string): Promise<unknown | null> { return null; }
  async save(_record: unknown): Promise<void> { return; }
}
export class InMemoryPredictionProvenanceRepository extends PredictionProvenanceRepository {}
