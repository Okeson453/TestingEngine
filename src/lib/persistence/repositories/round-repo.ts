export interface RoundRecord {
  id: string;
  externalRoundId: string;
  crashPoint: number;
  crashedAt: string;
}
export class RoundRepository {
  async findRecent(_limit: number): Promise<RoundRecord[]> { return []; }
  async findById(_id: string): Promise<RoundRecord | null> { return null; }
}
