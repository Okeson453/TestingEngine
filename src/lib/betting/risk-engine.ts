export class RiskEngine {
  async evaluate(_input: unknown): Promise<unknown> {
    return { approved: true, reason: 'stub' };
  }
}
