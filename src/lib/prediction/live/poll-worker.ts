/**
 * Poll worker — REST safety net / recovery only.
 * PLACEHOLDER_WILL_BE_REPLACED - use artifacts if this lands
 */
export const POLL_INTERVAL_MS = Number(process.env.POLL_WORKER_MS ?? process.env.PREDICTION_POLL_MS ?? 500);
export const STALE_PREDICTED_MS = Number(process.env.STUCK_STALE_MS ?? 5 * 60 * 1_000);
export class PollWorker {
  async start(): Promise<void> {}
  async stop(): Promise<void> {}
  async tickOnce(): Promise<{ fetched: number; inserted: number; validated: number; predictionAttempts: number; missedRounds: number; stuckRecovered: number; stuckPredicted: number; error: string | null }> {
    return { fetched: 0, inserted: 0, validated: 0, predictionAttempts: 0, missedRounds: 0, stuckRecovered: 0, stuckPredicted: 0, error: 'use full file from PR artifacts' };
  }
}
