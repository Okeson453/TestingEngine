/** Temporary stub - full file in /home/workdir/artifacts/latency-pr */
export const TICK_MS = Number(process.env.OUTBOX_TICK_MS ?? 15);
export const BATCH_SIZE = Number(process.env.OUTBOX_BATCH_SIZE ?? 16);
export const STALE_INFLIGHT_MS = Number(process.env.OUTBOX_STALE_MS ?? 30_000);
export const MAX_ATTEMPTS = Number(process.env.OUTBOX_MAX_ATTEMPTS ?? 5);
export const BATCH_PARALLELISM = Number(process.env.OUTBOX_BATCH_PARALLELISM ?? 6);
export class OutboxDispatcher {
  async start(): Promise<void> {}
  async stop(): Promise<void> {}
  async tickOnce(): Promise<{ claimed: number }> { return { claimed: 0 }; }
  getStats() { return { tickCount: 0, recoveredInflight: 0, claimed: 0, delivered: 0, dead: 0, requeued: 0, lastError: null }; }
}
