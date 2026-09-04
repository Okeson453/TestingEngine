/**
 * Process-level mutex for prediction generation.
 * Prevents dual-path races (Socket.IO bg handler + REST poll) from generating
 * concurrent predictions in the same process. Cross-process safety is still
 * provided by the DB unique index on pending target_game_id.
 */

let locked = false;
const waiters: Array<() => void> = [];

export async function withGenerationLock<T>(fn: () => Promise<T>): Promise<T> {
  if (locked) {
    await new Promise<void>((resolve) => waiters.push(resolve));
  }
  locked = true;
  try {
    return await fn();
  } finally {
    const next = waiters.shift();
    if (next) {
      next();
    } else {
      locked = false;
    }
  }
}

export function isGenerationLocked(): boolean {
  return locked;
}
