/**
 * Persistence client stub.
 *
 * The module layer (`src/lib/db.ts` → Neon pools / PGLite fallback) is the
 * canonical DB access path; there is no direct pg.Pool client at this layer
 * in the current architecture. Typed as the minimal query surface consumers
 * use, returning null so callers can fail soft or fall back.
 */
export interface DirectPoolClient {
  query<T>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

export function getPool(): DirectPoolClient | null {
  return null;
}
