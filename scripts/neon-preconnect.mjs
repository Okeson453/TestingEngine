/**
 * Pure-JS Neon TLS warm-up BEFORE any --experimental-strip-types TypeScript
 * import. Root cause of boot-time pool_acquire≈1055ms with waiting=0 is not
 * pool contention — it is first-connection TLS+auth to Neon. Paying that
 * cost here (while the event loop is still free) means subsequent app-pool
 * acquires hit an already-warm path or at least overlap less with strip-types
 * CPU stalls that inflate acquire wall-clock.
 *
 * Does NOT share the app's pg.Pool; opens one disposable client and closes it.
 * DNS/TLS session may still help subsequent connections depending on host/TLS
 * stack; the primary win is attribution + overlapping network with later CPU.
 */
import pg from "pg";

export async function neonPreconnect() {
  const url = process.env.DATABASE_URL;
  if (!url) return { ok: false, reason: "no_database_url", ms: 0 };
  const t0 = Date.now();
  const pool = new pg.Pool({
    connectionString: url,
    max: 1,
    connectionTimeoutMillis: Number(process.env.PG_POOL_CONN_TIMEOUT_MS ?? 15_000) || 15_000,
    ssl:
      process.env.PG_SSL === "0"
        ? undefined
        : {
            rejectUnauthorized:
              process.env.PG_SSL_REJECT_UNAUTHORIZED === "1" ||
              process.env.PG_SSL_REJECT_UNAUTHORIZED === "true",
          },
    family: process.env.PG_FAMILY === "0" ? undefined : Number(process.env.PG_FAMILY ?? 4) || 4,
  });
  try {
    const client = await pool.connect();
    try {
      await client.query("select 1");
    } finally {
      client.release();
    }
    const ms = Date.now() - t0;
    console.log(`[worker] neon preconnect ms=${ms} (TLS+auth paid before strip-types imports)`);
    return { ok: true, reason: "ok", ms };
  } catch (e) {
    const ms = Date.now() - t0;
    console.warn(
      `[worker] neon preconnect failed ms=${ms}: ${e instanceof Error ? e.message : String(e)}`,
    );
    return { ok: false, reason: String(e?.message ?? e), ms };
  } finally {
    await pool.end().catch(() => undefined);
  }
}
