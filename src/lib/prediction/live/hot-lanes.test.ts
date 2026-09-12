/**
 * Pinned hot-path lane regression tests (sep 12 latency-gate pass).
 *
 * The BG persist (predictor durable handoff) and the prediction-lane outbox
 * claim run on dedicated, permanently warm critical clients so a mid-round
 * pool rebuild (Neon TLS+auth ~1.0-1.1s — the production 1287ms outlier)
 * can never land on the hot path. These tests pin the contract:
 *   - under PGLite (no DATABASE_URL) the lanes fall back to the shared
 *     critical Sql — behaviour identical to the pool path, no divergence;
 *   - the lane Sql is cached (one wrapper per lane, not per call);
 *   - queries through the lane actually execute and persist.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  getPredictionPersistSql,
  getDispatchCriticalSql,
  getCriticalSql,
} from "@/lib/db";

test("hot lanes fall back to the critical Sql under PGLite / no-pool mode", async () => {
  const persist = await getPredictionPersistSql();
  const dispatch = await getDispatchCriticalSql();
  const critical = await getCriticalSql();
  // getPinnedLaneSql() returns getCriticalSql() when there is no pg pool.
  assert.equal(persist, critical);
  assert.equal(dispatch, critical);
});

test("hot lane wrapper is cached across calls", async () => {
  const a = await getPredictionPersistSql();
  const b = await getPredictionPersistSql();
  assert.equal(a, b);
});

test("queries execute and persist through the hot lane", async () => {
  const sql = await getPredictionPersistSql();
  const key = `hot-lane-test:${crypto.randomUUID()}`;
  await sql`
    insert into worker_state (key, value) values (${key}, 'v1')
    on conflict (key) do update set value = excluded.value, updated_at = now()
  `;
  const rows = await sql<{ value: string }>`
    select value from worker_state where key = ${key} limit 1
  `;
  assert.equal(rows[0]?.value, "v1");
});
