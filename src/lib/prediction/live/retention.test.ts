/**
 * Retention sweep tests (audit 2026-09-11).
 *
 * Contract:
 *   - deletes only live_event_log rows older than the retention window
 *   - keeps rows inside the window (and never touches other tables)
 *   - respects the per-sweep batch cap (batchCapHit reported, no throw)
 *   - the batched delete runs as short bounded statements (RETURNING id)
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { getSql } from "@/lib/db";
import {
  runLiveEventLogRetentionSweep,
  resetRetentionSweepForTests,
} from "./retention";

const MARKER = `retention-test-${Date.now()}`;

async function insertEvents(ageDays: number, count: number): Promise<void> {
  const sql = await getSql();
  for (let i = 0; i < count; i += 1) {
    await sql`
      INSERT INTO live_event_log (
        correlation_id, event_kind, game_id, payload, received_at, processed_at,
        processor_latency_ms, sla_violated
      ) VALUES (
        ${`${MARKER}-${ageDays}-${i}`}, 'BG', ${`${MARKER}-game-${i}`},
        ${JSON.stringify({ marker: MARKER })},
        now() - (${ageDays}::int * interval '1 day'),
        now(), 1, false
      )
    `;
  }
}

async function countMarkerRows(ageDays: number): Promise<number> {
  const sql = await getSql();
  const rows = await sql<{ c: number }>`
    SELECT count(*)::int AS c FROM live_event_log
    WHERE game_id LIKE ${`${MARKER}-game-%`}
      AND received_at < now() - (${ageDays}::int * interval '1 day')
      AND received_at >= now() - (${ageDays + 1}::int * interval '1 day')
  `;
  return rows[0]?.c ?? 0;
}

async function cleanup(): Promise<void> {
  const sql = await getSql();
  await sql`
    DELETE FROM live_event_log WHERE game_id LIKE ${`${MARKER}-game-%`}
  `;
}

test("retention sweep deletes rows past the window and keeps recent rows", async () => {
  await cleanup();
  try {
    await insertEvents(30, 3); // old — must be deleted
    await insertEvents(0, 2); // fresh — must survive
    const before = await countMarkerRows(30);
    assert.equal(before, 3, "setup: 3 old rows present");

    const result = await runLiveEventLogRetentionSweep(await getSql(), {
      retentionDays: 14,
      batchSize: 100,
      maxBatches: 5,
    });
    assert.equal(result.table, "live_event_log");
    assert.equal(result.batchCapHit, false);
    assert.equal(await countMarkerRows(30), 0, "old rows deleted");
    const sql = await getSql();
    const fresh = await sql<{ c: number }>`
      SELECT count(*)::int AS c FROM live_event_log WHERE game_id LIKE ${`${MARKER}-game-%`}
    `;
    assert.equal(fresh[0]?.c, 2, "recent rows kept");
  } finally {
    await cleanup();
    resetRetentionSweepForTests();
  }
});

test("retention sweep respects the batch cap and reports it", async () => {
  await cleanup();
  try {
    await insertEvents(30, 5);
    const result = await runLiveEventLogRetentionSweep(await getSql(), {
      retentionDays: 14,
      batchSize: 2,
      maxBatches: 1,
    });
    assert.equal(result.batchCapHit, true, "cap reported");
    assert.equal(result.deleted, 2, "exactly one batch of 2 deleted");
  } finally {
    await cleanup();
    resetRetentionSweepForTests();
  }
});
