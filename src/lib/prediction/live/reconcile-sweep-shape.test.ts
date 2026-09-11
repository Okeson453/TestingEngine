/**
 * Reconcile-sweep query-shape tests (audit 2026-09-11, general-pool latency).
 *
 * The unit tests in delivery-forensics.test.ts stub the SQL and ignore the
 * WHERE clause. These run against real PGlite so the predicates are actually
 * exercised:
 *   - terminal LATE rows are excluded from the re-check set (LATE can never
 *     recompute to non-LATE — including them re-fetched stable rows forever)
 *   - rows older than the recency window are skipped (no eternal head rescan)
 *   - unclassified rows inside the window are still scanned + repaired
 */
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import assert from "node:assert/strict";
import { getSql } from "@/lib/db";
import { reconcileForensicOutcomes } from "./delivery-forensics";

const PREFIX = `reconcile-test-${Date.now()}`;

async function insertDelivered(
  outcome: string | null,
  deliveredMinutesAgo: number,
): Promise<void> {
  const sql = await getSql();
  await sql`
    INSERT INTO notification_outbox (
      notification_id, type, content, metadata, target_game_id, status,
      delivery_outcome, telegram_accepted_at, delivered_at, created_at
    ) VALUES (
      ${randomUUID()}, 'prediction', ${"reconcile-shape-test"},
      ${JSON.stringify({ marker: PREFIX })}::jsonb,
      ${`${PREFIX}-target`}, 'delivered',
      ${outcome}, now() - (${deliveredMinutesAgo}::int * interval '1 minute'),
      now() - (${deliveredMinutesAgo}::int * interval '1 minute'),
      now() - (${deliveredMinutesAgo}::int * interval '1 minute')
    )
  `;
}

async function cleanup(): Promise<void> {
  const sql = await getSql();
  await sql`DELETE FROM notification_outbox WHERE metadata->>'marker' = ${PREFIX}`;
}

test("terminal LATE rows are excluded from the reconcile scan", async () => {
  await cleanup();
  try {
    await insertDelivered("LATE", 5);
    await insertDelivered("LATE", 10);
    const r = await reconcileForensicOutcomes(await getSql(), 200);
    assert.equal(r.scanned, 0, "LATE rows must not be rescanned");
    assert.equal(r.reclassified, 0);
  } finally {
    await cleanup();
  }
});

test("unclassified rows inside the window are scanned and repaired", async () => {
  await cleanup();
  try {
    await insertDelivered(null, 5);
    const r = await reconcileForensicOutcomes(await getSql(), 200);
    assert.equal(r.scanned, 1, "NULL-outcome row inside the window is scanned");
    assert.equal(r.reclassified, 1, "row repaired (UNKNOWN — no target start)");
  } finally {
    await cleanup();
  }
});

test("rows older than the recency window are skipped", async () => {
  await cleanup();
  try {
    // Default window is 2 hours; 3-hour-old unclassified row must be skipped.
    await insertDelivered(null, 180);
    const r = await reconcileForensicOutcomes(await getSql(), 200);
    assert.equal(r.scanned, 0, "row outside the window is not rescanned");
    assert.equal(r.reclassified, 0);
  } finally {
    await cleanup();
  }
});
