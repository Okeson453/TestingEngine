/**
 * P1 exactly-once delivery per target (sep 12 pass 4, migration 0045).
 *
 * Prod 10:55Z window: ED logged "duplicate pending", BG subsequently
 * enqueued, and TWO OUTBOX_DISPATCH deliveries landed 1.5s apart — raw logs
 * could not decide duplicate-vs-distinct. These tests pin the new structural
 * guarantee: at most ONE undelivered (pending/inflight) prediction outbox
 * row may exist per target_game_id, enforced by a partial unique index the
 * persist CTE handles with ON CONFLICT DO NOTHING (caller reads it as
 * kind=duplicate — the older row delivers, never both).
 *
 * DB-backed (real PGLite, migrations applied by scripts/migrate-pglite-local.mjs).
 * Run via the bun/node --test live suite.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { getSql } from "@/lib/db";
import { PREDICTION_PARALLELISM } from "@/lib/prediction/live/notification-worker";

const MIGRATION = readFileSync(
  join(__dirname, "../../../../migrations/0045_outbox_exactly_once_per_target.sql"),
  "utf8",
);
const predictorSrc = readFileSync(join(__dirname, "predictor.ts"), "utf8");

test("migration 0045: partial unique index on (type, target_game_id) where undelivered", () => {
  assert.match(
    MIGRATION,
    /create unique index.*notification_outbox_type_target_undelivered_uidx/i,
  );
  assert.match(MIGRATION, /where status in \('pending', 'inflight'\)/i);
  // Pre-clean must exist so historical duplicate rows cannot fail the CREATE.
  assert.match(MIGRATION, /superseded_duplicate/i);
});

test("persist CTE tolerates an undelivered-row conflict instead of throwing", () => {
  assert.match(
    predictorSrc,
    /on conflict \(type, target_game_id\) where status in \('pending', 'inflight'\) do nothing/,
  );
});

test("prediction lane parallelism is 2 (fresh signal must not queue behind a survivor send)", () => {
  assert.equal(PREDICTION_PARALLELISM, 2);
});

test("behavioral: second pending outbox insert for the same target conflicts, no throw", async () => {
  const sql = await getSql();
  const target = `eonc-${randomUUID()}`;
  const insert = (notificationId: string) => sql`
    insert into notification_outbox
      (notification_id, type, content, metadata, status, priority, target_game_id)
    values
      (${notificationId}::uuid, 'prediction', 't', '{}', 'pending', 100, ${target})
  `;
  try {
    await insert(randomUUID());
    // Second insert for the SAME target must not throw when it carries the
    // SAME ON CONFLICT clause the production persist CTE uses — the caller
    // detects the duplicate from 0 returned rows. Without migration 0045
    // this would have created a second deliverable row (the
    // duplicate-delivery bug).
    const ins = await sql`
      insert into notification_outbox
        (notification_id, type, content, metadata, status, priority, target_game_id)
      values
        (${randomUUID()}::uuid, 'prediction', 't', '{}', 'pending', 100, ${target})
      on conflict (type, target_game_id) where status in ('pending', 'inflight') do nothing
      returning notification_id
    `;
    assert.equal(ins.length, 0);
    const rows = await sql<{ n: string }>`
      select count(*)::int as n from notification_outbox
      where type = 'prediction' and target_game_id = ${target}
    `;
    assert.equal(Number(rows[0]?.n ?? 0), 1);
    // A dead-lettered predecessor frees the slot (temporal kill semantics).
    await sql`
      update notification_outbox set status = 'dead_letter'
      where type = 'prediction' and target_game_id = ${target}
    `;
    const ins2 = await insert(randomUUID());
    void ins2;
    const rows2 = await sql<{ n: string }>`
      select count(*)::int as n from notification_outbox
      where type = 'prediction' and target_game_id = ${target} and status = 'pending'
    `;
    assert.equal(Number(rows2[0]?.n ?? 0), 1);
  } finally {
    await sql`
      delete from notification_outbox where type = 'prediction' and target_game_id = ${target}
    `.catch(() => undefined);
  }
});
