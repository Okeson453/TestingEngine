/**
 * PASS 8 — PR-primary durable persistence root cause, locked at the DB level.
 *
 * Production 17:39:48 window: PR ownership reserved → prediction ready
 * 48.313 → durable persistence FAILED 48.493 (exactly one Neon RTT — the
 * server ANSWERED with an error) → BG recomputed 7.2s later and persisted
 * fine. Same statement, same target, different trigger_event value.
 *
 * Root cause: migration 0039's CHECK constraint enumerated
 * ('BG','ED','POLL') only. The PR-primary promotion (e0d2d5b) writes
 * trigger_event='PR' — every PR persist was rejected server-side with
 * SQLSTATE 23514 (check_violation), and BG became the implicit 7-second
 * recovery path. Migration 0047 re-adds the constraint with 'PR' allowed.
 *
 * These tests pin the constraint AT THE DATABASE — both the definition and
 * the behavior (PR inserts accepted, garbage rejected with 23514).
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { getSql } from "@/lib/db";

test("0047: trigger_event check constraint allows PR", async () => {
  const sql = await getSql();
  const rows = await sql<{ conname: string; definition: string }>`
    select conname, pg_get_constraintdef(oid) as definition
    from pg_constraint
    where conname = 'pending_predictions_trigger_event_check'
  `;
  assert.equal(rows.length, 1, "constraint must exist (0039/0047 applied)");
  const def = rows[0]!.definition;
  for (const v of ["BG", "ED", "POLL", "PR"]) {
    assert.ok(def.includes(`'${v}'`), `constraint must allow '${v}': ${def}`);
  }
});

test("0047: a PR-triggered pending prediction inserts cleanly", async () => {
  const sql = await getSql();
  const pid = `pass8-pr-check-${Date.now()}`;
  try {
    const rows = await sql<{ prediction_id: string; trigger_event: string }>`
      insert into pending_predictions
        (prediction_id, trigger_event, trigger_round_id, target_game_id, source_round_id)
      values (${pid}, 'PR', '999999', '1000000', '999999')
      returning prediction_id, trigger_event
    `;
    assert.equal(rows[0]!.trigger_event, "PR");
  } finally {
    await sql`delete from pending_predictions where prediction_id = ${pid}`;
  }
});

test("0047: a bogus trigger_event is still rejected with SQLSTATE 23514", async () => {
  const sql = await getSql();
  const pid = `pass8-bogus-check-${Date.now()}`;
  await assert.rejects(
    () =>
      sql`insert into pending_predictions
            (prediction_id, trigger_event, trigger_round_id, target_game_id, source_round_id)
          values (${pid}, 'BOGUS', '999999', '1000000', '999999')`,
    (err: unknown) => {
      const code = (err as { code?: string }).code;
      assert.equal(code, "23514", `expected check_violation 23514, got ${code}`);
      return true;
    },
  );
});

test("persistence failure details are INLINE in the log message (Railway strips JSON)", async () => {
  const { readFileSync } = await import("node:fs");
  const { fileURLToPath } = await import("node:url");
  const src = readFileSync(
    fileURLToPath(new URL("./predictor.ts", import.meta.url)),
    "utf8",
  );
  // The generic message alone made the 17:39Z window unattributable —
  // Railway strips every JSON field (failureReason/sqlstate/errorName).
  assert.ok(
    src.includes("[sqlstate=") && src.includes("[err=${errName}: ${errMsg}]"),
    "persistence failure message must inline err name+message+sqlstate",
  );
  assert.ok(
    src.includes("[atomic_cte=no explicit tx, nothing to roll back]"),
    "failure message must state rollback semantics",
  );
});
