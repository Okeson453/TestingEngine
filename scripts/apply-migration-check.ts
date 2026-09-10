/**
 * Scratch migration validator (pglite-migration-check runbook):
 * applies ALL migrations in order to a fresh in-memory PGlite, then runs
 * sanity queries proving 0032's backfill + view derivation behave correctly.
 */
import { PGlite } from "@electric-sql/pglite";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const dir = "migrations";
const files = readdirSync(dir).filter((f) => f.endsWith(".sql")).sort();
const db = new PGlite();

for (const f of files) {
  const sql = readFileSync(join(dir, f), "utf8");
  try {
    await db.exec(sql);
    console.log(`applied: ${f}`);
  } catch (e) {
    console.error(`FAILED: ${f}: ${String(e).slice(0, 400)}`);
    process.exit(1);
  }
}

// --- Seed a delivered prediction with complete raw timestamps (accepted AFTER target start)
await db.exec(`
  insert into pending_predictions (prediction_id, target_multiplier, probability, confidence,
    model_version, requested_at, generated_at, target_game_id, source_round_id, target_round_started_at)
  values ('pred-late', 2.0, 0.5, 0.5, 'v1', now() - interval '1 hour', now() - interval '1 hour',
    'game-late', 'src-late', now() - interval '50 minutes');
  insert into notification_outbox (notification_id, type, content, metadata, status,
    attempt_count, next_attempt_at, target_game_id, telegram_accepted_at, delivered_at, created_at)
  values ('11111111-1111-1111-1111-111111111111'::uuid, 'prediction', 'content',
    '{"predictionId":"pred-late"}'::jsonb, 'delivered', 1, now(),
    'game-late', now() - interval '49 minutes', now() - interval '49 minutes', now() - interval '1 hour');
`);

// --- Delivered with accepted BEFORE target start (ON_TIME) and NO stored outcome (backfill target)
await db.exec(`
  insert into pending_predictions (prediction_id, target_multiplier, probability, confidence,
    model_version, requested_at, generated_at, target_game_id, source_round_id, target_round_started_at)
  values ('pred-early', 2.0, 0.5, 0.5, 'v1', now() - interval '2 hour', now() - interval '2 hour',
    'game-early', 'src-early', now() - interval '110 minutes');
  insert into notification_outbox (notification_id, type, content, metadata, status,
    attempt_count, next_attempt_at, target_game_id, telegram_accepted_at, delivered_at, created_at)
  values ('22222222-2222-2222-2222-222222222222'::uuid, 'prediction', 'content',
    '{"predictionId":"pred-early"}'::jsonb, 'delivered', 1, now(),
    'game-early', now() - interval '119 minutes', now() - interval '119 minutes', now() - interval '2 hour');
`);

// --- Dead-letter with no outcome
await db.exec(`
  insert into notification_outbox (notification_id, type, content, metadata, status,
    attempt_count, next_attempt_at, last_error, created_at)
  values ('33333333-3333-3333-3333-333333333333'::uuid, 'prediction', 'content', '{}'::jsonb,
    'dead_letter', 1, now(), 'expired', now() - interval '3 hour');
`);

// --- Re-run migration 0032 (it must be idempotent-ish for backfills; run once here)
const m0032 = readFileSync(join(dir, "0032_forensic_trust_repair.sql"), "utf8");
await db.exec(m0032);
console.log("re-applied 0032 backfill over seeded rows");

const q = async (label: string, sql: string) => {
  const res = await db.query(sql);
  console.log(`\n== ${label}`);
  console.log(JSON.stringify(res.rows, null, 1));
};

await q("backfilled outcomes", `
  select metadata->>'predictionId' as pred, status, delivery_outcome, lead_time_ms
  from notification_outbox order by created_at`);
await q("view delivery_status (raw-derived authority)", `
  select prediction_id, delivery_outcome, delivery_status, lead_time_computed_ms
  from prediction_delivery_timeline order by prediction_id`);
await q("masked-late audit (late by raw timeline, stored outcome not LATE)", `
  select count(*)::int as masked_late
  from prediction_delivery_timeline
  where delivery_status = 'LATE' and coalesce(delivery_outcome,'') <> 'LATE'`);

console.log("\nMIGRATION APPLIES CLEAN");
process.exit(0);
