/**
 * Production-path latency validation: drive the REAL OutboxDispatcher against
 * the migrated local PGLite with a synthetic per-round-trip network latency
 * (simulating Neon RTT) and a stub Telegram transport. Measures the
 * SIGNAL_READY→Telegram-ACK path query count and wall time.
 *
 * Usage: bun scripts/validate-dispatch-latency.mjs
 * Run on the working tree (new path) and on a clean stash (old path) and diff.
 */
const RTT_MS = Number(process.env.SIM_RTT_MS ?? 100);

process.env.PG_DATA_PATH ||= new URL("../data/crashwave", import.meta.url).pathname;
process.env.TELEGRAM_BOT_TOKEN = "123:test";
process.env.TELEGRAM_CHAT_ID = "-1001";
process.env.DATABASE_URL = "";

const { PGlite } = await import("@electric-sql/pglite");
// Instrument at the PROTOTYPE so the app's own PGlite instance is counted and
// delayed too (the app creates its own instance inside db.ts).
let queryCount = 0;
const origQuery = PGlite.prototype.query;
PGlite.prototype.query = async function (...a) {
  queryCount += 1;
  await new Promise((r) => setTimeout(r, RTT_MS)); // synthetic network RTT
  return origQuery.apply(this, a);
};

const { _setTelegramTransportForTests } = await import("../src/lib/notifications/telegram.ts");
_setTelegramTransportForTests(async () => {
  await new Promise((r) => setTimeout(r, RTT_MS)); // Telegram leg at 1 RTT
  return {
    ok: true,
    status: 200,
    json: async () => ({ ok: true, result: { message_id: 1 } }),
  };
});

const { OutboxDispatcher } = await import("../src/lib/prediction/live/notification-worker.ts");
const { getSql } = await import("../src/lib/db.ts");

const sql = await getSql();
await sql`delete from notification_outbox where metadata->>'bench' = 'latency'`;

const id = crypto.randomUUID();
await sql`
  insert into notification_outbox (
    notification_id, type, content, metadata, status, attempt_count, next_attempt_at,
    created_at, telegram_deadline_at, target_game_id
  ) values (
    ${id}::uuid, 'prediction', '[bench] prediction',
    ${JSON.stringify({ bench: "latency", predictionId: id, targetGameId: "99000001" })}::jsonb,
    'pending', 0, now() - interval '1 millisecond', now(),
    ${new Date(Date.now() + 30_000).toISOString()}::timestamptz, '99000001'
  )
`;

const d = new OutboxDispatcher();
queryCount = 0;
const t0 = performance.now();
await d.tickOnce();
const elapsed = Math.round(performance.now() - t0);

const row = await sql`select status, send_started_at, dispatch_claimed_at, delivered_at from notification_outbox where notification_id = ${id}::uuid`;
console.log(JSON.stringify({
  simRttMs: RTT_MS,
  dispatchQueryCount: queryCount,
  ackWallMs: elapsed,
  finalStatus: row[0]?.status,
  hadSendStamp: row[0]?.send_started_at != null,
}));
process.exit(0);
