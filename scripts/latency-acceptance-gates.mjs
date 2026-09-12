/**
 * LATENCY ACCEPTANCE GATES (sep 12) — production-path load/stress harness.
 *
 * Drives the REAL BG-primary pipeline against a real Postgres with:
 *   - simulated Neon RTT on every query (pg.Client.prototype.query)
 *   - simulated Neon TLS+auth on every NEW connection (pg.Client.prototype.connect)
 *   - a stubbed Telegram transport (one RTT)
 *   - concurrent general/critical-pool noise per round (event-log writes,
 *     worker_state upserts, a reconcile stand-in on the critical pool)
 *   - a mid-run connection kill (SIM_KILL_ROUND) that reproduces the
 *     production 1093ms class of acquire outlier: Neon kills an idle server
 *     -side connection; the next pool consumer pays TLS+auth to rebuild.
 *
 * The harness is tree-agnostic: run it on the working tree (after) and on a
 * clean stash (before) and diff the printed gate table.
 *
 * Usage:
 *   DATABASE_URL=postgres://... PG_SSL=0 bun scripts/latency-acceptance-gates.mjs
 * Env:
 *   SIM_RTT_MS     per-query network RTT (default 25)
 *   SIM_TLS_MS     new-connection TLS+auth cost (default 1100)
 *   SIM_ROUNDS     BG rounds to run (default 40)
 *   SIM_KILL_ROUND round at which to kill an idle critical-pool client
 *                  (0 disables; default 25)
 *   SIM_NOISE      background pool noise on/off (default on)
 * Exit code 0 iff every gate passes.
 */
const RTT_MS = Number(process.env.SIM_RTT_MS ?? 25);
const TLS_MS = Number(process.env.SIM_TLS_MS ?? 1100);
const ROUNDS = Number(process.env.SIM_ROUNDS ?? 120);
const KILL_ROUND = Number(process.env.SIM_KILL_ROUND ?? 25);
const NOISE = process.env.SIM_NOISE !== "0";
const PINNED_KILL_ROUND = Number(process.env.SIM_PINNED_KILL_ROUND ?? 0);

if (!process.env.DATABASE_URL) {
  console.error("latency-acceptance-gates: DATABASE_URL is required (real-pool mode)");
  process.exit(2);
}
process.env.PG_SSL ||= "0";
process.env.MIN_SIGNAL_EDGE ||= "0"; // latency harness: emit every round
process.env.TELEGRAM_BOT_TOKEN ||= "123:test";
process.env.TELEGRAM_CHAT_ID ||= "-1001";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── pg instrumentation (must precede app-module import) ────────────────────
const pgmod = await import("pg");
const PgClient = pgmod.default.Client;
let queryCount = 0;
let newConnections = 0;
const origClientQuery = PgClient.prototype.query;
PgClient.prototype.query = async function (...args) {
  queryCount += 1;
  await sleep(RTT_MS); // simulated Neon network RTT
  return origClientQuery.apply(this, args);
};
const origClientConnect = PgClient.prototype.connect;
PgClient.prototype.connect = function (...args) {
  const cb = typeof args[args.length - 1] === "function" ? args.pop() : null;
  const run = async () => {
    newConnections += 1;
    await sleep(TLS_MS); // simulated Neon TLS+auth on every NEW connection
    return origClientConnect.apply(this, args);
  };
  if (cb) run().then(() => cb(), (e) => cb(e));
  else return run();
};

// ── log capture (predictor stage timings from PREDICTION_SIGNAL_READY) ─────
const signalProfiles = new Map(); // targetGameId -> stage ms fields
const origConsoleLog = console.log;
console.log = (...args) => {
  try {
    const line = typeof args[0] === "string" ? args[0] : "";
    if (line.includes("PREDICTION_SIGNAL_READY") && line.startsWith("{")) {
      const obj = JSON.parse(line);
      if (obj.targetGameId) {
        signalProfiles.set(String(obj.targetGameId), {
          claimMs: obj.claimMs, historyMs: obj.historyMs,
          predictionMs: obj.predictionMs, predictionToSignalMs: obj.predictionToSignalMs,
          totalMs: obj.totalMs,
        });
      }
    }
  } catch { /* not a profile line */ }
  origConsoleLog(...args);
};

// ── app modules ────────────────────────────────────────────────────────────
const { _setTelegramTransportForTests } = await import("../src/lib/notifications/telegram.ts");
const { attemptNPlusOnePrediction } = await import("../src/lib/prediction/live/prediction-attempt.ts");
const { OutboxDispatcher } = await import("../src/lib/prediction/live/notification-worker.ts");
const { getSql, getCriticalSql, getCriticalPool } = await import("../src/lib/db.ts");
const { insertNewRounds } = await import("../src/lib/crash/ingest.ts");
const { warmLiveHistoryBuffer, appendCompletedRound } = await import("../src/lib/prediction/live/live-history-buffer.ts");
const { observeCrashForACIE } = await import("../src/lib/prediction/live/predictor.ts");

const deliveries = [];
_setTelegramTransportForTests(async () => {
  const start = performance.now();
  await sleep(RTT_MS); // Telegram leg ≈ 1 RTT
  const end = performance.now();
  deliveries.push({ start, end });
  return { ok: true, status: 200, json: async () => ({ ok: true, result: { message_id: 1 } }) };
});

// ── event-loop stall monitor ───────────────────────────────────────────────
const loopLags = [];
let lastTick = performance.now();
const loopTimer = setInterval(() => {
  const now = performance.now();
  loopLags.push(Math.max(0, now - lastTick - 25));
  lastTick = now;
  if (loopLags.length > 20_000) loopLags.shift();
}, 25);
loopTimer.unref?.();

// ── DB bootstrap ───────────────────────────────────────────────────────────
const sql = await getSql();
let dbClockOffsetMs = 0;

// DB clock offset. Warm the pool first — the offset probe must not pay the
// simulated TLS+auth inside its own measurement window.
{
  await sql`select 1`;
  const r = await sql`select now()::text as n`;
  // host - db, measured at RESPONSE time (the probe query's own network
  // delay must not be baked into the offset).
  dbClockOffsetMs = Date.now() - new Date(String(r[0].n)).getTime();
}

await sql`truncate pending_predictions, prediction_validations, notification_outbox, live_event_log, crash_rounds, worker_state restart identity cascade`;

// Seed history like the integration suite (pattern known to fire signals).
{
  const r = await sql`select now()::text as n`;
  const dbNowMs = new Date(String(r[0].n)).getTime();
  const seeds = [];
  for (let i = 1; i <= 120; i += 1) {
    const crashedAt = new Date(dbNowMs - (120 - i) * 4_000);
    seeds.push({
      gameId: String(1000 + i),
      multiplier: 1 + (i % 13) * 0.13,
      hash: null,
      salt: null,
      beganAt: new Date(crashedAt.getTime() - 3_000),
      crashedAt,
    });
  }
  await insertNewRounds(seeds);
}

// Boot parity: prod warms the live history buffer before the pipeline starts.
await warmLiveHistoryBuffer(sql);

// Destroy a CHECKED-OUT (pinned) critical client — tests the pinned-lane
// fallback: the hot-path query should fail over to the pool and succeed.
function killPinnedClient() {
  const pool = getCriticalPool();
  if (!pool) return;
  const idle = new Set((pool._idle ?? []).map((it) => it?.client ?? it));
  const checked = (pool._clients ?? []).filter((c) => !idle.has(c));
  const stream = checked[0]?.connection?.stream;
  if (stream) {
    stream.destroy();
    console.warn(`[kill] destroyed a CHECKED-OUT (pinned) critical client`);
  } else {
    console.warn(`[kill] no checked-out client stream found`);
  }
}

function killIdleCriticalClient() {
  const pool = getCriticalPool();
  if (!pool) return;
  const idle = pool._idle;
  if (!Array.isArray(idle) || idle.length === 0) {
    console.warn(`[kill] no idle critical client to kill (round sim)`);
    return;
  }
  const item = idle[idle.length - 1];
  const client = item?.client ?? item;
  const stream = client?.connection?.stream;
  if (stream) {
    stream.destroy();
    console.warn(`[kill] destroyed an idle critical-pool socket (simulating Neon idle kill)`);
  } else {
    console.warn(`[kill] could not reach idle client stream — skipping`);
  }
}

// ── dispatcher ─────────────────────────────────────────────────────────────
const dispatcher = new OutboxDispatcher();
await dispatcher.start();

// ── noise generators ───────────────────────────────────────────────────────
function fireNoise(round) {
  if (!NOISE) return;
  void (async () => {
    try {
      const s = await getSql();
      await s`
        insert into live_event_log (correlation_id, event_kind, game_id, payload, received_at, processed_at, processor_latency_ms, sla_violated)
        values (${`noise:${round}:${Math.random()}`}, 'NOISE', 'noise', '{}'::jsonb, now(), now(), 0, false)
      `;
      await s`
        insert into worker_state (key, value) values ('noise_general', ${String(round)})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
    } catch { /* noise is best-effort */ }
  })();
  // Reconcile stand-in: the real bg reconcile CTE runs on the CRITICAL pool
  // concurrently with the persist.
  void (async () => {
    try {
      const s = await getCriticalSql();
      await s`
        insert into worker_state (key, value) values ('noise_critical', ${String(round)})
        on conflict (key) do update set value = excluded.value, updated_at = now()
      `;
    } catch { /* soft */ }
  })();
}

// ── percentile helper ──────────────────────────────────────────────────────
function pct(arr, p) {
  if (arr.length === 0) return Number.NaN;
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
}

// ── run rounds ─────────────────────────────────────────────────────────────
const samples = {
  decisionMs: [], // BG event → BG prediction decision (compute+gates)
  computeMs: [], // model computation itself
  handoffMs: [], // decision → durable outbox handoff
  bgToSignalReadyMs: [], // BG event → SIGNAL_READY (durable)
  signalReadyToClaimMs: [], // durable → claimed (DB clock)
  claimToDispatchMs: [], // claimed → telegram submit start
  handoffToDispatchMs: [], // durable → telegram submit start
  signalReadyToDeliveryMs: [], // SIGNAL_READY → delivery accepted
  e2eMs: [], // BG event → delivery accepted
};
let predicted = 0;
let skipped = 0;
let maxStall = 0;

for (let i = 1; i <= ROUNDS; i += 1) {
  const gameId = String(2000 + i); // source round N (started)
  const targetGameId = String(2000 + i + 1); // N+1
  fireNoise(i);
  if (KILL_ROUND > 0 && i >= KILL_ROUND && i % 15 === 0) killIdleCriticalClient();
  if (PINNED_KILL_ROUND > 0 && i === PINNED_KILL_ROUND) killPinnedClient();

  const tEvent = performance.now();
  // Prod parity: ACIE observed the crash of the last COMPLETED round (N-1,
  // the history tail) before BG(N) fires — the ED handler feeds it.
  const lastCompleted = String(2000 + i - 1);
  const lastCompletedMultiplier = 1 + ((i - 1 + 12) % 13) * 0.13; // pattern, tail-aligned
  observeCrashForACIE(i === 1 ? "1120" : lastCompleted, i === 1 ? 1 + (120 % 13) * 0.13 : lastCompletedMultiplier, new Date(Date.now() - 15_000).toISOString());
  const trace = { traceId: `gate:${i}`, sourceGameId: gameId, t0: tEvent, marks: { ws_received: tEvent } };
  const res = await attemptNPlusOnePrediction({
    sourceRoundId: gameId,
    sourceCrashAt: new Date(Date.now() - 10).toISOString(), // round N began ~now (BG trigger)
    source: "BG",
    correlationId: `gate:${i}`,
    trace,
  });
  const tReturn = performance.now();

  // Round N completes (prod: ED(N) arrives); append to the live history
  // buffer so the next BG attempt's freshness tail advances — regardless of
  // whether this round emitted a signal.
  appendCompletedRound({
    gameId,
    multiplier: lastCompletedMultiplier,
    crashedAt: new Date(Date.now() - 10).toISOString(),
    beganAt: new Date(Date.now() - 3_000).toISOString(),
  });

  if (!res.attempted || res.kind !== "predicted") {
    skipped += 1;
    if (i === 1) {
      console.error(`[harness] first round did not predict:`, JSON.stringify(res));
    }
    continue;
  }
  predicted += 1;

  const marks = trace.marks;
  const profile = signalProfiles.get(String(targetGameId)) ?? {};
  const computeMs = Number(profile.predictionMs ?? Number.NaN);
  const decisionMs = Number(profile.totalMs ?? Number.NaN);

  // Wait for the delivery stub to observe this round's send.
  const targetDeliveries = predicted;
  let waited = 0;
  while (deliveries.length < targetDeliveries && waited < 30_000) {
    await sleep(5);
    waited += 5;
  }
  if (deliveries.length < targetDeliveries) {
    console.error(`[harness] round ${i}: delivery never observed`);
    continue;
  }
  const stub = deliveries[deliveries.length - 1];

  const row = await sql`
    select created_at::text, dispatch_claimed_at::text, status
    from notification_outbox
    where metadata->>'targetGameId' = ${targetGameId}
    order by created_at desc limit 1
  `;
  const r = row[0];
  if (!r) {
    console.error(`[harness] round ${i}: no outbox row for ${targetGameId}`);
    continue;
  }
  const createdHost = new Date(r.created_at).getTime() + dbClockOffsetMs;
  const claimedHost = r.dispatch_claimed_at
    ? new Date(r.dispatch_claimed_at).getTime() + dbClockOffsetMs
    : Number.NaN;
  const stubStartWall = tEvent + (stub.start - tEvent); // same base
  const createdPerf = tEvent + (createdHost - (tEvent + (Date.now() - tEvent)));
  // Convert host epoch of DB timestamps into the performance.now base:
  const hostNowPerfDelta = Date.now() - tEvent - (performance.now() - tEvent); // ≈0 (same clock domain)
  const createdPerfMs = createdHost - (Date.now() - performance.now());
  const claimedPerfMs = claimedHost - (Date.now() - performance.now());

  samples.computeMs.push(computeMs);
  samples.decisionMs.push(decisionMs);
  // Durable handoff = decision done → outbox row committed (DB clock).
  samples.handoffMs.push(Number.isFinite(createdPerfMs) ? createdPerfMs - tEvent - decisionMs : Number.NaN);
  samples.bgToSignalReadyMs.push(tReturn - tEvent);
  samples.signalReadyToClaimMs.push(Number.isFinite(claimedPerfMs) ? claimedPerfMs - createdPerfMs : Number.NaN);
  samples.claimToDispatchMs.push(stub.start - claimedPerfMs);
  samples.handoffToDispatchMs.push(stub.start - createdPerfMs);
  samples.signalReadyToDeliveryMs.push(stub.end - createdPerfMs);
  samples.e2eMs.push(stub.end - tEvent);
  void stubStartWall; void createdPerf; void hostNowPerfDelta;

  // Small inter-round gap (rounds are ~10-70s in prod; stress tighter).
  await sleep(Number(process.env.SIM_ROUND_GAP_MS ?? 150));
}

clearInterval(loopTimer);

// loop stall stats (sample interval 25ms; drop the first partial sample)
const lags = loopLags.slice(1);
maxStall = lags.length ? Math.max(...lags) : 0;

// ── gates ──────────────────────────────────────────────────────────────────
const GATES = [
  { name: "BG event → decision", get: (s) => [s.decisionMs, [[50, 50], [95, 100], [99, 200]]] },
  { name: "BG compute", get: (s) => [s.computeMs, [[50, 10], [95, 25], [99, 50]]] },
  { name: "BG → durable handoff", get: (s) => [s.handoffMs, [[50, 50], [95, 100], [200, 200]]] },
  { name: "BG event → SIGNAL_READY", get: (s) => [s.bgToSignalReadyMs, [[50, 100], [95, 200], [99, 300]]] },
  { name: "SIGNAL_READY → claim", get: (s) => [s.signalReadyToClaimMs, [[50, 100], [95, 250], [99, 500]]] },
  { name: "claim → dispatch", get: (s) => [s.claimToDispatchMs, [[50, 100], [95, 250], [99, 500]]] },
  { name: "SIGNAL_READY → delivery", get: (s) => [s.signalReadyToDeliveryMs, [[50, 500], [95, 750], [99, 1000]]] },
  { name: "E2E BG → delivery", get: (s) => [s.e2eMs, [[50, 600], [95, 1000], [99, 1500]]] },
];

console.log(`\n=== LATENCY ACCEPTANCE GATES (rounds=${ROUNDS} predicted=${predicted} skipped=${skipped} simRTT=${RTT_MS}ms simTLS=${TLS_MS}ms killRound=${KILL_ROUND}) ===`);
let allPass = true;
for (const gate of GATES) {
  const [values, limits] = gate.get(samples);
  const vals = values.filter(Number.isFinite);
  let gatePass = vals.length > 0;
  const parts = [];
  for (const [p, limit] of limits) {
    const v = pct(vals, p);
    const ok = v <= limit;
    if (!ok) gatePass = false;
    parts.push(`P${p}=${Number.isFinite(v) ? v.toFixed(1) : "n/a"}ms (≤${limit})${ok ? "" : " FAIL"}`);
  }
  if (!gatePass) allPass = false;
  console.log(`${gatePass ? "PASS" : "FAIL"}  ${gate.name.padEnd(26)} n=${vals.length}  ${parts.join("  ")}`);
}

// stall / lag gates
const lagP95 = pct(lags, 95);
const lagP99 = pct(lags, 99);
{
  let ok = lagP95 <= 50 && lagP99 <= 100 && maxStall <= 250;
  if (!ok) allPass = false;
  console.log(`${ok ? "PASS" : "FAIL"}  event-loop lag               P95=${lagP95.toFixed(1)}ms (≤50)  P99=${lagP99.toFixed(1)}ms (≤100)  maxStall=${maxStall.toFixed(1)}ms (≤250)`);
}

console.log(`\nqueries=${queryCount} newConnections=${newConnections} deliveries=${deliveries.length}`);
console.log(allPass ? "ALL GATES PASS" : "GATE FAILURES PRESENT");
process.exitCode = allPass ? 0 : 1;
await dispatcher.stop();
process.exit(process.exitCode);
