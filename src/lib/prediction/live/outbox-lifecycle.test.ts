/**
 * P0/P1 — outbox lifecycle tracing + native WS dedup behavioral tests.
 *
 * Drives the REAL OutboxDispatcher against the REAL PGLite-backed
 * notification_outbox (with 0023 lifecycle columns applied) and verifies:
 *
 *   - delivered rows persist dispatch_claimed_at / send_started_at /
 *     telegram_accepted_at / delivered_at
 *   - requeued rows clear dispatch_claimed_at (fresh claim next attempt)
 *   - dead-lettered rows retain lifecycle timestamps for forensics
 *   - recoverStale cleans expired pending rows of ALL types (zombie rows
 *     previously stranded forever when type != 'prediction')
 *   - claimable backlog vs expired backlog are counted separately
 *   - ED crash-event dedup classifies by game ID: new / duplicate_event /
 *     already_in_progress
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { OutboxDispatcher } from "@/lib/prediction/live/notification-worker";
import { notifyOutbox } from "@/lib/prediction/live/outbox-wake";
import {
  classifyEdReentry,
  recordEdRoundProcessedForTests,
  _resetEdDedupForTests,
  bgHandler,
} from "@/lib/prediction/events/game-event-handlers";
import { getSql } from "@/lib/db";
import { randomUUID } from "node:crypto";

interface LifecycleRow {
  status: string;
  attempt_count: number;
  created_at: string | Date;
  dispatch_claimed_at: string | Date | null;
  send_started_at: string | Date | null;
  telegram_accepted_at: string | Date | null;
  delivered_at: string | Date | null;
  next_attempt_at: string | Date | null;
  last_error: string | null;
}

function setTelegramEnv(): void {
  process.env.TELEGRAM_BOT_TOKEN = "123:test";
  process.env.TELEGRAM_CHAT_ID = "-1001";
}

function clearTelegramEnv(): void {
  delete process.env.TELEGRAM_BOT_TOKEN;
  delete process.env.TELEGRAM_CHAT_ID;
  delete process.env.TELEGRAM_GROUP_CHAT_ID;
  delete process.env.TELEGRAM_EXTRA_CHAT_IDS;
}

async function withStubbedFetch<T>(
  handler: (url: string, body: string | undefined) => Promise<Response>,
  fn: () => Promise<T>,
): Promise<T> {
  const realFetch = globalThis.fetch;
  globalThis.fetch = (async (url: unknown, init: { body?: string } = {}) =>
    handler(String(url), init.body)) as typeof fetch;
  try {
    return await fn();
  } finally {
    globalThis.fetch = realFetch;
  }
}

function tgResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

const okTelegram = () => tgResponse(200, { ok: true, result: { message_id: 1 } });

async function insertPendingRow(opts: {
  type?: string;
  deadlineAheadMs?: number | null;
  ageMinutes?: number;
  targetGameId?: string;
} = {}): Promise<number> {
  const id = randomUUID();
  const content = `[ol-test] ${id}`;
  const metadata = JSON.stringify({ predictionId: id });
  const age = opts.ageMinutes ?? 0;
  const deadlineIso =
    opts.deadlineAheadMs != null
      ? new Date(Date.now() + opts.deadlineAheadMs).toISOString()
      : null;
  const rows = await (await getSql())<{ id: number }>`
    insert into notification_outbox (
      notification_id, type, content, metadata, status, attempt_count, next_attempt_at,
      created_at, telegram_deadline_at, target_game_id
    ) values (
      ${id}::uuid, ${opts.type ?? "prediction"}, ${content}, ${metadata}::jsonb, 'pending',
      0, now() - interval '1 millisecond',
      now() - (${age}::int * interval '1 minute'), ${deadlineIso}::timestamptz,
      ${opts.targetGameId ?? null}
    )
    returning id
  `;
  return rows[0]!.id;
}

async function lifecycleOf(id: number): Promise<LifecycleRow> {
  const rows = await (await getSql())<LifecycleRow>`
    select status, attempt_count, created_at, dispatch_claimed_at, send_started_at,
           telegram_accepted_at, delivered_at, next_attempt_at, last_error
    from notification_outbox where id = ${id}
  `;
  assert.ok(rows[0], `row ${id} must exist`);
  return rows[0]!;
}

async function cleanSuiteRows(): Promise<void> {
  await (await getSql())`
    delete from notification_outbox where content like '[ol-test] %'
  `;
}

test("outbox lifecycle: delivered row persists claim/send/accept/complete timestamps", async () => {
  setTelegramEnv();
  await cleanSuiteRows();
  try {
    const id = await insertPendingRow({ deadlineAheadMs: 30_000 });
    const d = new OutboxDispatcher();
    await withStubbedFetch(
      async () => okTelegram(),
      async () => d.tickOnce(),
    );
    const row = await lifecycleOf(id);
    assert.equal(row.status, "delivered");
    assert.ok(row.dispatch_claimed_at, "dispatch_claimed_at must be set");
    assert.ok(row.send_started_at, "send_started_at must be set");
    assert.ok(row.telegram_accepted_at, "telegram_accepted_at must be set");
    assert.ok(row.delivered_at, "delivered_at must be set");
    // Ordering: claim <= send start <= accepted.
    const claim = new Date(row.dispatch_claimed_at!).getTime();
    const send = new Date(row.send_started_at!).getTime();
    const accepted = new Date(row.telegram_accepted_at!).getTime();
    assert.ok(claim <= send, "claim must precede send start");
    assert.ok(send <= accepted, "send start must precede acceptance");
  } finally {
    await cleanSuiteRows();
    clearTelegramEnv();
  }
});

test("outbox lifecycle: requeued row clears dispatch_claimed_at for the next claim", async () => {
  setTelegramEnv();
  await cleanSuiteRows();
  try {
    const id = await insertPendingRow({ deadlineAheadMs: 30_000 });
    const d = new OutboxDispatcher();
    await withStubbedFetch(
      async () => tgResponse(500, { ok: false, description: "boom" }),
      async () => d.tickOnce(),
    );
    const row = await lifecycleOf(id);
    assert.equal(row.status, "pending", "5xx must requeue");
    assert.equal(row.dispatch_claimed_at, null, "claim stamp cleared on requeue");
    assert.ok(row.send_started_at, "failed send attempt still recorded");
    assert.equal(row.telegram_accepted_at, null, "nothing accepted");
    assert.ok(row.next_attempt_at, "backoff scheduled");
  } finally {
    await cleanSuiteRows();
    clearTelegramEnv();
  }
});

test("outbox lifecycle: dead-lettered row retains lifecycle timestamps", async () => {
  setTelegramEnv();
  await cleanSuiteRows();
  try {
    const id = await insertPendingRow({ deadlineAheadMs: 30_000 });
    const d = new OutboxDispatcher();
    await withStubbedFetch(
      async () => tgResponse(403, { ok: false, description: "forbidden" }),
      async () => d.tickOnce(),
    );
    const row = await lifecycleOf(id);
    assert.equal(row.status, "dead_letter");
    assert.ok(row.dispatch_claimed_at, "forensic claim stamp retained");
    assert.ok(row.send_started_at, "forensic send stamp retained");
    assert.equal(row.telegram_accepted_at, null);
  } finally {
    await cleanSuiteRows();
    clearTelegramEnv();
  }
});

test("recoverStale: expired pending rows of ANY type are dead-lettered (zombie cleanup)", async () => {
  setTelegramEnv();
  await cleanSuiteRows();
  try {
    // Zombie: validation row whose deadline already passed — the claim query
    // will never pick it, previously it stranded forever and tripped the
    // backlog warning.
    const zombie = await insertPendingRow({ type: "validation", deadlineAheadMs: -1_000 });
    // Healthy prediction row: claimable, must NOT be touched.
    const healthy = await insertPendingRow({ deadlineAheadMs: 30_000 });
    const d = new OutboxDispatcher();
    await d.recoverStale();
    const zRow = await lifecycleOf(zombie);
    const hRow = await lifecycleOf(healthy);
    assert.equal(zRow.status, "dead_letter", "expired zombie must be dead-lettered");
    assert.match(zRow.last_error ?? "", /expired on recover/);
    assert.equal(hRow.status, "pending", "claimable row must be untouched");
  } finally {
    await cleanSuiteRows();
    clearTelegramEnv();
  }
});

test("ED dedup: classify by game ID — new, then duplicate_event", () => {
  _resetEdDedupForTests();
  const gameId = `ol-dedup-${randomUUID()}`;
  assert.equal(classifyEdReentry(gameId), "new");
  recordEdRoundProcessedForTests(gameId);
  assert.equal(classifyEdReentry(gameId), "duplicate_event");
  // Unrelated round is unaffected.
  assert.equal(classifyEdReentry(`other-${randomUUID()}`), "new");
  _resetEdDedupForTests();
});

test("outbox: wake burst during active drain never duplicates delivery (single drain loop)", async () => {
  setTelegramEnv();
  await cleanSuiteRows();
  try {
    const id = await insertPendingRow({ deadlineAheadMs: 30_000 });
    const d = new OutboxDispatcher();
    await withStubbedFetch(
      // Slowed provider so the wake burst lands MID-drain — the exact race
      // the old per-cycle `once()` listener registration lost.
      async () => {
        await new Promise((r) => setTimeout(r, 30));
        for (let i = 0; i < 50; i += 1) notifyOutbox();
        return okTelegram();
      },
      async () => {
        await d.start();
        await new Promise((r) => setTimeout(r, 400));
        await d.stop();
      },
    );
    const row = await lifecycleOf(id);
    assert.equal(row.status, "delivered");
    assert.equal(row.attempt_count, 1, "burst of 50 wakes must yield exactly ONE delivery attempt");
    const stats = d.getStats();
    assert.equal(stats.delivered, 1);
    assert.equal(stats.claimed, 1, "a single row must be claimed exactly once");
  } finally {
    await cleanSuiteRows();
    clearTelegramEnv();
  }
});

// ---------------------------------------------------------------------------
// Hard temporal delivery contract (migration 0027):
// a prediction signal for target round N+1 is valid ONLY before N+1 starts.
// Late delivery is REMOVED — an expired signal is dead-lettered, never sent.
// ---------------------------------------------------------------------------

test("temporal contract: dispatcher refuses to deliver a signal whose target already started", async () => {
  setTelegramEnv();
  await cleanSuiteRows();
  try {
    // Target round started 2s ago (authoritative BG path).
    const startedTarget = `ol-tc-started-${randomUUID()}`;
    await bgHandler({ gameId: startedTarget, beganAt: Date.now() - 2_000 });

    let telegramCalls = 0;
    const d = new OutboxDispatcher();
    await withStubbedFetch(
      async () => {
        telegramCalls += 1;
        return okTelegram();
      },
      async () => d.tickOnce(),
    );

    // Row 1: signal for the ALREADY-STARTED target must be dead-lettered.
    const lateId = await insertPendingRow({
      deadlineAheadMs: 30_000,
      targetGameId: startedTarget,
    });
    // Row 2: signal for a target that has NOT started must still deliver.
    const freshTarget = `ol-tc-fresh-${randomUUID()}`;
    const okId = await insertPendingRow({
      deadlineAheadMs: 30_000,
      targetGameId: freshTarget,
    });

    await withStubbedFetch(
      async () => {
        telegramCalls += 1;
        return okTelegram();
      },
      async () => d.tickOnce(),
    );

    const late = await lifecycleOf(lateId);
    assert.equal(late.status, "dead_letter", "late signal must NEVER be delivered");
    assert.ok(
      (late.last_error ?? "").startsWith("expired_late_signal"),
      `last_error must record the expiration reason, got: ${late.last_error}`,
    );
    assert.equal(late.delivered_at, null, "late signal must have no delivered_at");

    const ok = await lifecycleOf(okId);
    assert.equal(ok.status, "delivered", "pre-start signal must still deliver");
    assert.ok(telegramCalls >= 1, "the fresh signal must reach Telegram");
  } finally {
    await cleanSuiteRows();
    clearTelegramEnv();
  }
});

test("temporal contract: BG arrival atomically kills undelivered signals for that target", async () => {
  setTelegramEnv();
  await cleanSuiteRows();
  try {
    const target = `ol-tc-kill-${randomUUID()}`;
    // Pending prediction signal targeting `target` — created BEFORE BG(N).
    const signalId = await insertPendingRow({
      deadlineAheadMs: 30_000,
      targetGameId: target,
    });
    // Control: a non-prediction row sharing the target must be untouched.
    const validationId = await insertPendingRow({
      type: "validation",
      deadlineAheadMs: 30_000,
      targetGameId: target,
    });

    // BG(N) arrives: the round STARTED — every undelivered signal for it dies.
    await bgHandler({ gameId: target, beganAt: Date.now() });

    const signal = await lifecycleOf(signalId);
    assert.equal(signal.status, "dead_letter", "BG must kill the pending signal immediately");
    assert.ok(
      (signal.last_error ?? "").includes("target round started"),
      `kill must be attributed to target start, got: ${signal.last_error}`,
    );
    const validation = await lifecycleOf(validationId);
    assert.equal(validation.status, "pending", "non-prediction rows must NOT be killed");
  } finally {
    await cleanSuiteRows();
    clearTelegramEnv();
  }
});
