/**
 * §9.4, §9.5, §9.6 — outbox dispatcher behavioral tests.
 *
 * These tests drive the REAL OutboxDispatcher against the REAL PGLite-backed
 * `notification_outbox` table, with `globalThis.fetch` stubbed so
 * `sendTelegramMessage` (the only network caller) never leaves the process.
 * They verify the actual status state machine:
 *
 *   PENDING --tick--> INFLIGHT --2xx--> DELIVERED
 *                            \--4xx (non-429)--> DEAD (no retry)
 *                            \--5xx/timeout/network--> PENDING (backoff)
 *   attempts >= MAX_ATTEMPTS --tick--> DEAD
 *   INFLIGHT stuck > STALE_MS --recoverStale--> PENDING (recovered)
 *
 * Plus: claim idempotency (a delivered row is never re-claimed), dispatcher
 * stats, and start/stop idempotency.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { OutboxDispatcher, MAX_ATTEMPTS } from "@/lib/prediction/live/notification-worker";
import { getSql } from "@/lib/db";
import { randomUUID } from "node:crypto";

interface OutboxState {
  status: string;
  attempt_count: number;
  delivered_at: string | Date | null;
  last_error: string | null;
  next_attempt_at: string | Date | null;
}

/** Env needed for sendTelegramMessage to actually reach (stubbed) fetch. */
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

/**
 * Replace global fetch for the duration of fn. handler receives the request
 * URL and JSON body string; return a Response (or throw to simulate a
 * network failure).
 */
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
const failTelegram = (status: number, description: string) =>
  tgResponse(status, { ok: false, description });

/** Insert a pending outbox row tagged with a unique content prefix. */
async function insertPendingRow(opts: {
  attemptCount?: number;
  deadlineAheadMs?: number | null;
} = {}): Promise<{ id: number; notificationId: string; content: string }> {
  const id = randomUUID();
  const content = `[nw-behavior] ${id}`;
  const metadata = JSON.stringify({ predictionId: id, kind: "prediction" });
  const rows = await (await getSql())<{ id: number }>`
    insert into notification_outbox (
      notification_id, type, content, metadata, status, attempt_count, next_attempt_at,
      telegram_deadline_at
    ) values (
      ${id}::uuid, 'prediction', ${content}, ${metadata}::jsonb, 'pending',
      ${opts.attemptCount ?? 0},
      now() - interval '1 millisecond',
      ${opts.deadlineAheadMs != null ? `now() + interval '${opts.deadlineAheadMs} milliseconds'` : null}
    )
    returning id
  `;
  return { id: rows[0]!.id, notificationId: id, content };
}

/** Remove rows this suite created so tests never see each other's leftovers. */
async function cleanSuiteRows(): Promise<void> {
  await (await getSql())`
    delete from notification_outbox where content like '[nw-behavior] %'
  `;
}

async function rowState(id: number): Promise<OutboxState> {
  const rows = await (await getSql())<OutboxState>`
    select status, attempt_count, delivered_at, last_error, next_attempt_at
    from notification_outbox where id = ${id}
  `;
  assert.ok(rows[0], `row ${id} must exist`);
  return rows[0]!;
}

test("outbox: 2xx transitions pending -> DELIVERED with delivered_at set", async () => {
  setTelegramEnv();
  await cleanSuiteRows();
  try {
    const { id, content } = await insertPendingRow();
    const d = new OutboxDispatcher();
    const sentBodies: string[] = [];
    const result = await withStubbedFetch(
      async (_url, body) => {
        sentBodies.push(body ?? "");
        return okTelegram();
      },
      () => d.tickOnce(),
    );
    assert.deepEqual(result, { recovered: 0, delivered: 1, dead: 0, requeued: 0 });
    assert.equal(sentBodies.length, 1, "exactly one Telegram POST");
    assert.ok(sentBodies[0]!.includes(content), "payload carries the outbox content");
    const s = await rowState(id);
    assert.equal(s.status, "delivered");
    assert.equal(s.attempt_count, 1);
    assert.ok(s.delivered_at != null, "delivered_at must be recorded");
    assert.equal(s.last_error, null);
    assert.equal(d.getStats().delivered, 1);

    // Claim idempotency: a second tick must not re-claim the delivered row.
    const second = await withStubbedFetch(
      async () => okTelegram(),
      () => d.tickOnce(),
    );
    assert.equal(second.delivered, 0);
    assert.equal(second.dead, 0);
  } finally {
    clearTelegramEnv();
  }
});

test("outbox: partial Telegram fan-out delivers once without retrying healthy chats", async () => {
  process.env.TELEGRAM_BOT_TOKEN = "123:test";
  process.env.TELEGRAM_CHAT_ID = "bad-chat";
  process.env.TELEGRAM_GROUP_CHAT_ID = "good-chat";
  await cleanSuiteRows();
  try {
    const { id } = await insertPendingRow();
    const d = new OutboxDispatcher();
    const result = await withStubbedFetch(
      async (_url, body) => {
        const payload = JSON.parse(body ?? "{}") as { chat_id?: string };
        return payload.chat_id === "good-chat"
          ? okTelegram()
          : failTelegram(400, "Bad Request: chat not found");
      },
      () => d.tickOnce(),
    );
    assert.deepEqual(result, { recovered: 0, delivered: 1, dead: 0, requeued: 0 });
    const s = await rowState(id);
    assert.equal(s.status, "delivered");
    assert.equal(s.attempt_count, 1);
    assert.ok(s.delivered_at != null);
  } finally {
    clearTelegramEnv();
  }
});

test("outbox: 4xx (non-429) transitions to DEAD immediately, no retry", async () => {
  setTelegramEnv();
  await cleanSuiteRows();
  try {
    const { id } = await insertPendingRow();
    const d = new OutboxDispatcher();
    const result = await withStubbedFetch(
      async () => failTelegram(400, "Bad Request: chat not found"),
      () => d.tickOnce(),
    );
    assert.deepEqual(result, { recovered: 0, delivered: 0, dead: 1, requeued: 0 });
    const s = await rowState(id);
    assert.equal(s.status, "dead_letter");
    assert.equal(s.attempt_count, 1, "exactly one attempt, then dead");
    assert.match(s.last_error ?? "", /Bad Request: chat not found/);
    assert.equal(d.getStats().dead, 1);
  } finally {
    clearTelegramEnv();
  }
});

test("outbox: 5xx requeues as pending with exponential backoff", async () => {
  setTelegramEnv();
  await cleanSuiteRows();
  try {
    const { id } = await insertPendingRow();
    const d = new OutboxDispatcher();
    const result = await withStubbedFetch(
      async () => failTelegram(500, "Internal Server Error"),
      () => d.tickOnce(),
    );
    assert.deepEqual(result, { recovered: 0, delivered: 0, dead: 0, requeued: 1 });
    const s = await rowState(id);
    assert.equal(s.status, "pending", "5xx must be retryable, not dead");
    assert.equal(s.attempt_count, 1);
    // Backoff = min(1000 * 2^(attempts-1), 60000) => 1000ms for attempt 1.
    const nextMs = new Date(s.next_attempt_at as string).getTime();
    assert.ok(nextMs > Date.now(), `next_attempt_at (${nextMs}) must be in the future`);
    assert.ok(nextMs <= Date.now() + 60_000, "backoff must respect the cap");
    assert.match(s.last_error ?? "", /Internal Server Error/);
  } finally {
    clearTelegramEnv();
  }
});

test("outbox: network failure requeues (operational miss is never permanent)", async () => {
  setTelegramEnv();
  await cleanSuiteRows();
  try {
    const { id } = await insertPendingRow();
    const d = new OutboxDispatcher();
    const result = await withStubbedFetch(
      async () => {
        throw new TypeError("fetch failed: ECONNREFUSED");
      },
      () => d.tickOnce(),
    );
    assert.deepEqual(result, { recovered: 0, delivered: 0, dead: 0, requeued: 1 });
    const s = await rowState(id);
    assert.equal(s.status, "pending", "network errors must retry, not dead-letter");
    assert.ok(s.attempt_count >= 1);
  } finally {
    clearTelegramEnv();
  }
});

test("outbox: attempts >= MAX_ATTEMPTS dead-letters even on retryable errors", async () => {
  setTelegramEnv();
  await cleanSuiteRows();
  try {
    // 4 prior attempts; claim increments to 5 == MAX_ATTEMPTS.
    const { id } = await insertPendingRow({ attemptCount: MAX_ATTEMPTS - 1 });
    const d = new OutboxDispatcher();
    const result = await withStubbedFetch(
      async () => failTelegram(500, "Internal Server Error"),
      () => d.tickOnce(),
    );
    assert.deepEqual(result, { recovered: 0, delivered: 0, dead: 1, requeued: 0 });
    const s = await rowState(id);
    assert.equal(s.status, "dead_letter");
    assert.equal(s.attempt_count, MAX_ATTEMPTS);
    assert.match(s.last_error ?? "", /Internal Server Error/);
    assert.equal(d.getStats().dead, 1);
  } finally {
    clearTelegramEnv();
  }
});

test("outbox: MAX_ATTEMPTS is 5", () => {
  assert.equal(MAX_ATTEMPTS, 5);
});

test("outbox: stale recovery resets rows older than STALE_INFLIGHT_MS to pending", async () => {
  await cleanSuiteRows();
  const sql = await getSql();
  const id = randomUUID();
  // The BEFORE UPDATE trigger forces updated_at = now(), so a follow-up
  // UPDATE can never backdate the row. Instead set updated_at directly at
  // INSERT time (defaults only apply when the column is omitted), making the
  // row appear stuck since 60s ago.
  await sql`
    insert into notification_outbox (
      notification_id, type, content, metadata, status, attempt_count, next_attempt_at,
      updated_at
    ) values (
      ${id}::uuid, 'prediction', '[nw-behavior] stuck', ${JSON.stringify({ predictionId: id })}::jsonb,
      'inflight', 1, now() - interval '60 seconds', now() - interval '60 seconds'
    )
  `;
  const d = new OutboxDispatcher();
  const recovered = await d.recoverStale();
  assert.ok(recovered >= 1, `expected at least 1 recovered, got ${recovered}`);
  const rows = await sql<{ status: string; last_error: string | null }>`
    select status, last_error from notification_outbox where notification_id = ${id}::uuid
  `;
  assert.equal(rows[0]?.status, "pending", "stale inflight must reset to pending");
  assert.ok((rows[0]?.last_error ?? "").includes("recovered from inflight"));
});

test("outbox: dispatcher stats start at zero", () => {
  const d = new OutboxDispatcher();
  const s = d.getStats();
  assert.equal(s.tickCount, 0);
  assert.equal(s.delivered, 0);
  assert.equal(s.dead, 0);
  assert.equal(s.requeued, 0);
});

test("outbox: start/stop is idempotent", async () => {
  const d = new OutboxDispatcher();
  await d.start();
  await d.start();
  await d.stop();
  await d.stop();
});
