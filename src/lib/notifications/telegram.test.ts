import { test } from "node:test";
import assert from "node:assert/strict";
import {
  formatPredictionMessage,
  formatValidationMessage,
  getConfiguredChatIds,
  sendTelegramMessage,
  telegramConfigured,
} from "./telegram.ts";

function withEnv<T>(vars: Record<string, string | undefined>, fn: () => T): T {
  const prev: Record<string, string | undefined> = {};
  for (const k of Object.keys(vars)) {
    prev[k] = process.env[k];
    if (vars[k] === undefined) delete process.env[k];
    else process.env[k] = vars[k] as string;
  }
  try {
    return fn();
  } finally {
    for (const k of Object.keys(prev)) {
      if (prev[k] === undefined) delete process.env[k];
      else process.env[k] = prev[k] as string;
    }
  }
}

test("telegramConfigured: false when token missing", () => {
  withEnv(
    { TELEGRAM_BOT_TOKEN: undefined, TELEGRAM_CHAT_ID: undefined },
    () => assert.equal(telegramConfigured(), false),
  );
  withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: undefined, TELEGRAM_GROUP_CHAT_ID: undefined, TELEGRAM_EXTRA_CHAT_IDS: undefined },
    () => assert.equal(telegramConfigured(), false),
  );
});

test("telegramConfigured: false when token present but no chat ids", () => {
  withEnv(
    {
      TELEGRAM_BOT_TOKEN: "123:abc",
      TELEGRAM_CHAT_ID: undefined,
      TELEGRAM_GROUP_CHAT_ID: undefined,
      TELEGRAM_EXTRA_CHAT_IDS: undefined,
    },
    () => assert.equal(telegramConfigured(), false),
  );
});

test("telegramConfigured: true when token + primary chat", () => {
  withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-100123", TELEGRAM_GROUP_CHAT_ID: undefined, TELEGRAM_EXTRA_CHAT_IDS: undefined },
    () => assert.equal(telegramConfigured(), true),
  );
});

test("telegramConfigured: true when token + group only (no primary)", () => {
  withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: undefined, TELEGRAM_GROUP_CHAT_ID: "-200456", TELEGRAM_EXTRA_CHAT_IDS: undefined },
    () => assert.equal(telegramConfigured(), true),
  );
});

test("telegramConfigured: true when token + extras only", () => {
  withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: undefined, TELEGRAM_GROUP_CHAT_ID: undefined, TELEGRAM_EXTRA_CHAT_IDS: "-300,-301" },
    () => assert.equal(telegramConfigured(), true),
  );
});

test("getConfiguredChatIds: primary + group + extras in order, deduped", () => {
  withEnv(
    {
      TELEGRAM_CHAT_ID: "100",
      TELEGRAM_GROUP_CHAT_ID: "-200",
      TELEGRAM_EXTRA_CHAT_IDS: "  -300 ,, -200 ,-301",
    },
    () => {
      assert.deepEqual(getConfiguredChatIds(), ["100", "-200", "-300", "-301"]);
    },
  );
});

test("getConfiguredChatIds: empty list when nothing set", () => {
  withEnv(
    {
      TELEGRAM_CHAT_ID: undefined,
      TELEGRAM_GROUP_CHAT_ID: undefined,
      TELEGRAM_EXTRA_CHAT_IDS: undefined,
    },
    () => assert.deepEqual(getConfiguredChatIds(), []),
  );
});

test("sendTelegramMessage: returns single not_configured result when env missing", async () => {
  await withEnv(
    {
      TELEGRAM_BOT_TOKEN: undefined,
      TELEGRAM_CHAT_ID: undefined,
      TELEGRAM_GROUP_CHAT_ID: undefined,
      TELEGRAM_EXTRA_CHAT_IDS: undefined,
    },
    async () => {
      const results = await sendTelegramMessage("hello");
      assert.equal(results.length, 1);
      assert.equal(results[0]!.ok, false);
      assert.equal(results[0]!.status, 0);
      assert.equal(results[0]!.error, "not_configured");
      assert.equal(results[0]!.chatId, "");
    },
  );
});

test("sendTelegramMessage: fans out to all configured chats in order", async () => {
  await withEnv(
    {
      TELEGRAM_BOT_TOKEN: "123:abc",
      TELEGRAM_CHAT_ID: "100",
      TELEGRAM_GROUP_CHAT_ID: "-200",
      TELEGRAM_EXTRA_CHAT_IDS: "-300,-301",
    },
    async () => {
      const realFetch = globalThis.fetch;
      const urls: string[] = [];
      const bodies: Array<Record<string, unknown>> = [];
      globalThis.fetch = (async (
        url: unknown,
        init: { body?: string } = {},
      ): Promise<Response> => {
        urls.push(String(url));
        bodies.push(JSON.parse(init.body ?? "{}"));
        return new Response(
          JSON.stringify({ ok: true, result: { message_id: urls.length } }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      }) as typeof fetch;
      try {
        const results = await sendTelegramMessage("hello");
        assert.equal(results.length, 4);
        for (const r of results) assert.equal(r.ok, true);
        assert.deepEqual(
          results.map((r) => r.chatId),
          ["100", "-200", "-300", "-301"],
        );
        // Each fetch body has the message + no-link-preview; the set of
        // chat_ids the API was called with must equal the configured set.
        // (bodies is appended in fetch-completion order, which is non-
        // deterministic, so we assert on the SET of chat_ids, not their
        // order in `bodies`.)
        for (const b of bodies) {
          assert.equal(b.text, "hello");
          assert.equal(b.disable_web_page_preview, true);
        }
        assert.deepEqual(
          bodies.map((b) => String(b.chat_id)).sort(),
          ["-200", "-300", "-301", "100"],
        );
        // 4 distinct URL fetches, all using the same token
        assert.equal(urls.length, 4);
        for (const u of urls) assert.match(u, /api\.telegram\.org\/bot123:abc\/sendMessage/);
      } finally {
        globalThis.fetch = realFetch;
      }
    },
  );
});

test("sendTelegramMessage: never throws on malformed upstream body", async () => {
  await withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-100123", TELEGRAM_GROUP_CHAT_ID: undefined, TELEGRAM_EXTRA_CHAT_IDS: undefined },
    async () => {
      const realFetch = globalThis.fetch;
      globalThis.fetch = (async () =>
        new Response("not-json", { status: 200 })) as typeof fetch;
      try {
        const results = await sendTelegramMessage("hello");
        assert.equal(results.length, 1);
        assert.equal(results[0]!.ok, false);
        assert.equal(results[0]!.status, 200);
        assert.equal(results[0]!.error, "malformed_response");
        assert.equal(results[0]!.chatId, "-100123");
      } finally {
        globalThis.fetch = realFetch;
      }
    },
  );
});

test("sendTelegramMessage: returns ok:true per chat on Telegram success envelope", async () => {
  await withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-100123", TELEGRAM_GROUP_CHAT_ID: undefined, TELEGRAM_EXTRA_CHAT_IDS: undefined },
    async () => {
      const realFetch = globalThis.fetch;
      globalThis.fetch = (async () =>
        new Response(JSON.stringify({ ok: true, result: { message_id: 1 } }), {
          status: 200,
        })) as typeof fetch;
      try {
        const results = await sendTelegramMessage("hello");
        assert.equal(results.length, 1);
        assert.equal(results[0]!.ok, true);
        assert.equal(results[0]!.status, 200);
        assert.equal(results[0]!.chatId, "-100123");
      } finally {
        globalThis.fetch = realFetch;
      }
    },
  );
});

test("sendTelegramMessage: captures Telegram description per chat on 4xx", async () => {
  await withEnv(
    { TELEGRAM_BOT_TOKEN: "bad", TELEGRAM_CHAT_ID: "-100123", TELEGRAM_GROUP_CHAT_ID: undefined, TELEGRAM_EXTRA_CHAT_IDS: undefined },
    async () => {
      const realFetch = globalThis.fetch;
      globalThis.fetch = (async () =>
        new Response(
          JSON.stringify({ ok: false, description: "chat not found" }),
          { status: 400 },
        )) as typeof fetch;
      try {
        const results = await sendTelegramMessage("hello");
        assert.equal(results.length, 1);
        assert.equal(results[0]!.ok, false);
        assert.equal(results[0]!.status, 400);
        assert.equal(results[0]!.error, "chat not found");
        assert.equal(results[0]!.chatId, "-100123");
      } finally {
        globalThis.fetch = realFetch;
      }
    },
  );
});

test("sendTelegramMessage: per-chat timeout produces timeout_2000ms", async () => {
  await withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-100123", TELEGRAM_GROUP_CHAT_ID: undefined, TELEGRAM_EXTRA_CHAT_IDS: undefined },
    async () => {
      const realFetch = globalThis.fetch;
      globalThis.fetch = ((_url: unknown, init: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          init.signal?.addEventListener("abort", () => {
            const err = new Error("aborted");
            (err as { name?: string }).name = "AbortError";
            reject(err);
          });
        })) as typeof fetch;
      try {
        const results = await sendTelegramMessage("hello");
        assert.equal(results.length, 1);
        assert.equal(results[0]!.ok, false);
        assert.equal(results[0]!.error, "timeout_2000ms");
        assert.equal(results[0]!.chatId, "-100123");
      } finally {
        globalThis.fetch = realFetch;
      }
    },
  );
});

test("sendTelegramMessage: never throws on per-chat network failure", async () => {
  await withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-100123", TELEGRAM_GROUP_CHAT_ID: undefined, TELEGRAM_EXTRA_CHAT_IDS: undefined },
    async () => {
      const realFetch = globalThis.fetch;
      globalThis.fetch = (async () => {
        throw new Error("ECONNREFUSED");
      }) as typeof fetch;
      try {
        const results = await sendTelegramMessage("hello");
        assert.equal(results.length, 1);
        assert.equal(results[0]!.ok, false);
        assert.equal(results[0]!.error, "ECONNREFUSED");
        assert.equal(results[0]!.chatId, "-100123");
      } finally {
        globalThis.fetch = realFetch;
      }
    },
  );
});

test("sendTelegramMessage: per-chat independence — one bad chat does not block others", async () => {
  await withEnv(
    {
      TELEGRAM_BOT_TOKEN: "123:abc",
      TELEGRAM_CHAT_ID: "100",
      TELEGRAM_GROUP_CHAT_ID: "-200",
      TELEGRAM_EXTRA_CHAT_IDS: undefined,
    },
    async () => {
      const realFetch = globalThis.fetch;
      globalThis.fetch = (async (
        _url: unknown,
        init: { body?: string } = {},
      ): Promise<Response> => {
        const parsed = JSON.parse(init.body ?? "{}");
        if (String(parsed.chat_id) === "100") {
          return new Response(
            JSON.stringify({ ok: false, description: "chat not found" }),
            { status: 400, headers: { "content-type": "application/json" } },
          );
        }
        return new Response(
          JSON.stringify({ ok: true, result: { message_id: 1 } }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      }) as typeof fetch;
      try {
        const results = await sendTelegramMessage("hello");
        assert.equal(results.length, 2);
        const byChat = new Map(results.map((r) => [r.chatId, r]));
        assert.equal(byChat.get("100")!.ok, false);
        assert.equal(byChat.get("100")!.error, "chat not found");
        assert.equal(byChat.get("-200")!.ok, true);
      } finally {
        globalThis.fetch = realFetch;
      }
    },
  );
});

test("formatPredictionMessage: contains target, last round, probability, confidence, regime", () => {
  const msg = formatPredictionMessage({
    predictionId: "p1",
    targetMultiplier: 1.3,
    probability: 0.62,
    confidence: 0.74,
    regimeName: "momentum-cool",
    lastRoundMultiplier: 1.24,
    generatedAt: "2026-01-01T00:00:00.000Z",
  });
  assert.match(msg, /🎯 Next round prediction/);
  assert.match(msg, /Target: 1\.30x/);
  assert.match(msg, /Last round: 1\.24x or after 1\.24 play the next round/);
  assert.match(msg, /Probability: 62%/);
  assert.match(msg, /Confidence: 0\.74/);
  assert.match(msg, /Regime: momentum-cool/);
});

test("formatPredictionMessage: cold-start fallback when no last round", () => {
  const msg = formatPredictionMessage({
    predictionId: "p1",
    targetMultiplier: 1.3,
    probability: 0.5,
    confidence: 0.5,
    regimeName: null,
    lastRoundMultiplier: null,
    generatedAt: "2026-01-01T00:00:00.000Z",
  });
  assert.match(msg, /Last round: \(no recent round in DB yet\)/);
  assert.match(msg, /Regime: n\/a/);
});

test("formatValidationMessage: WIN shape", () => {
  const msg = formatValidationMessage({
    predictionId: "p1",
    gameId: "g1",
    targetMultiplier: 1.3,
    actualMultiplier: 1.42,
    probability: 0.62,
    result: "WIN",
    resolvedAt: "2026-01-01T00:00:00.000Z",
  });
  assert.match(msg, /✅ WIN @ 1\.42x/);
  assert.match(msg, /Target: 1\.30x/);
  assert.match(msg, /Predicted prob: 62%/);
});

test("formatValidationMessage: LOSS shape", () => {
  const msg = formatValidationMessage({
    predictionId: "p1",
    gameId: "g1",
    targetMultiplier: 1.3,
    actualMultiplier: 1.18,
    probability: 0.62,
    result: "LOSS",
    resolvedAt: "2026-01-01T00:00:00.000Z",
  });
  assert.match(msg, /❌ LOSS @ 1\.18x/);
  assert.match(msg, /Target: 1\.30x/);
  assert.match(msg, /Predicted prob: 62%/);
});

test("formatPredictionMessage: never exposes env or secrets", () => {
  withEnv(
    { TELEGRAM_BOT_TOKEN: "SECRET_TOKEN_VALUE", TELEGRAM_CHAT_ID: "-1001", TELEGRAM_GROUP_CHAT_ID: undefined, TELEGRAM_EXTRA_CHAT_IDS: undefined },
    () => {
      const msg = formatPredictionMessage({
        predictionId: "p1",
        targetMultiplier: 1.3,
        probability: 0.5,
        confidence: 0.5,
        regimeName: null,
        lastRoundMultiplier: 1.0,
        generatedAt: "2026-01-01T00:00:00.000Z",
      });
      assert.doesNotMatch(msg, /SECRET_TOKEN_VALUE/);
      assert.doesNotMatch(msg, /-1001/);
    },
  );
});