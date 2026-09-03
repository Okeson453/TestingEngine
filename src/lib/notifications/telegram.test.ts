import { test } from "node:test";
import assert from "node:assert/strict";
import {
  formatPredictionMessage,
  formatValidationMessage,
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

test("telegramConfigured: false when either env var is missing", () => {
  withEnv(
    { TELEGRAM_BOT_TOKEN: undefined, TELEGRAM_CHAT_ID: undefined },
    () => assert.equal(telegramConfigured(), false),
  );
  withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: undefined },
    () => assert.equal(telegramConfigured(), false),
  );
  withEnv(
    { TELEGRAM_BOT_TOKEN: undefined, TELEGRAM_CHAT_ID: "-100123" },
    () => assert.equal(telegramConfigured(), false),
  );
});

test("telegramConfigured: true when both env vars are present", () => {
  withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-100123" },
    () => assert.equal(telegramConfigured(), true),
  );
});

test("sendTelegramMessage: returns not_configured when env missing", async () => {
  await withEnv(
    { TELEGRAM_BOT_TOKEN: undefined, TELEGRAM_CHAT_ID: undefined },
    async () => {
      const r = await sendTelegramMessage("hello");
      assert.equal(r.ok, false);
      assert.equal(r.status, 0);
      assert.equal(r.error, "not_configured");
    },
  );
});

test("sendTelegramMessage: never throws on malformed upstream body", async () => {
  await withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-100123" },
    async () => {
      const realFetch = globalThis.fetch;
      globalThis.fetch = (async () =>
        new Response("not-json", { status: 200 })) as typeof fetch;
      try {
        const r = await sendTelegramMessage("hello");
        assert.equal(r.ok, false);
        assert.equal(r.status, 200);
        assert.equal(r.error, "malformed_response");
      } finally {
        globalThis.fetch = realFetch;
      }
    },
  );
});

test("sendTelegramMessage: returns ok:true on Telegram success envelope", async () => {
  await withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-100123" },
    async () => {
      const realFetch = globalThis.fetch;
      globalThis.fetch = (async () =>
        new Response(JSON.stringify({ ok: true, result: { message_id: 1 } }), {
          status: 200,
        })) as typeof fetch;
      try {
        const r = await sendTelegramMessage("hello");
        assert.equal(r.ok, true);
        assert.equal(r.status, 200);
      } finally {
        globalThis.fetch = realFetch;
      }
    },
  );
});

test("sendTelegramMessage: captures Telegram description on 4xx", async () => {
  await withEnv(
    { TELEGRAM_BOT_TOKEN: "bad", TELEGRAM_CHAT_ID: "-100123" },
    async () => {
      const realFetch = globalThis.fetch;
      globalThis.fetch = (async () =>
        new Response(
          JSON.stringify({ ok: false, description: "chat not found" }),
          { status: 400 },
        )) as typeof fetch;
      try {
        const r = await sendTelegramMessage("hello");
        assert.equal(r.ok, false);
        assert.equal(r.status, 400);
        assert.equal(r.error, "chat not found");
      } finally {
        globalThis.fetch = realFetch;
      }
    },
  );
});

test("sendTelegramMessage: timeout produces timeout_5000ms error", async () => {
  await withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-100123" },
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
        const r = await sendTelegramMessage("hello");
        assert.equal(r.ok, false);
        assert.equal(r.error, "timeout_5000ms");
      } finally {
        globalThis.fetch = realFetch;
      }
    },
  );
});

test("sendTelegramMessage: never throws on network failure", async () => {
  await withEnv(
    { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-100123" },
    async () => {
      const realFetch = globalThis.fetch;
      globalThis.fetch = (async () => {
        throw new Error("ECONNREFUSED");
      }) as typeof fetch;
      try {
        const r = await sendTelegramMessage("hello");
        assert.equal(r.ok, false);
        assert.equal(r.error, "ECONNREFUSED");
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
    { TELEGRAM_BOT_TOKEN: "SECRET_TOKEN_VALUE", TELEGRAM_CHAT_ID: "-1001" },
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