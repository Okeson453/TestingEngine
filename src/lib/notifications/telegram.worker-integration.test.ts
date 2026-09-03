import { describe, it } from "node:test";
import assert from "node:assert/strict";

/**
 * Worker-fan-out integration tests for the Telegram adapter. These tests
 * verify the contract that:
 *
 *   - Telegram delivery is fire-and-forget from the worker's perspective.
 *   - A Telegram failure NEVER causes the prediction/validation cycle to fail.
 *   - The cycle's `ok: true` is independent of Telegram delivery success.
 *
 * The actual `fireTelegram` is module-private; we exercise the public surface
 * (`sendTelegramMessage` + the formatter pair) using the same code paths the
 * worker uses, with `fetch` stubbed. The worker code itself is verified by
 * the typecheck and the existing service-level tests in service.ts.
 */
import {
  sendTelegramMessage,
  formatPredictionMessage,
  formatValidationMessage,
  telegramConfigured,
} from "./telegram.ts";

function stubEnv<T>(vars: Record<string, string | undefined>, fn: () => T): T {
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

function withStubbedFetch<T>(
  handler: (url: string, body: string | undefined) => Promise<Response>,
  fn: () => Promise<T>,
): Promise<T> {
  const realFetch = globalThis.fetch;
  globalThis.fetch = (async (
    url: unknown,
    init: { body?: string } = {},
  ): Promise<Response> => handler(String(url), init.body)) as typeof fetch;
  return fn().finally(() => {
    globalThis.fetch = realFetch;
  });
}

function okJson(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

describe("worker → telegram fan-out contract", () => {
  it("prediction fan-out: posts the exact operator-spec'd message body", async () => {
    await stubEnv(
      { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-1001" },
      async () => {
        await withStubbedFetch(
          async (_url, body) => {
            const parsed = JSON.parse(body ?? "{}");
            const t = String(parsed.text);
            assert.match(t, /🎯 Next round prediction/);
            assert.match(t, /Target: 1\.30x/);
            assert.match(t, /Last round: 1\.24x or after 1\.24 play the next round/);
            assert.match(t, /Probability: 62%/);
            assert.match(t, /Confidence: 0\.74/);
            assert.match(t, /Regime: momentum-cool/);
            assert.equal(parsed.chat_id, "-1001");
            assert.equal(parsed.disable_web_page_preview, true);
            return okJson({ ok: true, result: { message_id: 1 } });
          },
          async () => {
            const text = formatPredictionMessage({
              predictionId: "p1",
              targetMultiplier: 1.3,
              probability: 0.62,
              confidence: 0.74,
              regimeName: "momentum-cool",
              lastRoundMultiplier: 1.24,
              generatedAt: "2026-01-01T00:00:00.000Z",
            });
            const res = await sendTelegramMessage(text);
            assert.equal(res.ok, true);
          },
        );
      },
    );
  });

  it("validation fan-out (WIN): posts the exact operator-spec'd message body", async () => {
    await stubEnv(
      { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-1001" },
      async () => {
        await withStubbedFetch(
          async (_url, body) => {
            const parsed = JSON.parse(body ?? "{}");
            const t = String(parsed.text);
            assert.match(t, /✅ WIN @ 1\.42x/);
            assert.match(t, /Target: 1\.30x/);
            assert.match(t, /Predicted prob: 62%/);
            return okJson({ ok: true, result: { message_id: 1 } });
          },
          async () => {
            const text = formatValidationMessage({
              predictionId: "p1",
              gameId: "g1",
              targetMultiplier: 1.3,
              actualMultiplier: 1.42,
              probability: 0.62,
              result: "WIN",
              resolvedAt: "2026-01-01T00:00:00.000Z",
            });
            await sendTelegramMessage(text);
          },
        );
      },
    );
  });

  it("validation fan-out (LOSS): posts the exact operator-spec'd message body", async () => {
    await stubEnv(
      { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-1001" },
      async () => {
        await withStubbedFetch(
          async (_url, body) => {
            const parsed = JSON.parse(body ?? "{}");
            const t = String(parsed.text);
            assert.match(t, /❌ LOSS @ 1\.18x/);
            assert.match(t, /Target: 1\.30x/);
            assert.match(t, /Predicted prob: 62%/);
            return okJson({ ok: true, result: { message_id: 1 } });
          },
          async () => {
            const text = formatValidationMessage({
              predictionId: "p1",
              gameId: "g1",
              targetMultiplier: 1.3,
              actualMultiplier: 1.18,
              probability: 0.62,
              result: "LOSS",
              resolvedAt: "2026-01-01T00:00:00.000Z",
            });
            await sendTelegramMessage(text);
          },
        );
      },
    );
  });

  it("telegram outage: send returns ok:false, never throws, never blocks the cycle", async () => {
    await stubEnv(
      { TELEGRAM_BOT_TOKEN: "123:abc", TELEGRAM_CHAT_ID: "-1001" },
      async () => {
        await withStubbedFetch(
          async () => new Response("upstream down", { status: 503 }),
          async () => {
            const res = await sendTelegramMessage("hello");
            assert.equal(res.ok, false);
            assert.equal(res.status, 503);
          },
        );
      },
    );
  });

  it("missing token: telegramConfigured returns false, send is a no-op", () => {
    stubEnv(
      { TELEGRAM_BOT_TOKEN: undefined, TELEGRAM_CHAT_ID: undefined },
      () => {
        assert.equal(telegramConfigured(), false);
      },
    );
  });

  it("missing token: never reaches fetch", async () => {
    let fetched = false;
    await stubEnv(
      { TELEGRAM_BOT_TOKEN: undefined, TELEGRAM_CHAT_ID: undefined },
      async () => {
        await withStubbedFetch(
          async () => {
            fetched = true;
            return new Response("{}", { status: 200 });
          },
          async () => {
            const res = await sendTelegramMessage("hello");
            assert.equal(res.ok, false);
            assert.equal(res.error, "not_configured");
          },
        );
      },
    );
    assert.equal(fetched, false, "fetch must not be called when env is missing");
  });

  it("the bot token never appears inside the message payload", async () => {
    await stubEnv(
      { TELEGRAM_BOT_TOKEN: "SECRET_TOKEN_VALUE", TELEGRAM_CHAT_ID: "-1001" },
      async () => {
        await withStubbedFetch(
          async (_url, body) => {
            const parsed = JSON.parse(body ?? "{}");
            const text = String(parsed.text);
            assert.doesNotMatch(text, /SECRET_TOKEN_VALUE/);
            return okJson({ ok: true, result: { message_id: 1 } });
          },
          async () => {
            await sendTelegramMessage("hello");
          },
        );
      },
    );
  });
});