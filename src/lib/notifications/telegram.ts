/**
 * Telegram push-delivery adapter for the BcTracker prediction worker.
 *
 * Server-only. Reads the Telegram bot token and the chat-id list from
 * `process.env` at the moment of send so Railway env-var changes take effect
 * on the next send without a redeploy. Plain text only — no Markdown/HTML,
 * no bot commands, no WebSocket. Fire-and-forget from the caller's perspective:
 * the function never throws, never awaits anything inside the cycle, and never
 * leaks credentials to logs.
 *
 * This module is the ONLY place in the codebase that constructs a request to
 * `api.telegram.org`. The worker treats Telegram strictly as a notification
 * surface — a Telegram outage cannot block, terminate, or corrupt the
 * prediction/validation lifecycle.
 *
 * Multi-destination delivery (env-driven, server-only):
 *   TELEGRAM_BOT_TOKEN         (required)  Bot API token from @BotFather
 *   TELEGRAM_CHAT_ID           (required*) Primary 1:1 chat id
 *   TELEGRAM_GROUP_CHAT_ID     (optional)  Single group/supergroup/channel id
 *   TELEGRAM_EXTRA_CHAT_IDS    (optional)  Comma-separated list of additional
 *                                            chat ids (e.g. multiple groups
 *                                            or alert channels)
 *   * The "configured" check requires the token AND at least one of
 *     TELEGRAM_CHAT_ID, TELEGRAM_GROUP_CHAT_ID, or TELEGRAM_EXTRA_CHAT_IDS.
 *     Every message is fanned out to all configured chat ids; each send is
 *     independent — a failure to one chat never affects the others.
 */

const TELEGRAM_API = "https://api.telegram.org";
const SEND_TIMEOUT_MS = 5_000;

export type SendResult = {
  ok: boolean;
  status: number;
  error?: string;
  chatId: string;
};

/** Narrow shape passed to `formatPredictionMessage`. */
export interface PredictionForMessage {
  predictionId: string;
  targetMultiplier: number;
  probability: number;
  confidence: number;
  regimeName: string | null;
  lastRoundMultiplier: number | null;
  generatedAt: string;
}

/** Narrow shape passed to `formatValidationMessage`. */
export interface ValidationForMessage {
  predictionId: string;
  gameId: string;
  targetMultiplier: number;
  actualMultiplier: number;
  probability: number;
  result: "WIN" | "LOSS";
  resolvedAt: string;
}

function readEnv(name: string): string {
  if (typeof process === "undefined") return "";
  const v = process.env[name];
  return typeof v === "string" ? v : "";
}

/**
 * Parse a comma-separated list of chat ids, trim each, drop empties, preserve
 * order, dedupe. Returns the list as a new array. Defensive against
 * `",, ,123,123,  -100 ,,"` style operator misconfig.
 */
function parseChatList(raw: string): string[] {
  if (!raw) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const part of raw.split(",")) {
    const t = part.trim();
    if (!t) continue;
    if (seen.has(t)) continue;
    seen.add(t);
    out.push(t);
  }
  return out;
}

/**
 * The ordered, deduped list of chat ids the bot will fan out to on every
 * send. Order: primary, then group, then extras (each in input order). The
 * worker does not care about order — Telegram's API is independent per chat.
 */
export function getConfiguredChatIds(): string[] {
  return parseChatList(
    [
      readEnv("TELEGRAM_CHAT_ID"),
      readEnv("TELEGRAM_GROUP_CHAT_ID"),
      readEnv("TELEGRAM_EXTRA_CHAT_IDS"),
    ].join(","),
  );
}

/** Whether the bot is configured (token present + at least one chat id). */
export function telegramConfigured(): boolean {
  return Boolean(readEnv("TELEGRAM_BOT_TOKEN")) && getConfiguredChatIds().length > 0;
}

/** Format the multiplier the operator asked for: `1.30x`. */
function fmtMult(n: number): string {
  const v = Number.isFinite(n) ? n : 0;
  return `${v.toFixed(2)}x`;
}

/** Format probability as an integer percent. */
function fmtProb(p: number): string {
  const pct = Math.round((Number.isFinite(p) ? p : 0) * 100);
  return `${pct}%`;
}

/** Format confidence as a 2-decimal number. */
function fmtConfidence(c: number): string {
  const v = Number.isFinite(c) ? c : 0;
  return v.toFixed(2);
}

export function formatPredictionMessage(p: PredictionForMessage): string {
  const lastLine =
    p.lastRoundMultiplier != null
      ? `Last round: ${fmtMult(p.lastRoundMultiplier)} or after ${fmtMult(p.lastRoundMultiplier).replace(/x$/, "")} play the next round`
      : "Last round: (no recent round in DB yet)";
  return [
    "🎯 Next round prediction",
    `Target: ${fmtMult(p.targetMultiplier)}`,
    lastLine,
    `Probability: ${fmtProb(p.probability)}`,
    `Confidence: ${fmtConfidence(p.confidence)}`,
    `Regime: ${p.regimeName ?? "n/a"}`,
  ].join("\n");
}

export function formatValidationMessage(v: ValidationForMessage): string {
  const icon = v.result === "WIN" ? "✅ WIN" : "❌ LOSS";
  return [
    `${icon} @ ${fmtMult(v.actualMultiplier)}`,
    `Target: ${fmtMult(v.targetMultiplier)}`,
    `Predicted prob: ${fmtProb(v.probability)}`,
  ].join("\n");
}

/**
 * Low-level: send a single message to a single chat id. Never throws.
 * Every failure mode (missing token, timeout, HTTP error, malformed body,
 * network failure) is captured into the returned `SendResult` and tagged
 * with the destination chat id so the caller can record per-chat health.
 */
async function sendToChat(
  token: string,
  chatId: string,
  text: string,
  timeoutMs: number = SEND_TIMEOUT_MS,
): Promise<SendResult> {
  const url = `${TELEGRAM_API}/bot${token}/sendMessage`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        chat_id: chatId,
        text,
        disable_web_page_preview: true,
      }),
      signal: controller.signal,
    });
    let parsed: unknown = null;
    try {
      parsed = await response.json();
    } catch {
      return { ok: false, status: response.status, error: "malformed_response", chatId };
    }
    if (response.ok && isTelegramOk(parsed)) {
      return { ok: true, status: response.status, chatId };
    }
    const description =
      isRecord(parsed) && typeof parsed.description === "string"
        ? parsed.description
        : `http_${response.status}`;
    return { ok: false, status: response.status, error: description, chatId };
  } catch (e: unknown) {
    const name = (e as { name?: string })?.name;
    if (name === "AbortError") {
      return { ok: false, status: 0, error: `timeout_${timeoutMs}ms`, chatId };
    }
    return { ok: false, status: 0, error: (e as Error)?.message ?? "network_error", chatId };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Fan out a message to every configured chat id. Returns one `SendResult`
 * per destination, in the same order as `getConfiguredChatIds()`. When the
 * bot is not configured, returns a single `not_configured` result tagged
 * with the empty chat id so callers can record the no-op without crashing.
 *
 * Never throws. Each destination is an independent `AbortController`-bound
 * fetch, so a slow or failing chat never delays or blocks another.
 */
export async function sendTelegramMessage(
  text: string,
  options?: { timeout?: number },
): Promise<SendResult[]> {
  const token = readEnv("TELEGRAM_BOT_TOKEN");
  const chatIds = getConfiguredChatIds();
  const timeoutMs = options?.timeout ?? SEND_TIMEOUT_MS;
  if (!token || chatIds.length === 0) {
    return [{ ok: false, status: 0, error: "not_configured", chatId: "" }];
  }
  return Promise.all(
    chatIds.map((chatId) => sendToChat(token, chatId, text, timeoutMs)),
  );
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function isTelegramOk(parsed: unknown): boolean {
  return isRecord(parsed) && parsed.ok === true;
}