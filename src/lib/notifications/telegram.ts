/**
 * Telegram push-delivery adapter for the BcTracker prediction worker.
 *
 * Server-only. Reads `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` from
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
 */

const TELEGRAM_API = "https://api.telegram.org";
const SEND_TIMEOUT_MS = 5_000;

export type SendResult = {
  ok: boolean;
  status: number;
  error?: string;
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

/** Whether both Telegram env vars are present (token + chat id). */
export function telegramConfigured(): boolean {
  return Boolean(readEnv("TELEGRAM_BOT_TOKEN")) && Boolean(readEnv("TELEGRAM_CHAT_ID"));
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
 * Low-level: send arbitrary text to the configured chat. Never throws —
 * every failure mode (missing env, timeout, HTTP error, malformed body,
 * network failure) is captured into the returned `SendResult`.
 */
export async function sendTelegramMessage(text: string): Promise<SendResult> {
  const token = readEnv("TELEGRAM_BOT_TOKEN");
  const chatId = readEnv("TELEGRAM_CHAT_ID");
  if (!token || !chatId) {
    return { ok: false, status: 0, error: "not_configured" };
  }

  const url = `${TELEGRAM_API}/bot${token}/sendMessage`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), SEND_TIMEOUT_MS);
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
      return { ok: false, status: response.status, error: "malformed_response" };
    }
    if (response.ok && isTelegramOk(parsed)) {
      return { ok: true, status: response.status };
    }
    const description =
      isRecord(parsed) && typeof parsed.description === "string"
        ? parsed.description
        : `http_${response.status}`;
    return { ok: false, status: response.status, error: description };
  } catch (e: unknown) {
    const name = (e as { name?: string })?.name;
    if (name === "AbortError") {
      return { ok: false, status: 0, error: `timeout_${SEND_TIMEOUT_MS}ms` };
    }
    return { ok: false, status: 0, error: (e as Error)?.message ?? "network_error" };
  } finally {
    clearTimeout(timer);
  }
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function isTelegramOk(parsed: unknown): boolean {
  return isRecord(parsed) && parsed.ok === true;
}