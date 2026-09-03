import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("notifications:telegram");

const TELEGRAM_API_BASE = "https://api.telegram.org/bot";
const SEND_TIMEOUT_MS = 5_000;

let warnedNotConfigured = false;

export interface SendResult {
  ok: boolean;
  status: number;
  error?: string;
}

export interface PredictionForMessage {
  predictionId: string;
  targetMultiplier: number;
  probability: number;
  confidence: number;
  regimeName: string | null;
  lastRoundMultiplier: number | null;
  generatedAt: string;
}

export interface ValidationForMessage {
  predictionId: string;
  gameId: string;
  targetMultiplier: number;
  actualMultiplier: number;
  probability: number;
  result: "WIN" | "LOSS";
  resolvedAt: string;
}

export function telegramConfigured(): boolean {
  return Boolean(process.env.TELEGRAM_BOT_TOKEN) && Boolean(process.env.TELEGRAM_CHAT_ID);
}

function readEnv(): { token: string | null; chatId: string | null } {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  return {
    token: typeof token === "string" ? token : null,
    chatId: typeof chatId === "string" ? chatId : null,
  };
}

export async function sendTelegramMessage(text: string): Promise<SendResult> {
  const { token, chatId } = readEnv();
  if (!token || !chatId) {
    if (!warnedNotConfigured) {
      warnedNotConfigured = true;
      logger.debug(
        { component: "Telegram" },
        "Telegram not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing); notifications disabled",
      );
    }
    return { ok: false, status: 0, error: "not_configured" };
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), SEND_TIMEOUT_MS);
  try {
    const res = await fetch(`${TELEGRAM_API_BASE}${token}/sendMessage`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        chat_id: chatId,
        text,
        disable_web_page_preview: true,
      }),
      signal: controller.signal,
    });

    const status = res.status;
    let parsed: { ok?: boolean; description?: string } = {};
    try {
      parsed = (await res.json()) as { ok?: boolean; description?: string };
    } catch {
      // Malformed/empty body — fall through; we still have the HTTP status.
    }

    if (res.ok && parsed.ok !== false) {
      return { ok: true, status };
    }
    const error = parsed.description ?? `HTTP ${status}`;
    logger.warn(
      { component: "Telegram", status, error },
      "Telegram send failed",
    );
    return { ok: false, status, error };
  } catch (e: unknown) {
    const aborted =
      e instanceof DOMException && e.name === "AbortError";
    const error = aborted
      ? `timeout_${SEND_TIMEOUT_MS}ms`
      : (e instanceof Error ? e.message : String(e));
    logger.warn({ component: "Telegram", error }, "Telegram send failed");
    return { ok: false, status: 0, error };
  } finally {
    clearTimeout(timer);
  }
}

function fmtMultiplier(n: number): string {
  return Number.isFinite(n) ? n.toFixed(2) : String(n);
}

function fmtPercent(p: number): string {
  return Math.round(p * 100).toString();
}

export function formatPredictionMessage(p: PredictionForMessage): string {
  const lastRound =
    p.lastRoundMultiplier == null
      ? "(no recent round in DB yet)"
      : `${fmtMultiplier(p.lastRoundMultiplier)}x or after ${fmtMultiplier(p.lastRoundMultiplier)} play the next round`;
  return [
    "🎯 Next round prediction",
    `Target: ${fmtMultiplier(p.targetMultiplier)}x`,
    `Last round: ${lastRound}`,
    `Probability: ${fmtPercent(p.probability)}%`,
    `Confidence: ${String(p.confidence)}`,
    `Regime: ${p.regimeName ?? "unknown"}`,
  ].join("\n");
}

export function formatValidationMessage(v: ValidationForMessage): string {
  const emoji = v.result === "WIN" ? "✅" : "❌";
  return [
    `${emoji} ${v.result} @ ${fmtMultiplier(v.actualMultiplier)}x`,
    `Target: ${fmtMultiplier(v.targetMultiplier)}x`,
    `Predicted prob: ${fmtPercent(v.probability)}%`,
  ].join("\n");
}
