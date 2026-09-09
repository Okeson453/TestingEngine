/**
 * BC.Game socket sign (p/t) — aligned with tested workspace sign.ts.
 * Node 24: never assign globalThis.navigator (read-only getter).
 *
 * Resilience:
 * - Reuse cached p/t past TTL when refresh fails (stale-ok)
 * - Retry transient fetch failures
 * - Never hard-block connect when a prior signature exists
 */
import { writeFile, unlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("bc-sign");

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";
const FALLBACK_WR = "wr_utils-BY40daAC.js";
/** Prefer fresh sign within this window. */
const SIGN_TTL_MS = Number(process.env.BCGAME_SIGN_TTL_MS ?? 60_000);
/** Allow stale cached sign up to this age when refresh fails. */
const SIGN_STALE_MAX_MS = Number(process.env.BCGAME_SIGN_STALE_MAX_MS ?? 10 * 60_000);

type SignUtils = {
  t1: (ua: string) => string;
  t2: (source: string, ua: string) => string;
};

type Signed = { p: string; t: string; ua: string; at: number };

let cachedUtils: SignUtils | null = null;
let cachedSign: Signed | null = null;
let inflight: Promise<Signed> | null = null;
let softFailUntil = 0;

function installDomPolyfill(): void {
  const loc = "https://bc.game/game/crash";
  const g = globalThis as typeof globalThis & {
    document?: { location: string };
    window?: typeof globalThis;
    self?: typeof globalThis;
  };
  g.document = { location: loc };
  g.window ??= g;
  g.self ??= g;
}

async function sleep(ms: number): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}

async function discoverWrUtilsUrl(): Promise<string> {
  const htmlRes = await fetch("https://bc.game/game/crash", {
    headers: {
      "user-agent": UA,
      accept: "text/html,application/xhtml+xml",
      "accept-language": "en",
    },
    signal: AbortSignal.timeout(12_000),
  });
  if (!htmlRes.ok) throw new Error(`crash page ${htmlRes.status}`);
  const html = await htmlRes.text();
  const indexMatch = html.match(/\/assets\/index-[^"']+\.js/);
  const indexPath = indexMatch?.[0] ?? "/assets/index-ChLSFpM-.js";
  const indexUrl = `https://bc.game${indexPath}`;
  // Validate URL before fetch to prevent crashes from malformed paths
  try {
    new URL(indexUrl);
  } catch {
    throw new Error(`invalid index URL constructed: ${indexUrl}`);
  }
  const jsRes = await fetch(indexUrl, {
    headers: { "user-agent": UA, "accept-language": "en" },
    signal: AbortSignal.timeout(15_000),
  });
  if (!jsRes.ok) throw new Error(`index bundle ${jsRes.status}`);
  const js = await jsRes.text();
  const wr = js.match(/wr_utils-[\w-]+\.js/)?.[0] ?? FALLBACK_WR;
  const wrUrl = `https://bc.game/assets/${wr}`;
  // Validate wr_utils URL too
  try {
    new URL(wrUrl);
  } catch {
    throw new Error(`invalid wr_utils URL constructed: ${wrUrl}`);
  }
  return wrUrl;
}

async function loadSignUtils(): Promise<SignUtils> {
  if (cachedUtils) return cachedUtils;
  installDomPolyfill();
  let lastErr: unknown;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const url = await discoverWrUtilsUrl();
      const res = await fetch(url, {
        headers: { "user-agent": UA, "accept-language": "en" },
        signal: AbortSignal.timeout(15_000),
      });
      if (!res.ok) throw new Error(`wr_utils fetch ${res.status}`);
      const body = await res.text();
      const file = join(tmpdir(), `te-wr-utils-${process.pid}-${attempt}.mjs`);
      await writeFile(file, body, "utf8");
      try {
        const mod = (await import(pathToFileURL(file).href)) as {
          default: Promise<SignUtils> | SignUtils;
        };
        const utils = await mod.default;
        if (typeof utils?.t1 !== "function" || typeof utils?.t2 !== "function") {
          throw new Error("wr_utils missing t1/t2");
        }
        cachedUtils = utils;
        logger.info({ url, attempt }, "wr_utils loaded");
        return cachedUtils;
      } finally {
        void unlink(file).catch(() => undefined);
      }
    } catch (e) {
      lastErr = e;
      logger.warn(
        { attempt, error: e instanceof Error ? e.message : String(e) },
        "wr_utils load attempt failed",
      );
      if (attempt < 3) await sleep(400 * attempt);
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

async function refreshSign(): Promise<Signed> {
  const utils = await loadSignUtils();
  const probe = utils.t1(UA);
  let lastErr: unknown;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const testRes = await fetch(
        `https://socketv4.bc.game/test/?p=${encodeURIComponent(probe)}`,
        {
          headers: {
            "user-agent": UA,
            origin: "https://bc.game",
            referer: "https://bc.game/game/crash",
            "accept-language": "en",
          },
          signal: AbortSignal.timeout(8_000),
        },
      );
      if (!testRes.ok) throw new Error(`/test/ ${testRes.status}`);
      const t = await testRes.text();
      if (!t) throw new Error("empty /test/ sign");
      const p = utils.t2(t, UA);
      return { p, t, ua: UA, at: Date.now() };
    } catch (e) {
      lastErr = e;
      if (attempt < 3) await sleep(300 * attempt);
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

function staleCached(): Signed | null {
  if (!cachedSign) return null;
  if (Date.now() - cachedSign.at > SIGN_STALE_MAX_MS) return null;
  return cachedSign;
}

export async function signSocketQuery(): Promise<{ p: string; t: string; ua: string }> {
  const envP = process.env.BCGAME_SOCKET_P;
  const envT = process.env.BCGAME_SOCKET_T;
  if (envP && envT) return { p: envP, t: envT, ua: UA };

  if (cachedSign && Date.now() - cachedSign.at < SIGN_TTL_MS) {
    return { p: cachedSign.p, t: cachedSign.t, ua: cachedSign.ua };
  }

  // Soft-fail window: still serve stale cache so reconnects don't die.
  if (Date.now() < softFailUntil) {
    const stale = staleCached();
    if (stale) {
      logger.info(
        { ageMs: Date.now() - stale.at },
        "using stale sign during soft-fail window",
      );
      return { p: stale.p, t: stale.t, ua: stale.ua };
    }
  }

  if (inflight) return inflight;

  inflight = (async () => {
    try {
      const signed = await refreshSign();
      cachedSign = signed;
      softFailUntil = 0;
      logger.info({}, "socket query signed");
      return { p: signed.p, t: signed.t, ua: signed.ua };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      softFailUntil = Date.now() + 5_000;
      const stale = staleCached();
      if (stale) {
        logger.warn(
          { error: msg, ageMs: Date.now() - stale.at },
          "sign refresh failed — using stale cache",
        );
        return { p: stale.p, t: stale.t, ua: stale.ua };
      }
      logger.warn({ error: msg }, "sign failed");
      throw new Error(`socket sign unavailable: ${msg}`);
    } finally {
      inflight = null;
    }
  })();

  return inflight;
}

export function prefetchSign(): void {
  if (cachedSign && Date.now() - cachedSign.at < SIGN_TTL_MS * 0.5) return;
  void signSocketQuery().catch(() => {});
}

export function prewarmSign(): void {
  void signSocketQuery().catch(() => {});
}
