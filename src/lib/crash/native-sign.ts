/**
 * BC.Game socket sign (p/t) — aligned with tested workspace sign.ts.
 * Node 24 exposes globalThis.navigator as a read-only getter; never assign it.
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
const SIGN_TTL_MS = 25_000;

type SignUtils = {
  t1: (ua: string) => string;
  t2: (source: string, ua: string) => string;
};

type Signed = { p: string; t: string; ua: string; at: number };

let cachedUtils: SignUtils | null = null;
let cachedSign: Signed | null = null;
let inflight: Promise<Signed> | null = null;
let disabledUntil = 0;

/** Minimal DOM polyfill — do NOT touch navigator (getter-only on Node 24+). */
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

async function discoverWrUtilsUrl(): Promise<string> {
  const htmlRes = await fetch("https://bc.game/game/crash", {
    headers: { "user-agent": UA, accept: "text/html" },
    signal: AbortSignal.timeout(10_000),
  });
  if (!htmlRes.ok) throw new Error(`crash page ${htmlRes.status}`);
  const html = await htmlRes.text();
  const indexMatch = html.match(/\/assets\/index-[^"']+\.js/);
  const indexPath = indexMatch?.[0] ?? "/assets/index-ChLSFpM-.js";
  const jsRes = await fetch(`https://bc.game${indexPath}`, {
    headers: { "user-agent": UA },
    signal: AbortSignal.timeout(15_000),
  });
  if (!jsRes.ok) throw new Error(`index bundle ${jsRes.status}`);
  const js = await jsRes.text();
  const wr = js.match(/wr_utils-[\w-]+\.js/)?.[0] ?? FALLBACK_WR;
  return `https://bc.game/assets/${wr}`;
}

async function loadSignUtils(): Promise<SignUtils> {
  if (cachedUtils) return cachedUtils;
  installDomPolyfill();
  const url = await discoverWrUtilsUrl();
  const res = await fetch(url, {
    headers: { "user-agent": UA },
    signal: AbortSignal.timeout(15_000),
  });
  if (!res.ok) throw new Error(`wr_utils fetch ${res.status}`);
  const body = await res.text();
  const file = join(tmpdir(), `te-wr-utils-${process.pid}.mjs`);
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
    logger.info({ url }, "wr_utils loaded");
    return cachedUtils;
  } finally {
    void unlink(file).catch(() => undefined);
  }
}

export async function signSocketQuery(): Promise<{ p: string; t: string; ua: string }> {
  const envP = process.env.BCGAME_SOCKET_P;
  const envT = process.env.BCGAME_SOCKET_T;
  if (envP && envT) return { p: envP, t: envT, ua: UA };

  if (Date.now() < disabledUntil) {
    throw new Error("socket sign unavailable");
  }
  if (cachedSign && Date.now() - cachedSign.at < SIGN_TTL_MS) {
    return { p: cachedSign.p, t: cachedSign.t, ua: cachedSign.ua };
  }
  if (inflight) return inflight;

  inflight = (async () => {
    try {
      const utils = await loadSignUtils();
      const probe = utils.t1(UA);
      const testRes = await fetch(
        `https://socketv4.bc.game/test/?p=${encodeURIComponent(probe)}`,
        {
          headers: {
            "user-agent": UA,
            origin: "https://bc.game",
            referer: "https://bc.game/game/crash",
          },
          signal: AbortSignal.timeout(8_000),
        },
      );
      if (!testRes.ok) throw new Error(`/test/ ${testRes.status}`);
      const t = await testRes.text();
      if (!t) throw new Error("empty /test/ sign");
      const p = utils.t2(t, UA);
      cachedSign = { p, t, ua: UA, at: Date.now() };
      logger.info({}, "socket query signed");
      return { p, t, ua: UA };
    } catch (err) {
      disabledUntil = Date.now() + 15_000;
      logger.warn(
        { error: err instanceof Error ? err.message : String(err) },
        "sign failed",
      );
      throw new Error(
        `socket sign unavailable: ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      inflight = null;
    }
  })();

  return inflight;
}

export function prefetchSign(): void {
  if (Date.now() < disabledUntil) return;
  if (cachedSign && Date.now() - cachedSign.at < SIGN_TTL_MS * 0.6) return;
  void signSocketQuery().catch(() => {});
}

export function prewarmSign(): void {
  void signSocketQuery().catch(() => {});
}
