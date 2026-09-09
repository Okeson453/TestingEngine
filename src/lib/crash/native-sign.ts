import { spawn } from "node:child_process";
import { writeFile, unlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { getLogger } from "@/lib/observability/logger";

const log = {
  info: (c: string, m: string, extra?: Record<string, unknown>) =>
    getLogger(c).info(extra ?? {}, m),
  warn: (c: string, m: string, extra?: Record<string, unknown>) =>
    getLogger(c).warn(extra ?? {}, m),
};

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";
const SIGN_TTL_MS = 25_000;
const FALLBACK_WR = "wr_utils-BY40daAC.js";

type Signed = { p: string; t: string; ua: string; at: number };
type SignUtils = { t1: (ua: string) => string; t2: (source: string, ua: string) => string };

let cachedSign: Signed | null = null;
let inflight: Promise<Signed> | null = null;
let disabledUntil = 0;

function installDomPolyfill(): void {
  const loc = "https://bc.game/game/crash";
  const g = globalThis as Record<string, unknown>;
  if (!g.location) {
    g.location = { href: loc, toString() { return loc; }, ancestorOrigins: [] };
  }
  if (!g.document) {
    g.document = {
      location: g.location,
      addEventListener() {},
      removeEventListener() {},
      createElement() { return { setAttribute() {}, style: {} }; },
      body: { appendChild() {} },
      head: { appendChild() {} },
      getElementsByTagName() { return []; },
    };
  }
  if (!g.window) g.window = g;
  if (!g.self) g.self = g;
  if (!g.navigator) g.navigator = { userAgent: UA };
}

async function discoverWrUtilsUrl(): Promise<string> {
  const page = await fetch("https://bc.game/game/crash", {
    headers: { "user-agent": UA, accept: "text/html" },
    signal: AbortSignal.timeout(10_000),
  });
  if (!page.ok) throw new Error(`crash page ${page.status}`);
  const html = await page.text();
  const asset =
    html.match(/\/assets\/index-[A-Za-z0-9_-]+\.js/)?.[0] ??
    html.match(/assets\/index-[A-Za-z0-9_-]+\.js/)?.[0];
  if (!asset) return `https://bc.game/assets/${FALLBACK_WR}`;
  const assetUrl = asset.startsWith("http") ? asset : `https://bc.game${asset.startsWith("/") ? "" : "/"}${asset}`;
  const jsRes = await fetch(assetUrl, {
    headers: { "user-agent": UA },
    signal: AbortSignal.timeout(12_000),
  });
  if (!jsRes.ok) throw new Error(`index bundle ${jsRes.status}`);
  const js = await jsRes.text();
  const wr = js.match(/wr_utils-[A-Za-z0-9_-]+\.js/)?.[0] ?? FALLBACK_WR;
  return `https://bc.game/assets/${wr}`;
}

async function loadSignUtilsInProcess(): Promise<SignUtils> {
  installDomPolyfill();
  const url = await discoverWrUtilsUrl();
  const res = await fetch(url, {
    headers: { "user-agent": UA },
    signal: AbortSignal.timeout(12_000),
  });
  if (!res.ok) throw new Error(`wr_utils fetch ${res.status}`);
  const body = await res.text();
  const tmp = join(tmpdir(), `wr_utils_${process.pid}_${Date.now()}.mjs`);
  await writeFile(tmp, body, "utf8");
  try {
    const mod = await import(pathToFileURL(tmp).href);
    const utils = (await (mod.default ?? mod)) as SignUtils;
    if (typeof utils?.t1 !== "function" || typeof utils?.t2 !== "function") {
      throw new Error("wr_utils missing t1/t2");
    }
    return utils;
  } finally {
    void unlink(tmp).catch(() => undefined);
  }
}

async function signInProcess(): Promise<Signed> {
  const utils = await loadSignUtilsInProcess();
  const probe = utils.t1(UA);
  const testRes = await fetch(
    `https://socketv4.bc.game/test/?p=${encodeURIComponent(probe)}`,
    {
      headers: {
        "user-agent": UA,
        origin: "https://bc.game",
        referer: "https://bc.game/game/crash",
      },
      signal: AbortSignal.timeout(6_000),
    },
  );
  if (!testRes.ok) throw new Error(`/test/ ${testRes.status}`);
  const t = await testRes.text();
  if (!t) throw new Error("empty /test/ sign");
  const p = utils.t2(t, UA);
  return { p, t, ua: UA, at: Date.now() };
}

const WORKER = `
const UA = ${JSON.stringify(UA)};
const loc = "https://bc.game/game/crash";
const g = globalThis;
g.location = { href: loc, toString() { return loc; }, ancestorOrigins: [] };
g.document = {
  location: g.location,
  addEventListener() {},
  removeEventListener() {},
  createElement() { return { setAttribute() {}, style: {} }; },
  body: { appendChild() {} },
  head: { appendChild() {} },
  getElementsByTagName() { return []; },
};
g.window = g; g.self = g;
g.navigator = { userAgent: UA };
async function main() {
  const page = await fetch("https://bc.game/game/crash", {
    headers: { "user-agent": UA, accept: "text/html" },
    signal: AbortSignal.timeout(10000),
  });
  if (!page.ok) throw new Error("crash page " + page.status);
  const html = await page.text();
  const asset = html.match(/\\/assets\\/index-[A-Za-z0-9_-]+\\.js/)?.[0];
  let wr = "wr_utils-BY40daAC.js";
  if (asset) {
    const assetUrl = "https://bc.game" + asset;
    const jsRes = await fetch(assetUrl, { headers: { "user-agent": UA }, signal: AbortSignal.timeout(12000) });
    if (jsRes.ok) {
      const js = await jsRes.text();
      wr = js.match(/wr_utils-[A-Za-z0-9_-]+\\.js/)?.[0] ?? wr;
    }
  }
  const url = "https://bc.game/assets/" + wr;
  const res = await fetch(url, { headers: { "user-agent": UA }, signal: AbortSignal.timeout(12000) });
  if (!res.ok) throw new Error("wr_utils fetch " + res.status);
  const body = await res.text();
  const blob = new Blob([body], { type: "text/javascript" });
  const mod = await import(URL.createObjectURL(blob));
  const utils = await mod.default;
  const probe = utils.t1(UA);
  const testRes = await fetch("https://socketv4.bc.game/test/?p=" + encodeURIComponent(probe), {
    headers: { "user-agent": UA, origin: "https://bc.game", referer: loc },
    signal: AbortSignal.timeout(6000),
  });
  if (!testRes.ok) throw new Error("/test/ " + testRes.status);
  const t = await testRes.text();
  if (!t) throw new Error("empty /test/ sign");
  const p = utils.t2(t, UA);
  process.stdout.write(JSON.stringify({ p, t, ua: UA }));
}
main().catch((err) => {
  process.stderr.write(String(err && err.message ? err.message : err));
  process.exit(1);
});
`;

function signInChild(): Promise<Signed> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["--input-type=module", "-e", WORKER], {
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, NODE_OPTIONS: "" },
    });
    let out = "";
    let err = "";
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error("sign worker timeout"));
    }, 14_000);
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (c) => {
      out += c;
    });
    child.stderr.on("data", (c) => {
      err += c;
    });
    child.on("error", (e) => {
      clearTimeout(timer);
      reject(e);
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      if (code === 0 && out) {
        try {
          const parsed = JSON.parse(out) as { p: string; t: string; ua: string };
          resolve({ ...parsed, at: Date.now() });
          return;
        } catch (e) {
          reject(e instanceof Error ? e : new Error("bad sign json"));
          return;
        }
      }
      reject(new Error(err.trim() || `sign worker exit ${code}`));
    });
  });
}

export async function signSocketQuery(): Promise<{ p: string; t: string; ua: string }> {
  const envP = process.env.BCGAME_SOCKET_P;
  const envT = process.env.BCGAME_SOCKET_T;
  if (envP && envT) return { p: envP, t: envT, ua: UA };
  if (Date.now() < disabledUntil) {
    throw new Error("socket sign unavailable");
  }
  if (cachedSign && Date.now() - cachedSign.at < SIGN_TTL_MS) return cachedSign;
  if (inflight) return inflight;
  inflight = (async () => {
    try {
      const signed = await signInChild();
      cachedSign = signed;
      log.info("bc-sign", "signed in isolated worker");
      return signed;
    } catch (childErr) {
      log.warn("bc-sign", "sign child failed — trying in-process", {
        error: childErr instanceof Error ? childErr.message : String(childErr),
      });
      try {
        const signed = await signInProcess();
        cachedSign = signed;
        log.info("bc-sign", "signed in-process");
        return signed;
      } catch (procErr) {
        disabledUntil = Date.now() + 15_000;
        log.warn("bc-sign", "sign worker failed", {
          child: childErr instanceof Error ? childErr.message : String(childErr),
          process: procErr instanceof Error ? procErr.message : String(procErr),
        });
        throw new Error("socket sign unavailable");
      }
    }
  })().finally(() => {
    inflight = null;
  });
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
