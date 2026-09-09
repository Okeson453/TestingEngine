import { spawn } from "node:child_process";
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

type Signed = { p: string; t: string; ua: string; at: number };

let cachedSign: Signed | null = null;
let inflight: Promise<Signed> | null = null;
let disabledUntil = 0;

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
  cookie: "",
};
g.window = g;
g.self = g;
try { g.navigator = { userAgent: UA }; } catch {}
g.WebSocket = class { constructor() { this.readyState = 0; } };
g.addEventListener = () => {};
g.removeEventListener = () => {};

async function main() {
  const step = (s) => (e) => { throw new Error(s + ": " + (e && e.message ? e.message : e)); };
  let htmlRes;
  try {
    htmlRes = await fetch("https://bc.game/game/crash", {
      headers: { "user-agent": UA, accept: "text/html" },
      signal: AbortSignal.timeout(8000),
    });
  } catch (e) { throw step("crash page fetch")(e); }
  if (!htmlRes.ok) throw new Error("crash page http " + htmlRes.status + " (waf/cloudflare?)");
  const html = await htmlRes.text();
  const indexMatch = html.match(/\\/assets\\/index-[^"']+\\.js/);
  const indexPath = indexMatch?.[0] ?? "/assets/index-ChLSFpM-.js";
  let jsRes;
  try {
    jsRes = await fetch("https://bc.game" + indexPath, {
      headers: { "user-agent": UA },
      signal: AbortSignal.timeout(12000),
    });
  } catch (e) { throw step("index bundle fetch " + indexPath)(e); }
  if (!jsRes.ok) throw new Error("index bundle http " + jsRes.status + " " + indexPath);
  const js = await jsRes.text();
  const wr = js.match(/wr_utils-[\\w-]+\\.js/)?.[0] ?? "wr_utils-BY40daAC.js";
  const url = "https://bc.game/assets/" + wr;
  let res;
  try {
    res = await fetch(url, { headers: { "user-agent": UA }, signal: AbortSignal.timeout(12000) });
  } catch (e) { throw step("wr_utils fetch " + wr)(e); }
  if (!res.ok) throw new Error("wr_utils fetch " + wr + " http " + res.status);
  const body = await res.text();
  const blob = new Blob([body], { type: "text/javascript" });
  const mod = await import(URL.createObjectURL(blob));
  const utils = await mod.default;
  const probe = utils.t1(UA);
  const testRes = await fetch("https://socketv4.bc.game/test/?p=" + encodeURIComponent(probe), {
    headers: { "user-agent": UA, origin: "https://bc.game", referer: loc },
    signal: AbortSignal.timeout(6000),
  });
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
  inflight = signInChild()
    .then((signed) => {
      cachedSign = signed;
      log.info("bc-sign", "signed in isolated worker");
      return signed;
    })
    .catch((err) => {
      disabledUntil = Date.now() + 60_000;
      log.warn("bc-sign", "sign worker failed", {
        error: err instanceof Error ? err.message : String(err),
      });
      throw new Error("socket sign unavailable");
    })
    .finally(() => {
      inflight = null;
    });
  return inflight;
}

export function prefetchSign(): void {
  if (Date.now() < disabledUntil) return;
  if (cachedSign && Date.now() - cachedSign.at < SIGN_TTL_MS * 0.6) return;
  void signSocketQuery().catch(() => {
    /* next connect will retry after cooldown */
  });
}
