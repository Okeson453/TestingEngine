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
import vm from "node:vm";
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
  // Minimal DOM stubs for wr_utils — intentionally not full Window/Location types.
  const g = globalThis as unknown as {
    document?: { location: { href: string; toString(): string } };
    window?: unknown;
    self?: unknown;
  };
  const locationLike = {
    href: loc,
    toString() {
      return loc;
    },
  };
  g.document = { location: locationLike };
  g.window ??= globalThis;
  g.self ??= globalThis;
}

// ——— node:vm sandbox (audit rec 4) ———
// The wr_utils bundle is fetched from bc.game at runtime and executed here.
// Dynamic import() runs it with FULL Node globals (fs, net, process) — a
// compromised or rotated bundle could exfiltrate DATABASE_URL / bot tokens.
// The sandbox runs it in a context that sees only an explicit allowlist.
//
// Bundle format verified live (2026-09-10, wr_utils-DPR8KNaj.js): a
// self-contained ES module — wasm is inlined as a base64 data: URL, so there
// are NO imports and NO network at init. Runtime needs: WebAssembly,
// TextEncoder/TextDecoder, atob, tolerant window/global/self (SENTRY_RELEASE
// block, try/catch-wrapped). fetch exists only in a dead branch; we provide a
// rejecting stub so an unexpected fetch fails loud instead of leaking out.
type VmModuleCtor = new (
  code: string,
  opts?: { identifier?: string; context?: vm.Context },
) => {
  link(linker: () => void): Promise<void>;
  evaluate(): Promise<unknown>;
  namespace: Record<string, unknown>;
};

function getSourceTextModule(): VmModuleCtor | null {
  // Requires --experimental-vm-modules (not enabled → null, caller falls back)
  const ctor = (vm as unknown as { SourceTextModule?: VmModuleCtor })
    .SourceTextModule;
  return typeof ctor === "function" ? ctor : null;
}

/** True when the node:vm sandbox path is available in this runtime. */
export function isWrUtilsSandboxAvailable(): boolean {
  return getSourceTextModule() !== null;
}

/**
 * Evaluate a wr_utils bundle body inside a node:vm context with a minimal
 * global allowlist. Exported for tests; production callers go via
 * loadSignUtils. Throws structured errors on any bundle that fails
 * validation — same contract as the legacy path.
 */
export async function evaluateWrUtilsBundleInSandbox(
  body: string,
): Promise<SignUtils> {
  const SourceTextModule = getSourceTextModule();
  if (!SourceTextModule) {
    throw new Error(
      "wr_utils sandbox unavailable: node:vm.SourceTextModule requires --experimental-vm-modules",
    );
  }
  const sandbox: Record<string, unknown> = {
    // SENTRY_RELEASE block touches window/global/self inside try/catch —
    // plain objects satisfy the typeof checks without host access.
    window: {},
    global: {},
    self: {},
    WebAssembly,
    TextEncoder,
    TextDecoder,
    atob,
    btoa,
    console,
    // Dead branch in the verified bundle (wasm is a data: URL) — provided so
    // any surprise fetch is a loud structured failure, never a host request.
    // BOOT REJECTION FIX (16:55:46 unhandledRejection class): the stub used
    // to return a bare Promise.reject. When the bundle calls fetch() and
    // does not await/chain the result, that rejection has NO handler inside
    // the VM and escapes the context as a process unhandledRejection during
    // boot. The rejection is now handled AT THE BOUNDARY THAT CREATED IT:
    // our own .catch records the violation loudly (structured error log with
    // the exact message), while the SAME rejected promise is still returned
    // to the bundle — if the bundle awaits it, it observes the identical
    // error. Nothing is suppressed or relabeled: the violation is reported
    // deliberately, with the calling context it previously lacked.
    fetch: () => {
      const violation = Promise.reject(
        new Error("wr_utils sandbox: fetch is not allowed"),
      );
      violation.catch((err: Error) => {
        console.error(
          JSON.stringify({
            level: "error",
            time: new Date().toISOString(),
            component: "native-sign",
            msg: "wr_utils sandbox fetch violation (handled at source)",
            error: { name: err.name, message: err.message },
          }),
        );
      });
      return violation;
    },
  };
  sandbox.globalThis = sandbox;
  const context = vm.createContext(sandbox);
  const mod = new SourceTextModule(body, { context, identifier: "wr_utils.vm.mjs" });
  // Bundle is self-contained (no static imports — verified live); the linker
  // must never be called. If a rotated bundle starts importing, this throws
  // and the structured sign-failure path handles it.
  await mod.link(() => {
    throw new Error("wr_utils sandbox: bundle attempted an import");
  });
  await mod.evaluate();
  const exported = mod.namespace.default as Promise<SignUtils> | SignUtils;
  const utils = await exported;
  if (typeof utils?.t1 !== "function" || typeof utils?.t2 !== "function") {
    throw new Error("wr_utils missing t1/t2");
  }
  return utils;
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

/**
 * True when the privileged (unsandboxed) dynamic-import fallback may run.
 * Production FAILS CLOSED — fix plan Phase 3. Exported for tests.
 */
export function isPrivilegedFallbackAllowed(): boolean {
  return process.env.NODE_ENV !== "production";
}

let sandboxFallbackWarned = false;

let loadInflight: Promise<SignUtils> | null = null;

/**
 * Load (fetch + sandbox-evaluate) the wr_utils bundle, single-flight.
 *
 * Concurrent callers share ONE fetch+evaluate attempt chain: without this,
 * every sign request that raced a cold cache ran its own 3-attempt loop
 * (prod 2026-09-10: five overlapping bundle fetches during boot). Failures
 * are NOT cached — the next caller starts a fresh chain, so transient
 * cold-network fetch failures self-heal.
 */
async function loadSignUtils(): Promise<SignUtils> {
  if (cachedUtils) return cachedUtils;
  if (loadInflight) return loadInflight;
  loadInflight = loadSignUtilsOnce();
  try {
    return await loadInflight;
  } finally {
    loadInflight = null;
  }
}

async function loadSignUtilsOnce(): Promise<SignUtils> {
  // SEP 12 (15:46 boot): start the worker_state cache read IN PARALLEL with
  // the fresh fetch chain. Previously the cache was only consulted after all
  // 3 fresh attempts failed — a guaranteed fail-wait-retry-fallback sequence
  // (~2.5s added to sign-ready) whenever bc.game was flaky. With the read
  // in-flight, a failed fresh attempt can short-circuit to the cache
  // immediately; a successful fresh fetch still wins and persists a new
  // last-good bundle.
  const cachedPromise: Promise<Awaited<ReturnType<typeof loadCachedWrUtilsBundle>> | null> =
    loadCachedWrUtilsBundle().catch(() => null);
  const applyCached = async (): Promise<SignUtils | null> => {
    const cached = await cachedPromise;
    if (!cached) return null;
    try {
      const utils = await evaluateWrUtilsBundleInSandbox(cached.body);
      cachedUtils = utils;
      logger.warn(
        { cachedAt: new Date(cached.at).toISOString(), source: cached.url },
        "wr_utils loaded from worker_state cache (fresh fetch chain failed)",
      );
      return utils;
    } catch (e) {
      logger.warn(
        { error: e instanceof Error ? e.message : String(e) },
        "wr_utils DB-cache fallback failed — stale or rotated bundle",
      );
      return null;
    }
  };
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

      // Audit rec 4: prefer the node:vm sandbox. Dynamic import() would run
      // the fetched bundle with full Node globals (fs/net/process) — a
      // rotated or malicious bundle could exfiltrate env/secrets. The
      // sandbox sees only the allowlisted globals it verifiably needs.
      if (getSourceTextModule() !== null) {
        const utils = await evaluateWrUtilsBundleInSandbox(body);
        cachedUtils = utils;
        // SEP 11 FIX: persist the last-good bundle so a flaky bc.game boot
        // (7 failed fetch attempts, 00:12 deploy log) can fall back to the
        // DB cache instead of delaying sign readiness ~19s. Fire-and-forget.
        void persistWrUtilsBundleCache(body, url).catch(() => undefined);
        logger.info({ url, attempt, sandboxed: true }, "wr_utils loaded");
        return cachedUtils;
      }

      // Fix plan Phase 3: production FAILS CLOSED. A privileged dynamic
      // import of downloaded third-party code is not an acceptable fallback
      // in production — refuse and let the structured sign-failure path
      // degrade the WS instead of exposing process/fs/net to the bundle.
      if (!isPrivilegedFallbackAllowed()) {
        throw new Error(
          "wr_utils sandbox unavailable in production (needs NODE_OPTIONS=--experimental-vm-modules) — refusing UNSANDBOXED dynamic import of downloaded code (fail closed)",
        );
      }
      if (!sandboxFallbackWarned) {
        sandboxFallbackWarned = true;
        logger.warn(
          { component: "bc-sign" },
          "wr_utils sandbox unavailable (dev mode) — falling back to privileged dynamic import of the fetched bundle; production fails closed",
        );
      }
      // Legacy path: full-privilege dynamic import via a temp .mjs file.
      installDomPolyfill();
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
        logger.info({ url, attempt, sandboxed: false }, "wr_utils loaded");
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
      if (attempt < 3) {
        // Cache landed while we were failing? Use it now — no fail-wait-retry.
        const early = await Promise.race([
          cachedPromise,
          new Promise<null>((r) => setTimeout(() => r(null), 0)),
        ]);
        if (early) {
          const utils = await applyCached();
          if (utils) return utils;
        }
        await sleep(400 * attempt);
      }
    }
  }

  // SEP 11 FIX (wr_utils load): all fresh attempts failed. Before throwing,
  // fall back to the last-good bundle persisted in worker_state — bc.game
  // fetches from Railway were flaky at boot (7 failed attempts, ~19s of
  // sign-readiness delay, 00:12 deploy log) and the bundle rotates rarely.
  // Fresh fetch is always preferred; this cache only rescues a failing boot.
  // The cache read has been running in parallel since function entry, so
  // this await is usually already resolved (no serialized 543ms tail).
  const utils = await applyCached();
  if (utils) return utils;
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

const WR_UTILS_CACHE_KEY = "wr_utils_bundle_cache";
/** Process-local cache so boot/hot path never re-reads the multi-hundred-KB
 *  worker_state blob (prod measured SELECT ~573ms). */
let memWrUtilsBundleCache: { body: string; url: string; at: number } | null = null;

/** Persist the last-good bundle body so boots survive bc.game flakiness. */
async function persistWrUtilsBundleCache(body: string, url: string): Promise<void> {
  memWrUtilsBundleCache = { body, url, at: Date.now() };
  // P3 (sep 12 10:40Z directive): the bundle body is multi-hundred-KB; the
  // worker_state upsert held a general-pool client for ~578ms inside the
  // boot window (prod: slow_query_ms=578 loop_lag_ms=1 — payload transfer
  // over the Neon link, not CPU). The in-memory cache is authoritative for
  // this process, and the durable copy only matters for a FUTURE boot's
  // fetch-failure fallback — so defer the write until boot and the first
  // prediction rounds are long over. unref'd: an early process exit merely
  // skips the cache write (the next successful fetch re-persists it).
  const timer = setTimeout(() => {
    void (async () => {
      try {
        const { getSql } = await import("@/lib/db");
        const sql = await getSql();
        await sql`
          insert into worker_state (key, value, updated_at)
          values (${WR_UTILS_CACHE_KEY}, ${JSON.stringify({ body, url, at: Date.now() })}, now())
          on conflict (key) do update
            set value = excluded.value, updated_at = now()
        `;
      } catch {
        /* soft — memory cache remains authoritative for this process */
      }
    })();
  }, 60_000);
  timer.unref?.();
}

async function loadCachedWrUtilsBundle(): Promise<{ body: string; url: string; at: number } | null> {
  if (memWrUtilsBundleCache?.body) return memWrUtilsBundleCache;
  try {
    const { getSql } = await import("@/lib/db");
    const sql = await getSql();
    const rows = await sql<{ value: string | null }>`
      select value from worker_state where key = ${WR_UTILS_CACHE_KEY} limit 1
    `;
    const raw = rows[0]?.value;
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { body?: string; url?: string; at?: number };
    if (!parsed.body || !parsed.url || !parsed.at) return null;
    memWrUtilsBundleCache = { body: parsed.body, url: parsed.url, at: parsed.at };
    return memWrUtilsBundleCache;
  } catch {
    return null;
  }
}

/**
 * Startup readiness gate (second-opinion report #1): signing is a HARD
 * dependency of the live pipeline — without it the native socket cannot
 * connect, ED never fires, and the worker produces nothing while still
 * advertising itself as up. Boot awaits this BEFORE starting the WS /
 * pipeline; production treats exhaustion as fatal so the runtime restarts a
 * clean worker instead of running a dead one.
 *
 * Fresh chain per call: bypasses the soft-fail/stale-cache path on purpose —
 * a boot gate must not "succeed" by serving stale credentials.
 */
export async function ensureSignReady(timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastErr: unknown;
  let attempt = 0;
  while (Date.now() < deadline) {
    attempt += 1;
    try {
      const signed = await refreshSign();
      cachedSign = signed;
      softFailUntil = 0;
      logger.info(
        { component: "bc-sign", attempt, timeoutMs },
        "sign readiness confirmed (bundle + self-test)",
      );
      return;
    } catch (e) {
      lastErr = e;
      await sleep(500);
    }
  }
  throw new Error(
    `sign not ready after ${attempt} attempts in ${timeoutMs}ms: ${
      lastErr instanceof Error ? lastErr.message : String(lastErr)
    }`,
  );
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

export async function signSocketQuery(): Promise<Signed> {
  const envP = process.env.BCGAME_SOCKET_P;
  const envT = process.env.BCGAME_SOCKET_T;
  if (envP && envT) return { p: envP, t: envT, ua: UA, at: Date.now() };

  if (cachedSign && Date.now() - cachedSign.at < SIGN_TTL_MS) {
    return { ...cachedSign };
  }

  // Soft-fail window: still serve stale cache so reconnects don't die.
  if (Date.now() < softFailUntil) {
    const stale = staleCached();
    if (stale) {
      logger.info(
        { ageMs: Date.now() - stale.at },
        "using stale sign during soft-fail window",
      );
      return { ...stale };
    }
  }

  if (inflight) return inflight;

  inflight = (async () => {
    try {
      const signed = await refreshSign();
      cachedSign = signed;
      softFailUntil = 0;
      logger.info({}, "socket query signed");
      return { p: signed.p, t: signed.t, ua: signed.ua, at: signed.at };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      softFailUntil = Date.now() + 5_000;
      const stale = staleCached();
      if (stale) {
        logger.warn(
          { error: msg, ageMs: Date.now() - stale.at },
          "sign refresh failed — using stale cache",
        );
        return { ...stale };
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
