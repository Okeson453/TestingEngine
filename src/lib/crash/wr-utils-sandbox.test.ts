/**
 * node:vm sandbox for the fetched wr_utils bundle (audit rec 4).
 *
 * Bundle format verified live (2026-09-10, wr_utils-DPR8KNaj.js): a
 * self-contained ES module whose default export is a Promise<{t1, t2}>; wasm
 * is inlined as a base64 data: URL, so there are no imports and no network at
 * init. These tests lock that contract with synthetic bundle doubles:
 *
 *   1. A well-formed bundle evaluates in the sandbox and CANNOT see host
 *      globals (process / fs / env are absent from the context).
 *   2. A bundle missing t1/t2 fails with the structured
 *      "wr_utils missing t1/t2" error.
 *   3. A bundle with static imports is rejected by the linker (the verified
 *      bundle is self-contained; an import is a format change = fail loud).
 *
 * The sandbox needs node:vm.SourceTextModule (--experimental-vm-modules).
 * Runtimes without it (e.g. bun) skip the real-sandbox tests — the official
 * package.json runner is node, where they execute for real.
 */
import { test } from "node:test";
import assert from "node:assert/strict";

const {
  evaluateWrUtilsBundleInSandbox,
  isWrUtilsSandboxAvailable,
} = await import("@/lib/crash/native-sign");

const sandboxAvailable = isWrUtilsSandboxAvailable();

test("sandbox evaluates a verified-format bundle double in isolation", async (t) => {
  if (!sandboxAvailable) return t.skip("node:vm.SourceTextModule unavailable in this runtime");
  // Mimics the live bundle: default export is a Promise resolving to {t1,t2}.
  // t1 reports whether HOST globals leaked into the evaluation context.
  const body = [
    "const leaked = typeof process !== 'undefined' || typeof require !== 'undefined';",
    "if (typeof WebAssembly !== 'object') throw new Error('WebAssembly missing from sandbox');",
    "if (typeof atob !== 'function') throw new Error('atob missing from sandbox');",
    "export default Promise.resolve({ t1: () => leaked ? 'LEAKED' : 'isolated', t2: () => 't2' });",
  ].join("\n");
  const utils = await evaluateWrUtilsBundleInSandbox(body);
  assert.equal(utils.t1("ua"), "isolated", "host globals must not be visible inside the sandbox");
  assert.equal(utils.t2("src", "ua"), "t2");
});

test("sandbox rejects a bundle whose default export is missing t1/t2", async (t) => {
  if (!sandboxAvailable) return t.skip("node:vm.SourceTextModule unavailable in this runtime");
  const body = "export default Promise.resolve({ t1: () => 'x' });";
  await assert.rejects(
    () => evaluateWrUtilsBundleInSandbox(body),
    /wr_utils missing t1\/t2/,
  );
});

test("sandbox rejects a bundle with static imports (format drift = fail loud)", async (t) => {
  if (!sandboxAvailable) return t.skip("node:vm.SourceTextModule unavailable in this runtime");
  const body = "import something from 'node:fs';\nexport default Promise.resolve(something);";
  await assert.rejects(
    () => evaluateWrUtilsBundleInSandbox(body),
    /bundle attempted an import/,
  );
});

test("sandbox reports a throwing bundle as a structured failure", async (t) => {
  if (!sandboxAvailable) return t.skip("node:vm.SourceTextModule unavailable in this runtime");
  const body = "export default Promise.reject(new Error('rotated bundle exploded'));";
  await assert.rejects(
    () => evaluateWrUtilsBundleInSandbox(body),
    /rotated bundle exploded/,
  );
});

// ——— Directive 17:27Z: rotating-variant browser-global union ———
// bc.game serves DIFFERENT BYTES under the SAME wr_utils-*.js filename per
// fetch (verified live, 6 samples: 43631-44176B). The variants differ in an
// anti-bot preamble that touches a varying subset of browser globals:
//   document (property probe + addEventListener('visibilitychange')),
//   requestAnimationFrame, URL, setTimeout, location. The prod errors
//   "ReferenceError: document is not defined" / "requestAnimationFrame is
//   not defined" were two variants of the SAME failure class: the sandbox
//   allowlist lagged the variant union. The sandbox stub set must cover the
//   UNION so any served variant evaluates — that is the determinism fix.

test("sandbox source stubs the full observed variant union", async () => {
  // Runtime-independent source assertion: runs even where the vm sandbox
  // itself cannot (bun), keeping the contract visible in every runner.
  const { readFileSync } = await import("node:fs");
  const { fileURLToPath } = await import("node:url");
  const src = readFileSync(
    fileURLToPath(new URL("./native-sign.ts", import.meta.url)),
    "utf8",
  );
  for (const stub of [
    "document:", "addEventListener: () => {}", "removeEventListener: () => {}",
    "requestAnimationFrame: () => 0", "cancelAnimationFrame: () => {}",
    "location:", "navigator:", "URL,", "setTimeout: () => 0",
    "module.require is not allowed",
  ]) {
    assert.ok(src.includes(stub), `sandbox allowlist must contain ${stub}`);
  }
});

test("a bundle double mimicking the anti-bot preamble evaluates in the sandbox", async (t) => {
  if (!sandboxAvailable) return t.skip("node:vm.SourceTextModule unavailable in this runtime");
  // Mimics the live preamble shape: document property probe inside new URL,
  // visibilitychange listener, rAF callback, setTimeout — then the real
  // wasm-bindgen contract. All preamble globals must come from the sandbox
  // allowlist, and host globals must STILL be invisible (leak check intact).
  const body = `
    document.addEventListener('visibilitychange', () => {}, { once: true });
    new URL(document.location.href);
    requestAnimationFrame(() => 0);
    const timer = setTimeout(() => {}, 0);
    if (typeof process !== 'undefined' || typeof require !== 'undefined') {
      throw new Error('host globals leaked into sandbox');
    }
    export default Promise.resolve({ t1: () => 'ok', t2: () => 't2' });
  `;
  const utils = await evaluateWrUtilsBundleInSandbox(body);
  assert.equal(utils.t1("ua"), "ok");
  assert.equal(utils.t2("src", "ua"), "t2");
});
