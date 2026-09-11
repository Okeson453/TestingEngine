/**
 * Startup unhandledRejection regression (sep 11).
 *
 * Production 16:55 boot traced the startup "unhandledRejection" to the
 * wr_utils sandbox FETCH STUB: it returned a bare Promise.reject, and when
 * the bundle called fetch() without chaining, that rejection escaped the VM
 * unhandled (process-level unhandledRejection at boot).
 *
 * Root fix (native-sign.ts): the stub handles its own rejection AT SOURCE —
 * it logs a loud structured violation AND returns the same rejected promise
 * to the bundle. These tests lock both halves of that contract:
 *
 *   1. No rejection escapes to the process (no unhandledRejection event).
 *   2. The bundle still OBSERVES the rejection (fail loud inside the VM).
 *
 * Needs node:vm.SourceTextModule (--experimental-vm-modules). Runtimes
 * without it (bun) skip — the official package.json runner is node.
 */
import { test } from "node:test";
import assert from "node:assert/strict";

const {
  evaluateWrUtilsBundleInSandbox,
  isWrUtilsSandboxAvailable,
} = await import("@/lib/crash/native-sign");

const sandboxAvailable = isWrUtilsSandboxAvailable();

test("sandbox fetch violation is handled at source — no process unhandledRejection", async (t) => {
  if (!sandboxAvailable) return t.skip("node:vm.SourceTextModule unavailable in this runtime");

  const violations: unknown[] = [];
  const handler = (reason: unknown) => violations.push(reason);
  process.on("unhandledRejection", handler);

  // Suppress the stub's structured console.error noise for this test.
  const origError = console.error;
  console.error = () => undefined;

  try {
    // Bundle double: calls the sandboxed fetch() and does NOT chain it —
    // the exact production shape that escaped as unhandledRejection.
    const body = [
      "fetch('https://rotated.invalid/x');",
      "export default Promise.resolve({ t1: () => 'ok', t2: () => 'ok' });",
    ].join("\n");
    const utils = await evaluateWrUtilsBundleInSandbox(body);
    assert.equal(utils.t1("ua"), "ok");

    // Let microtasks + the stub's internal .catch settle.
    await new Promise((r) => setTimeout(r, 25));
    assert.equal(
      violations.length,
      0,
      `fetch-violation rejection escaped unhandled: ${String(violations[0])}`,
    );
  } finally {
    console.error = origError;
    process.off("unhandledRejection", handler);
  }
});

test("bundle still observes the fetch rejection (fail loud inside the VM)", async (t) => {
  if (!sandboxAvailable) return t.skip("node:vm.SourceTextModule unavailable in this runtime");

  const origError = console.error;
  console.error = () => undefined;
  try {
    const body = [
      "export default fetch('https://rotated.invalid/x')",
      "  .then(() => Promise.resolve({ t1: () => 'x', t2: () => 'x' }));",
    ].join("\n");
    await assert.rejects(
      () => evaluateWrUtilsBundleInSandbox(body),
      /wr_utils sandbox: fetch is not allowed/,
    );
  } finally {
    console.error = origError;
  }
});
