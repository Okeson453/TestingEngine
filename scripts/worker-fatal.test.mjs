/**
 * Fix 1 regression tests — process failure handlers (worker-fatal.mjs).
 *
 * Proves the extracted registration logic still:
 *   - logs the structured uncaughtException line WITHOUT exiting on
 *     non-fatal errors (e.g. the wr_utils setTimeout TypeError)
 *   - exits(1) AND closes the pool on /FATAL/i errors
 *   - exits(1) on unhandledRejection when WORKER_FATAL_ON_UNCAUGHT=1
 *   - unregisters cleanly via the disposer
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { registerProcessFailureHandlers } from "./worker-fatal.mjs";

function fireUncaught(err) {
  process.emit("uncaughtException", err);
}
function fireRejection(reason) {
  process.emit("unhandledRejection", reason, null);
}

test("non-fatal uncaughtException logs structured JSON and does NOT exit", () => {
  const logs = [];
  let exitCode = null;
  let poolClosed = false;
  const fakeConsole = { error: (m) => logs.push(m) };
  const off = registerProcessFailureHandlers({
    exit: (code) => {
      exitCode = code;
    },
    endPool: () => {
      poolClosed = true;
    },
    console: fakeConsole,
  });
  try {
    fireUncaught(new TypeError("setTimeout callback blew up"));
    assert.equal(exitCode, null, "non-fatal error must not exit");
    assert.equal(poolClosed, false, "pool must stay open for non-fatal errors");
    assert.equal(logs.length, 1);
    const parsed = JSON.parse(logs[0]);
    assert.equal(parsed.msg, "uncaughtException");
    assert.equal(parsed.level, "fatal");
    assert.equal(parsed.component, "worker-entry");
    assert.equal(parsed.error.name, "TypeError");
    assert.equal(parsed.error.message, "setTimeout callback blew up");
    assert.ok(parsed.error.stack, "stack captured (bounded)");
  } finally {
    off();
  }
});

test("/FATAL/i uncaughtException exits(1) and closes the pool", () => {
  let exitCode = null;
  let poolClosed = false;
  const off = registerProcessFailureHandlers({
    exit: (code) => {
      exitCode = code;
    },
    endPool: () => {
      poolClosed = true;
    },
    console: { error: () => {} },
  });
  try {
    fireUncaught(new Error("FATAL: boot invariant broken"));
    assert.equal(exitCode, 1, "fatal error must exit(1)");
    assert.equal(poolClosed, true, "pool must be closed before exit");
  } finally {
    off();
  }
});

test("unhandledRejection exits only when WORKER_FATAL_ON_UNCAUGHT=1", () => {
  let exitCode = null;
  const off = registerProcessFailureHandlers({
    fatalEnv: { WORKER_FATAL_ON_UNCAUGHT: "1" },
    exit: (code) => {
      exitCode = code;
    },
    console: { error: () => {} },
  });
  try {
    fireRejection(new Error("promise rejected"));
    assert.equal(exitCode, 1);
  } finally {
    off();
  }

  let exitCode2 = null;
  const off2 = registerProcessFailureHandlers({
    fatalEnv: {},
    exit: (code) => {
      exitCode2 = code;
    },
    console: { error: () => {} },
  });
  try {
    fireRejection(new Error("promise rejected"));
    assert.equal(exitCode2, null, "rejection without the flag must not exit");
  } finally {
    off2();
  }
});

test("disposer removes the handlers (no double registration)", () => {
  let exitCode = null;
  const off = registerProcessFailureHandlers({
    exit: (code) => {
      exitCode = code;
    },
    console: { error: () => {} },
  });
  off();
  // After off(), nothing is listening: emit produces no exit and no throw.
  fireUncaught(new Error("FATAL: after dispose"));
  assert.equal(exitCode, null);
});
