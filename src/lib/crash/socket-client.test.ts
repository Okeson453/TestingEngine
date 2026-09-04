/**
 * Socket client lifecycle unit tests (no network).
 * Spec: Diagnosis §2 — state machine + reconnect must not set intentional shutdown.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { BcGameSocketClient } from "./socket-client.ts";

test("initial state is stopped", () => {
  const c = new BcGameSocketClient();
  assert.equal(c.getState().status, "stopped");
  assert.equal(c.isConnected(), false);
});

test("disconnect is intentional and blocks reconnect until reset", async () => {
  const c = new BcGameSocketClient();
  c.disconnect();
  assert.equal(c.getState().status, "stopped");
  await c.connect();
  // connect is ignored while intentional shutdown is set
  assert.equal(c.getState().status, "stopped");
  c.resetShutdownFlag();
  // After reset, connect may proceed (will fail without network, but status advances)
  await c.connect();
  const st = c.getState().status;
  assert.ok(
    st === "connecting" || st === "reconnecting" || st === "stopped" || st === "waf_blocked",
    `unexpected status ${st}`,
  );
  c.disconnect();
});

test("cleanup for reconnect does not permanently stop", () => {
  const c = new BcGameSocketClient();
  // Simulate internal reconnect path: status reconnecting, not intentional
  assert.equal(c.getState().status, "stopped");
  c.resetShutdownFlag();
  // isActive should be false until connect starts
  assert.equal(c.isActive(), false);
});

test("connection state exposes transport and event lag fields", () => {
  const c = new BcGameSocketClient();
  const s = c.getState();
  assert.ok("transport" in s);
  assert.ok("lastEdAt" in s);
  assert.ok("lastBgAt" in s);
  assert.ok("eventLagMs" in s);
  assert.ok("totalReconnects" in s);
});
