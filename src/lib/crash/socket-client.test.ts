/**
 * Socket client lifecycle unit tests (no network required for most cases).
 * Spec: Diagnosis §2 — state machine + reconnect must not set intentional shutdown.
 * Protocol: docs/bcgame-crash-transport-report.md §6
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { BcGameSocketClient } from "./socket-client.ts";
import {
  decodeBinaryPacket,
  decodeBegin,
  decodeEnd,
} from "./transport/bcgame-crash-transport.ts";

test("initial state is stopped", () => {
  const c = new BcGameSocketClient();
  assert.equal(c.getState().status, "stopped");
  assert.equal(c.isConnected(), false);
  assert.equal(c.isActive(), false);
});

test("disconnect is intentional and blocks reconnect until reset", async () => {
  const c = new BcGameSocketClient();
  c.disconnect();
  assert.equal(c.getState().status, "stopped");
  await c.connect();
  assert.equal(c.getState().status, "stopped");
  c.resetShutdownFlag();
  assert.equal(c.getState().status, "stopped");
  assert.equal(c.isActive(), false);
});

test("cleanup for reconnect does not permanently stop", () => {
  const c = new BcGameSocketClient();
  assert.equal(c.getState().status, "stopped");
  c.resetShutdownFlag();
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
  assert.ok("socketId" in s);
  assert.ok("reconnectAttempts" in s);
});

test("decodeBinaryPacket parses namespace CONNECT envelope", () => {
  const nsp = Buffer.from("/g/cm", "utf8");
  const buf = Buffer.concat([
    Buffer.from([0x04, 0x00, nsp.length]),
    nsp,
    Buffer.from([0]),
  ]);
  const pkt = decodeBinaryPacket(buf);
  assert.equal(pkt.type, 0);
  assert.equal(pkt.namespace, "/g/cm");
});

test("decodeBinaryPacket parses EVENT with ackId (join shape)", () => {
  const buf = Buffer.from("048200000000052f672f636d046a6f696e", "hex");
  const pkt = decodeBinaryPacket(buf);
  assert.equal(pkt.type, 2);
  assert.equal(pkt.ackId, 0);
  assert.equal(pkt.namespace, "/g/cm");
  assert.equal(pkt.event, "join");
});

test("decodeEnd maps maxRate to multiplier / 100", () => {
  function writeVarint(n: number): number[] {
    const out: number[] = [];
    while (n > 0x7f) {
      out.push((n & 0x7f) | 0x80);
      n >>>= 7;
    }
    out.push(n);
    return out;
  }
  const bytes: number[] = [];
  bytes.push(...writeVarint((1 << 3) | 0));
  bytes.push(...writeVarint(9586584));
  bytes.push(...writeVarint((6 << 3) | 0));
  bytes.push(...writeVarint(370));
  const decoded = decodeEnd(new Uint8Array(bytes));
  assert.equal(decoded.roundId, 9586584);
  assert.equal(decoded.maxRate, 370);
  assert.equal(decoded.multiplier, 3.7);
});

test("decodeBegin reads roundId and startTime", () => {
  function writeVarint(n: number): number[] {
    const out: number[] = [];
    while (n > 0x7f) {
      out.push((n & 0x7f) | 0x80);
      n >>>= 7;
    }
    out.push(n);
    return out;
  }
  const bytes: number[] = [];
  bytes.push(...writeVarint((1 << 3) | 0));
  bytes.push(...writeVarint(100));
  bytes.push(...writeVarint((4 << 3) | 0));
  bytes.push(...writeVarint(1234567890));
  const decoded = decodeBegin(new Uint8Array(bytes));
  assert.equal(decoded.roundId, 100);
  assert.equal(decoded.startTime, 1234567890);
});

test("on() registers and returns unsubscribe", async () => {
  const c = new BcGameSocketClient();
  let calls = 0;
  const off = c.on("ed", async () => {
    calls += 1;
  });
  assert.equal(typeof off, "function");
  off();
  assert.equal(calls, 0);
});

test("getDiscoveredEvents starts empty", () => {
  const c = new BcGameSocketClient();
  assert.deepEqual(c.getDiscoveredEvents(), []);
});

test("decodeBegin preserves full epoch-ms timestamps (not 32-bit truncated)", () => {
  function writeVarint(n: number): number[] {
    const out: number[] = [];
    let v = n;
    while (v >= 0x80) {
      out.push((v & 0x7f) | 0x80);
      v = Math.floor(v / 128);
    }
    out.push(v);
    return out;
  }
  const epochMs = 1_725_820_800_000;
  const bytes: number[] = [];
  bytes.push(...writeVarint((1 << 3) | 0));
  bytes.push(...writeVarint(9586585));
  bytes.push(...writeVarint((4 << 3) | 0));
  bytes.push(...writeVarint(epochMs));
  const decoded = decodeBegin(new Uint8Array(bytes));
  assert.equal(decoded.roundId, 9586585);
  assert.equal(decoded.startTime, epochMs);
  assert.ok(decoded.startTime > 1e12, `startTime too small: ${decoded.startTime}`);
});
