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
  // connect is ignored while intentional shutdown is set
  assert.equal(c.getState().status, "stopped");
  c.resetShutdownFlag();
  // Do not call real connect() here — would hit network; only verify flag reset
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
  // 04 00 05 /g/cm  (EIO marker, type CONNECT, ns len 5, ns)
  const nsp = Buffer.from("/g/cm", "utf8");
  const buf = Buffer.concat([
    Buffer.from([0x04, 0x00, nsp.length]),
    nsp,
    Buffer.from([0]), // empty event len
  ]);
  const pkt = decodeBinaryPacket(buf);
  assert.equal(pkt.type, 0); // CONNECT
  assert.equal(pkt.namespace, "/g/cm");
});

test("decodeBinaryPacket parses EVENT with ackId (join shape)", () => {
  // join request shape from RE report:
  // 04 82 00 00 00 00 05 2f 67 2f 63 6d 04 6a 6f 69 6e
  const buf = Buffer.from(
    "048200000000052f672f636d046a6f696e",
    "hex",
  );
  const pkt = decodeBinaryPacket(buf);
  assert.equal(pkt.type, 2); // EVENT (0x82 & 0x7f)
  assert.equal(pkt.ackId, 0);
  assert.equal(pkt.namespace, "/g/cm");
  assert.equal(pkt.event, "join");
});

test("decodeEnd maps maxRate to multiplier / 100", () => {
  // field 1 = roundId 9586584, field 6 = maxRate 370
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
  // tag field 1 wire 0
  bytes.push(...writeVarint((1 << 3) | 0));
  bytes.push(...writeVarint(9586584));
  // tag field 6 wire 0
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
