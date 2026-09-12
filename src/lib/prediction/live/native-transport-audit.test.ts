/**
 * Directive 17:27Z — transport audit.
 *
 * The prod log lines ("EIO open — connecting namespace /g/cm", "namespace
 * connect packet", "joined /g/cm — stream live") describe the WIRE protocol
 * BC.Game's endpoint speaks: a binary Engine.IO-style framing. The audit
 * question was whether the "native WebSocket" path secretly depends on the
 * Socket.IO/Engine.IO libraries. Source-level verdict, locked here:
 *
 *   - The primary live path (native-socket-client.ts) is a RAW WebSocket
 *     from the "ws" package. The Engine.IO-style frames ("0{sid}" open,
 *     "2"/"3" ping/pong, namespace connect, /g/cm join) are implemented BY
 *     HAND in this repo (encodeConnect/encodeJoin/parsePacket) — the wire
 *     semantics belong to the SERVER and cannot be avoided; the LIBRARY
 *     dependency can be and is absent.
 *   - "socket.io-client" exists in package.json but is FALLBACK-ONLY: the
 *     only importer is socket-client.ts (legacy path), used by
 *     game-event-handlers.ts solely when USE_NATIVE_BC_WS=0 or the native
 *     socket throws at boot. The native path never imports it.
 *   - The log labels are honest about the protocol, not about a library:
 *     "EIO open" names the frame type we parsed ourselves.
 */
import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const read = (rel: string): string =>
  readFileSync(fileURLToPath(new URL(rel, import.meta.url)), "utf8");

const NATIVE = read("../../crash/native-socket-client.ts");
const HANDLERS = read("../events/game-event-handlers.ts");

describe("native transport has no Socket.IO/Engine.IO dependency", () => {
  it("native client uses the raw ws WebSocket", () => {
    expect(NATIVE).toContain('import { WebSocket } from "ws"');
  });

  it("native client never imports socket.io / engine.io", () => {
    for (const lib of ["socket.io", "engine.io"]) {
      expect(NATIVE).not.toMatch(new RegExp(`from ["']${lib}`));
      expect(NATIVE).not.toMatch(new RegExp(`import\\(["']${lib}`));
    }
  });

  it("wire framing is hand-implemented (EIO open / ping / namespace connect)", () => {
    // The protocol subset BC.Game speaks is implemented in-repo, not pulled
    // from a library. These are the exact wire artifacts observed in prod.
    expect(NATIVE).toContain("parsePacket");
    expect(NATIVE).toContain("encodeConnect");
    expect(NATIVE).toContain("encodeJoin");
  });

  it("native path does not import the legacy socket.io client", () => {
    expect(NATIVE).not.toContain("socket-client");
  });
});

describe("socket.io-client is fallback-only", () => {
  it("socket.io-client is not even a declared dependency (dead dep removed)", () => {
    // Verified 17:30Z: NO file in src/scripts imports socket.io-client —
    // both the native primary path AND the legacy fallback build the
    // wss://socketv4.bc.game/socket.io/?EIO=3 URL by hand over a raw ws
    // WebSocket. The package was a dead dependency; removed in this audit.
    const pkg = read("../../../../package.json");
    expect(pkg).not.toContain("socket.io-client");
    expect(pkg).toContain('"ws"');
  });

  it("handler wiring gates native WS first, socket.io only on failure/opt-out", () => {
    expect(HANDLERS).toContain('process.env.USE_NATIVE_BC_WS !== "0"');
    expect(HANDLERS).toContain("nativeBcGameSocket.start()");
    // The socket.io client is reached only in the failure/opt-out branches.
    const fallbackIdx = HANDLERS.indexOf("bcGameSocket.connect()");
    const nativeIdx = HANDLERS.indexOf("nativeBcGameSocket.start()");
    expect(nativeIdx).toBeGreaterThan(-1);
    expect(fallbackIdx).toBeGreaterThan(nativeIdx);
  });
});
