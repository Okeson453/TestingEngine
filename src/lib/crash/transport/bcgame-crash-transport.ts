/**
 * BC.Game Crash Transport Client
 *
 * Reverse-engineered from live CDP capture (2026-09-06) and bundle analysis.
 * Reproduces the minimum transport-loading and event-consumption flow required
 * for an external engineering client to observe the same public Crash-game
 * event stream, isolated from BC.Game's UI layer.
 *
 * CONFIRMED from live runtime/network capture:
 *  - Endpoint: wss://socketv4.bc.game/socket.io/?Accept-Language=en&p=<sign>&t=<source>&EIO=3&transport=websocket
 *  - Engine.IO v3 (EIO=3), WebSocket-only
 *  - Custom binary Socket.IO parser (envelope: [type byte (+0x80→4-byte BE ackId)][1-byte ns len][ns][1-byte event len][event][payload])
 *  - Sign flow: t1(userAgent) → fetch /test/?p=<t1> → t2(response, userAgent) → p=<t2>, t=<response>
 *  - t1/t2 are WASM exports from wr_utils-C-YrHJp6.js (wasm-bindgen/Rust → WebAssembly)
 *  - Namespace: /g/cm (Crash game)
 *  - Events: pr (Prepare), bg (Begin), pg (Progress), e (Escape), ed (End), st (Settle), b/xb/tb (bet streams)
 *  - Join: socket.emit("join") with ackId=0, empty data → server ACK with CrashInfo protobuf
 *  - Heartbeat: client sends text "2" (EIO ping) every pingInterval (5000ms)
 *  - Payloads: Protocol Buffers (NOT JSON)
 *
 * This is a clean-room reimplementation of the transport layer. It does not
 * import or depend on BC.Game's bundles, UI components, or application code.
 */

import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

// ============================================================================
// Types
// ============================================================================

/** Round lifecycle events on the /g/cm namespace */
export type CrashEventName = "pr" | "bg" | "pg" | "e" | "ed" | "st" | "b" | "xb" | "tb";

/** Normalized round-phase status (mirrors BC.Game's status$ subject) */
export type RoundStatus = 0 /* idle */ | 1 /* preparing */ | 2 /* running */ | 3 /* ended */;

/** Prepare event: new round announced */
export interface PrepareEvent {
  event: "pr";
  roundId: number;
  prepareTime: number;
  startTime: number;
  receivedAt: number;
}

/** Begin event: round started, multiplier rising */
export interface BeginEvent {
  event: "bg";
  roundId: number;
  startTime: number;
  receivedAt: number;
}

/** Progress event: multiplier update (high frequency, ~2/s) */
export interface ProgressEvent {
  event: "pg";
  elapsed: number;
  roundId: number;
  multiplier: number;
  receivedAt: number;
}

/** Escape event: player cashed out */
export interface EscapeEvent {
  event: "e";
  userId: number;
  betId: number;
  odds: number;
  force: boolean;
  betIndex: number;
  receivedAt: number;
}

/** End event: round crashed */
export interface EndEvent {
  event: "ed";
  roundId: number;
  maxRate: number;
  multiplier: number;
  hash: string;
  receivedAt: number;
}

/** Settle event: round settled with all escapes */
export interface SettleEvent {
  event: "st";
  roundId: number;
  escapes: Array<{ userId: number; betId: number; odds: number; force: boolean; betIndex: number }>;
  maxRate: number;
  multiplier: number;
  hash: string;
  receivedAt: number;
}

export type CrashEvent =
  | PrepareEvent
  | BeginEvent
  | ProgressEvent
  | EscapeEvent
  | EndEvent
  | SettleEvent;

export type CrashEventHandler = (event: CrashEvent) => void;

export type ConnectionStatus =
  | "stopped"
  | "signing"
  | "connecting"
  | "connected"
  | "reconnecting"
  | "error";

export interface TransportState {
  status: ConnectionStatus;
  sid: string | null;
  pingInterval: number;
  pingTimeout: number;
  lastError: string | null;
  lastEventAt: number | null;
  reconnectAttempts: number;
  totalEvents: number;
}

// ============================================================================
// WASM Sign Module (wr_utils)
// ============================================================================

interface WasmSignModule {
  t1: (userAgent: string) => string;
  t2: (testResponse: string, userAgent: string) => string;
}

/**
 * Load the wr_utils WASM module and expose t1/t2 sign functions.
 *
 * The WASM binary is extracted from BC.Game's wr_utils-C-YrHJp6.js bundle.
 * It is a wasm-bindgen (Rust → WebAssembly) module with two exports:
 *  - t1(ptr, str_ptr, str_len): first-stage sign from User-Agent
 *  - t2(ptr, str_a_ptr, str_a_len, str_b_ptr, str_b_len): second-stage sign from /test/ response + User-Agent
 *
 * String marshaling follows the standard wasm-bindgen pattern:
 *  - Encode JS string to UTF-8, malloc in WASM memory, copy bytes
 *  - Call WASM function with stack pointer (-16) for return values
 *  - Read back two i32s (ptr, len) from the WASM stack
 *  - Decode UTF-8 string from WASM memory
 */
async function loadSignModule(wasmPath: string): Promise<WasmSignModule> {
  const wasmBinary = await readFile(wasmPath);

  // wasm-bindgen imports
  const importObject = {
    "./wr_utils_bg.js": {
      __wbg_now_9c5990bda04c7e53: () => Date.now(),
      __wbindgen_throw: (ptr: number, len: number) => {
        const mem = new Uint8Array(instance.exports.memory.buffer);
        throw new Error(`WASM throw: ${new TextDecoder().decode(mem.subarray(ptr, ptr + len))}`);
      },
    },
  };

  const { instance } = await WebAssembly.instantiate(wasmBinary, importObject as any);
  const exports = instance.exports as any;

  // Lazy memory views (re-create if memory grows)
  let cachedU8: Uint8Array | null = null;
  let cachedI32: Int32Array | null = null;

  function getU8(): Uint8Array {
    if (!cachedU8 || cachedU8.byteLength === 0) {
      cachedU8 = new Uint8Array(exports.memory.buffer);
    }
    return cachedU8;
  }

  function getI32(): Int32Array {
    if (!cachedI32 || cachedI32.byteLength === 0) {
      cachedI32 = new Int32Array(exports.memory.buffer);
    }
    return cachedI32;
  }

  // Invalidate cache after any allocation (memory may grow)
  function invalidateCache(): void {
    cachedU8 = null;
    cachedI32 = null;
  }

  /**
   * Pass a JS string to WASM: encode UTF-8, malloc, copy.
   * Returns [ptr, length].
   */
  function passStringToWasm(str: string): [number, number] {
    const encoder = new TextEncoder();
    const bytes = encoder.encode(str);
    const ptr = exports.__wbindgen_malloc(bytes.length, 1) >>> 0;
    getU8().subarray(ptr, ptr + bytes.length).set(bytes);
    invalidateCache();
    return [ptr, bytes.length];
  }

  /**
   * Read a string back from WASM memory: decode UTF-8 from [ptr, ptr+len).
   */
  function getStringFromWasm(ptr: number, len: number): string {
    const mem = getU8();
    return new TextDecoder("utf-8", { fatal: true }).decode(mem.subarray(ptr >>> 0, (ptr >>> 0) + len));
  }

  /**
   * t1: first-stage sign function.
   * Takes User-Agent string, returns sign string for /test/ endpoint.
   */
  function t1(userAgent: string): string {
    const stackPtr = exports.__wbindgen_add_to_stack_pointer(-16);
    try {
      const [strPtr, strLen] = passStringToWasm(userAgent);
      exports.t1(stackPtr, strPtr, strLen);
      const i32 = getI32();
      const retPtr = i32[stackPtr / 4 + 0];
      const retLen = i32[stackPtr / 4 + 1];
      const result = getStringFromWasm(retPtr, retLen);
      exports.__wbindgen_free(retPtr, retLen, 1);
      invalidateCache();
      return result;
    } finally {
      exports.__wbindgen_add_to_stack_pointer(16);
      invalidateCache();
    }
  }

  /**
   * t2: second-stage sign function.
   * Takes /test/ response text and User-Agent, returns final sign for WS URL.
   */
  function t2(testResponse: string, userAgent: string): string {
    const stackPtr = exports.__wbindgen_add_to_stack_pointer(-16);
    try {
      const [aPtr, aLen] = passStringToWasm(testResponse);
      const [bPtr, bLen] = passStringToWasm(userAgent);
      exports.t2(stackPtr, aPtr, aLen, bPtr, bLen);
      const i32 = getI32();
      const retPtr = i32[stackPtr / 4 + 0];
      const retLen = i32[stackPtr / 4 + 1];
      const result = getStringFromWasm(retPtr, retLen);
      exports.__wbindgen_free(retPtr, retLen, 1);
      invalidateCache();
      return result;
    } finally {
      exports.__wbindgen_add_to_stack_pointer(16);
      invalidateCache();
    }
  }

  return { t1, t2 };
}

// ============================================================================
// Custom Socket.IO Binary Parser (T8)
// ============================================================================

/** Socket.IO packet types (as used in BC.Game's custom parser) */
const PacketType = {
  CONNECT: 0,
  DISCONNECT: 1,
  EVENT: 2,
  ACK: 3,
  ERROR: 4,
  BINARY_EVENT: 5,
  BINARY_ACK: 6,
} as const;

interface DecodedPacket {
  type: number;
  namespace: string;
  event: string;
  ackId: number | null;
  payload: Uint8Array;
}

/**
 * Decode a binary frame using BC.Game's custom Socket.IO parser envelope.
 *
 * Format: [type byte (+0x80 → 4-byte BE ackId)][1-byte ns length][ns][1-byte event length][event][payload]
 *
 * The first byte of the binary WebSocket frame is the EIO MESSAGE marker (0x04)
 * and is stripped before parsing. For ACK frames from the server, the type has
 * the 0x80 bit set with a 4-byte big-endian ackId following.
 */
function decodeBinaryPacket(data: ArrayBuffer | Buffer): DecodedPacket {
  const bytes = data instanceof Buffer ? new Uint8Array(data) : new Uint8Array(data);
  let off = 0;

  // Strip EIO binary message marker (0x04 = MESSAGE)
  if (bytes[off] === 0x04 || bytes[off] === 0x05) {
    off++;
  }

  // Type byte
  let type = bytes[off++];

  // AckId: if high bit set, read 4-byte BE ackId
  let ackId: number | null = null;
  if (type & 0x80) {
    type = type & 0x7f;
    const view = new DataView(bytes.buffer, off, 4);
    ackId = view.getUint32(0);
    off += 4;
  }

  // Namespace
  const nsLen = bytes[off++];
  const ns = nsLen > 0 ? new TextDecoder().decode(bytes.subarray(off, off + nsLen)) : "";
  off += nsLen;

  // Event name
  const evLen = bytes[off++];
  const event = evLen > 0 ? new TextDecoder().decode(bytes.subarray(off, off + evLen)) : "";
  off += evLen;

  // Payload (remaining bytes)
  const payload = bytes.slice(off);

  // Remap binary types to non-binary (as BC.Game's decoder does)
  if (type === PacketType.BINARY_EVENT) type = PacketType.EVENT;
  if (type === PacketType.BINARY_ACK) type = PacketType.ACK;

  return { type, namespace: ns, event, ackId, payload };
}

/**
 * Encode a binary packet (for client → server).
 * Used for namespace CONNECT and EVENT with ack.
 */
function encodeBinaryPacket(
  type: number,
  namespace: string,
  event: string = "",
  ackId: number | null = null,
  payload: Uint8Array = new Uint8Array(0),
): Buffer {
  const encoder = new TextEncoder();
  const nsBytes = encoder.encode(namespace);
  const evBytes = encoder.encode(event);

  // Calculate size
  let typeByte = type;
  const hasAckId = ackId !== null;
  if (hasAckId) typeByte |= 0x80;

  const size = 1 /* EIO marker */ + 1 /* type */ + (hasAckId ? 4 : 0) + 1 /* ns len */ + nsBytes.length + 1 /* ev len */ + evBytes.length + payload.length;
  const buf = Buffer.alloc(size);
  let off = 0;

  // EIO binary message marker
  buf[off++] = 0x04;

  // Type byte
  buf[off++] = typeByte;

  // AckId (4-byte BE)
  if (hasAckId) {
    buf.writeUInt32BE(ackId!, off);
    off += 4;
  }

  // Namespace
  buf[off++] = nsBytes.length;
  buf.set(nsBytes, off);
  off += nsBytes.length;

  // Event
  buf[off++] = evBytes.length;
  buf.set(evBytes, off);
  off += evBytes.length;

  // Payload
  buf.set(payload, off);

  return buf;
}

// ============================================================================
// Minimal Protobuf Decoder
// ============================================================================

/**
 * Minimal protobuf wire-format reader.
 * Only implements the field types used by BC.Game's Crash protos:
 * int64 (varint), int32 (varint), string (length-delimited), bool (varint),
 * and nested messages (length-delimited).
 */
class ProtobufReader {
  private buf: Uint8Array;
  private pos: number;
  private end: number;

  constructor(buf: Uint8Array, length?: number) {
    this.buf = buf;
    this.pos = 0;
    this.end = length !== undefined ? length : buf.length;
  }

  private readVarint(): number {
    let result = 0;
    let shift = 0;
    let byte: number;
    do {
      byte = this.buf[this.pos++];
      result |= (byte & 0x7f) << shift;
      shift += 7;
    } while (byte & 0x80 && shift < 35);
    // For values that don't fit in 32 bits, we lose precision (JS number limit)
    // but roundIds and timestamps fit fine
    return result >>> 0;
  }

  /** Read a field tag: returns [fieldNumber, wireType] */
  readTag(): [number, number] | null {
    if (this.pos >= this.end) return null;
    const tag = this.readVarint();
    return [tag >>> 3, tag & 0x7];
  }

  readInt32(): number {
    return this.readVarint();
  }

  readInt64(): number {
    // Varint-encoded 64-bit value; we read full varint but may lose precision > 2^53
    let result = 0;
    let shift = 0;
    let byte: number;
    do {
      byte = this.buf[this.pos++];
      if (shift < 32) {
        result |= (byte & 0x7f) << shift;
      }
      shift += 7;
    } while (byte & 0x80);
    return result >>> 0;
  }

  readBool(): boolean {
    return this.readVarint() !== 0;
  }

  readString(): string {
    const len = this.readVarint();
    const str = new TextDecoder().decode(this.buf.subarray(this.pos, this.pos + len));
    this.pos += len;
    return str;
  }

  /** Read a length-delimited sub-message */
  readMessage(): ProtobufReader {
    const len = this.readVarint();
    const start = this.pos;
    this.pos += len;
    return new ProtobufReader(this.buf.subarray(start, start + len));
  }

  /** Read raw bytes */
  readBytes(): Uint8Array {
    const len = this.readVarint();
    const bytes = this.buf.subarray(this.pos, this.pos + len);
    this.pos += len;
    return bytes;
  }

  skipField(wireType: number): void {
    switch (wireType) {
      case 0: this.readVarint(); break;
      case 1: this.pos += 8; break;
      case 2: { const len = this.readVarint(); this.pos += len; break; }
      case 5: this.pos += 4; break;
      default: throw new Error(`Unknown wire type ${wireType}`);
    }
  }
}

// ============================================================================
// Protobuf Decoders for Crash Events
// ============================================================================

/** Decode Prepare (pr) protobuf */
function decodePrepare(buf: Uint8Array): Omit<PrepareEvent, "event" | "receivedAt"> {
  const r = new ProtobufReader(buf);
  let roundId = 0, prepareTime = 0, startTime = 0;
  let tag: [number, number] | null;
  while ((tag = r.readTag()) !== null) {
    const [field, wire] = tag;
    switch (field) {
      case 1: roundId = r.readInt64(); break;
      case 3: prepareTime = r.readInt64(); break;
      case 4: startTime = r.readInt64(); break;
      default: r.skipField(wire); break;
    }
  }
  return { roundId, prepareTime, startTime };
}

/** Decode Begin (bg) protobuf */
function decodeBegin(buf: Uint8Array): Omit<BeginEvent, "event" | "receivedAt"> {
  const r = new ProtobufReader(buf);
  let roundId = 0, startTime = 0;
  let tag: [number, number] | null;
  while ((tag = r.readTag()) !== null) {
    const [field, wire] = tag;
    switch (field) {
      case 1: roundId = r.readInt64(); break;
      case 4: startTime = r.readInt64(); break;
      default: r.skipField(wire); break;
    }
  }
  return { roundId, startTime };
}

/** Decode Progress (pg) protobuf */
function decodeProgress(buf: Uint8Array): Omit<ProgressEvent, "event" | "receivedAt"> {
  const r = new ProtobufReader(buf);
  let elapsed = 0, roundId = 0;
  let tag: [number, number] | null;
  while ((tag = r.readTag()) !== null) {
    const [field, wire] = tag;
    switch (field) {
      case 1: elapsed = r.readInt64(); break;
      case 2: roundId = r.readInt64(); break;
      default: r.skipField(wire); break;
    }
  }
  // BC.Game computes: multiplier = e^(c * elapsed), where c ≈ 6e-5 (CONFIRMED from source)
  // The exact formula: rate = Math.pow(Math.E, 0.00006 * elapsed)
  // But elapsed is in milliseconds, so the rate grows exponentially over time.
  // We return the raw elapsed; the consumer can compute the multiplier if needed.
  const multiplier = Math.pow(Math.E, 0.00006 * elapsed);
  return { elapsed, roundId, multiplier };
}

/** Decode Escape (e) protobuf */
function decodeEscape(buf: Uint8Array): Omit<EscapeEvent, "event" | "receivedAt"> {
  const r = new ProtobufReader(buf);
  let userId = 0, betId = 0, odds = 0, force = false, betIndex = 0;
  let tag: [number, number] | null;
  while ((tag = r.readTag()) !== null) {
    const [field, wire] = tag;
    switch (field) {
      case 1: userId = r.readInt64(); break;
      case 2: betId = r.readInt64(); break;
      case 3: odds = r.readInt32(); break;
      case 4: force = r.readBool(); break;
      case 5: betIndex = r.readInt32(); break;
      default: r.skipField(wire); break;
    }
  }
  return { userId, betId, odds, force, betIndex };
}

/** Decode End (ed) protobuf */
function decodeEnd(buf: Uint8Array): Omit<EndEvent, "event" | "receivedAt"> {
  const r = new ProtobufReader(buf);
  let roundId = 0, maxRate = 0;
  let hash = "";
  let tag: [number, number] | null;
  while ((tag = r.readTag()) !== null) {
    const [field, wire] = tag;
    switch (field) {
      case 1: roundId = r.readInt64(); break;
      case 6: maxRate = r.readInt32(); break;
      case 7: hash = r.readString(); break;
      default: r.skipField(wire); break;
    }
  }
  return { roundId, maxRate, multiplier: maxRate / 100, hash };
}

/** Decode Settle (st) protobuf */
function decodeSettle(buf: Uint8Array): Omit<SettleEvent, "event" | "receivedAt"> {
  const r = new ProtobufReader(buf);
  let roundId = 0, maxRate = 0;
  let hash = "";
  const escapes: Array<{ userId: number; betId: number; odds: number; force: boolean; betIndex: number }> = [];
  let tag: [number, number] | null;
  while ((tag = r.readTag()) !== null) {
    const [field, wire] = tag;
    switch (field) {
      case 1: roundId = r.readInt64(); break;
      case 2: {
        const sub = r.readMessage();
        let userId = 0, betId = 0, odds = 0, force = false, betIndex = 0;
        let stag: [number, number] | null;
        while ((stag = sub.readTag()) !== null) {
          const [sf, sw] = stag;
          switch (sf) {
            case 1: userId = sub.readInt64(); break;
            case 2: betId = sub.readInt64(); break;
            case 3: odds = sub.readInt32(); break;
            case 4: force = sub.readBool(); break;
            case 5: betIndex = sub.readInt32(); break;
            default: sub.skipField(sw); break;
          }
        }
        escapes.push({ userId, betId, odds, force, betIndex });
        break;
      }
      case 6: maxRate = r.readInt32(); break;
      case 7: hash = r.readString(); break;
      default: r.skipField(wire); break;
    }
  }
  return { roundId, escapes, maxRate, multiplier: maxRate / 100, hash };
}

// ============================================================================
// Transport Client
// ============================================================================

const CRASH_NAMESPACE = "/g/cm";
const DEFAULT_SOCKET_HOST = "wss://socketv4.bc.game";
const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36";
const RECONNECT_DELAY_MS = 1_000;
const RECONNECT_DELAY_MAX_MS = 30_000;
const STALE_AFTER_MS = 15_000;

export interface TransportClientOptions {
  /** Path to wr_utils.wasm (sign module). Defaults to the bundled copy. */
  wasmPath?: string;
  /** Override socket host (default: wss://socketv4.bc.game) */
  socketHost?: string;
  /** Override User-Agent (default: Chrome 145 on Windows) */
  userAgent?: string;
  /** Auto-reconnect on disconnect (default: true) */
  autoReconnect?: boolean;
  /** Log function for debug output */
  log?: (msg: string, data?: unknown) => void;
}

export class BcGameCrashTransport {
  private ws: WebSocket | null = null;
  private signModule: WasmSignModule | null = null;
  private handlers: Map<CrashEventName, Set<CrashEventHandler>> = new Map();
  private state: TransportState = {
    status: "stopped",
    sid: null,
    pingInterval: 5000,
    pingTimeout: 25000,
    lastError: null,
    lastEventAt: null,
    reconnectAttempts: 0,
    totalEvents: 0,
  };
  private intentionalShutdown = false;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private staleTimer: ReturnType<typeof setInterval> | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private lastPacketAt = 0;
  private options: Required<TransportClientOptions>;

  constructor(options: TransportClientOptions = {}) {
    this.options = {
      wasmPath: options.wasmPath ?? this.getDefaultWasmPath(),
      socketHost: options.socketHost ?? DEFAULT_SOCKET_HOST,
      userAgent: options.userAgent ?? DEFAULT_USER_AGENT,
      autoReconnect: options.autoReconnect ?? true,
      log: options.log ?? (() => {}),
    };
  }

  private getDefaultWasmPath(): string {
    // Resolve relative to this module's location
    const here = typeof __dirname !== "undefined"
      ? __dirname
      : dirname(fileURLToPath(import.meta.url));
    return join(here, "wr_utils.wasm");
  }

  // ---- Event subscription ----

  on(event: CrashEventName, handler: CrashEventHandler): () => void {
    if (!this.handlers.has(event)) {
      this.handlers.set(event, new Set());
    }
    this.handlers.get(event)!.add(handler);
    return () => { this.handlers.get(event)?.delete(handler); };
  }

  getState(): TransportState {
    return { ...this.state };
  }

  // ---- Connection lifecycle ----

  async connect(): Promise<void> {
    if (this.intentionalShutdown) return;
    if (this.state.status === "connected" || this.state.status === "connecting") return;

    const isReconnect = this.state.reconnectAttempts > 0;
    this.updateState({ status: "signing", lastError: null });

    try {
      // 1. Load WASM sign module
      if (!this.signModule) {
        this.options.log("Loading WASM sign module...");
        this.signModule = await loadSignModule(this.options.wasmPath);
        this.options.log("WASM sign module loaded");
      }

      // 2. Sign flow: t1(UA) → fetch /test/ → t2(response, UA)
      this.updateState({ status: "connecting" });
      const sign = await this.performSignFlow();

      // 3. Open WebSocket with signed query params
      const wsUrl = this.buildWebSocketUrl(sign);
      this.options.log("Connecting to WebSocket", { url: wsUrl.replace(/&p=[^&]+/, "&p=<redacted>") });

      this.ws = new WebSocket(wsUrl, {
        headers: {
          "User-Agent": this.options.userAgent,
          "Origin": "https://bc.game",
          "Accept-Language": "en",
        },
      } as any);
      // Set binary type to receive ArrayBuffer
      this.ws.binaryType = "arraybuffer";

      this.ws.onopen = () => this.handleWsOpen();
      this.ws.onmessage = (event) => this.handleWsMessage(event);
      this.ws.onclose = (event) => this.handleWsClose(event);
      this.ws.onerror = (error) => this.handleWsError(error);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.options.log("Connect failed", { error: msg });
      this.updateState({ status: "error", lastError: msg });
      if (this.options.autoReconnect && !this.intentionalShutdown) {
        this.scheduleReconnect();
      }
    }
  }

  disconnect(): void {
    this.intentionalShutdown = true;
    this.clearTimers();
    if (this.ws) {
      try { this.ws.close(); } catch { /* ignore */ }
      this.ws = null;
    }
    this.updateState({ status: "stopped", sid: null });
  }

  // ---- Sign flow ----

  private async performSignFlow(): Promise<{ p: string; t: string }> {
    if (!this.signModule) throw new Error("Sign module not loaded");

    // Step 1: t1(userAgent) → first sign
    const firstSign = this.signModule.t1(this.options.userAgent);
    this.options.log("Sign: t1(UA) computed", { sign: firstSign.slice(0, 16) + "..." });

    // Step 2: fetch https://socketv4.bc.game/test/?p=<firstSign> with credentials
    const testUrl = `${this.options.socketHost.replace("wss://", "https://")}/test/?p=${encodeURIComponent(firstSign)}`;
    this.options.log("Sign: fetching /test/", { url: testUrl.replace(/p=[^&]+/, "p=<redacted>") });

    const response = await fetch(testUrl, {
      headers: {
        "User-Agent": this.options.userAgent,
        "Origin": "https://bc.game",
        "Referer": "https://bc.game/game/crash",
        "Accept-Language": "en",
      },
    });

    if (!response.ok) {
      throw new Error(`/test/ returned ${response.status}: ${response.statusText}`);
    }

    const testResponseText = await response.text();
    this.options.log("Sign: /test/ response received", { length: testResponseText.length });

    // Step 3: t2(testResponse, userAgent) → final sign for p
    const finalSign = this.signModule.t2(testResponseText, this.options.userAgent);
    this.options.log("Sign: t2(response, UA) computed", { sign: finalSign.slice(0, 16) + "..." });

    // p = t2(response, UA), t = raw /test/ response text
    return { p: finalSign, t: testResponseText };
  }

  private buildWebSocketUrl(sign: { p: string; t: string }): string {
    const base = this.options.socketHost;
    const params = new URLSearchParams({
      "Accept-Language": "en",
      "p": sign.p,
      "t": sign.t,
      "EIO": "3",
      "transport": "websocket",
    });
    return `${base}/socket.io/?${params.toString()}`;
  }

  // ---- WebSocket event handlers ----

  private handleWsOpen(): void {
    this.options.log("WebSocket connected, awaiting EIO handshake");
    // Don't set "connected" yet — wait for EIO open frame with sid
  }

  private handleWsMessage(event: MessageEvent): void {
    this.lastPacketAt = Date.now();

    // Text frame (EIO control)
    if (typeof event.data === "string") {
      this.handleTextFrame(event.data);
      return;
    }

    // Binary frame (Socket.IO packet)
    if (event.data instanceof ArrayBuffer || event.data instanceof Buffer) {
      this.handleBinaryFrame(event.data);
      return;
    }
  }

  /** Handle EIO v3 text frames */
  private handleTextFrame(data: string): void {
    const eioType = data.charCodeAt(0);

    switch (eioType) {
      case 0x30: // '0' = Engine.IO OPEN
        this.handleEioOpen(data.slice(1));
        break;
      case 0x32: // '2' = Engine.IO PING (server → client in EIO v3, but BC.Game sends client → server)
        // Respond with pong
        this.sendText("3");
        break;
      case 0x33: // '3' = Engine.IO PONG
        // Heartbeat response received
        break;
      case 0x34: // '4' = Engine.IO MESSAGE (text-encoded Socket.IO packet)
        // Text Socket.IO packets (rare in BC.Game; most are binary)
        this.options.log("EIO text message", { data: data.slice(0, 100) });
        break;
      case 0x31: // '1' = Engine.IO CLOSE
        this.options.log("EIO close frame received");
        break;
      default:
        // Some text frames are JSON ACK messages (observed in capture)
        if (data.startsWith("{")) {
          this.options.log("Text JSON frame", { data: data.slice(0, 100) });
        } else {
          this.options.log("Unknown text frame", { type: eioType, data: data.slice(0, 50) });
        }
    }
  }

  /** Handle EIO v3 open handshake */
  private handleEioOpen(json: string): void {
    try {
      const handshake = JSON.parse(json);
      this.updateState({
        status: "connected",
        sid: handshake.sid ?? null,
        pingInterval: handshake.pingInterval ?? 5000,
        pingTimeout: handshake.pingTimeout ?? 25000,
        reconnectAttempts: 0,
      });
      this.options.log("EIO handshake", {
        sid: this.state.sid,
        pingInterval: this.state.pingInterval,
        pingTimeout: this.state.pingTimeout,
      });

      // Start heartbeat
      this.startPing();

      // Connect to /g/cm namespace
      this.sendNamespaceConnect(CRASH_NAMESPACE);

      // Start staleness watchdog
      this.startStaleWatchdog();
    } catch (err) {
      this.options.log("Failed to parse EIO open", { error: String(err), json: json.slice(0, 100) });
    }
  }

  /** Handle binary Socket.IO packet */
  private handleBinaryFrame(data: ArrayBuffer | Buffer): void {
    try {
      const packet = decodeBinaryPacket(data);

      switch (packet.type) {
        case PacketType.CONNECT:
          this.options.log("Namespace connected", { ns: packet.namespace });
          if (packet.namespace === CRASH_NAMESPACE) {
            // Send join request with ackId=0
            this.sendJoinRequest();
          }
          break;

        case PacketType.EVENT:
          this.handleCrashEvent(packet);
          break;

        case PacketType.ACK:
          // ACK response (e.g., join response with CrashInfo protobuf)
          this.options.log("ACK received", {
            ns: packet.namespace,
            ackId: packet.ackId,
            payloadLen: packet.payload.length,
          });
          // The join ACK contains the CrashInfo protobuf (48KB capabilities blob)
          // We could decode it if needed, but it's not required for round lifecycle
          break;

        case PacketType.DISCONNECT:
          this.options.log("Namespace disconnected", { ns: packet.namespace });
          break;

        case PacketType.ERROR:
          this.options.log("Socket.IO error", { ns: packet.namespace, payload: packet.payload.length });
          break;

        default:
          // Other types (e.g., type 9 observed in capture — likely internal protocol frames)
          break;
      }
    } catch (err) {
      this.options.log("Failed to decode binary frame", { error: String(err) });
    }
  }

  /** Decode and dispatch Crash-game events */
  private handleCrashEvent(packet: DecodedPacket): void {
    // Only process events on the /g/cm namespace
    if (packet.namespace !== CRASH_NAMESPACE && packet.namespace !== "") return;

    const now = Date.now();
    this.state.lastEventAt = now;
    this.state.totalEvents++;

    const eventName = packet.event as CrashEventName;
    let crashEvent: CrashEvent | null = null;

    try {
      switch (eventName) {
        case "pr":
          crashEvent = { event: "pr", receivedAt: now, ...decodePrepare(packet.payload) };
          break;
        case "bg":
          crashEvent = { event: "bg", receivedAt: now, ...decodeBegin(packet.payload) };
          break;
        case "pg":
          crashEvent = { event: "pg", receivedAt: now, ...decodeProgress(packet.payload) };
          break;
        case "e":
          crashEvent = { event: "e", receivedAt: now, ...decodeEscape(packet.payload) };
          break;
        case "ed":
          crashEvent = { event: "ed", receivedAt: now, ...decodeEnd(packet.payload) };
          break;
        case "st":
          crashEvent = { event: "st", receivedAt: now, ...decodeSettle(packet.payload) };
          break;
        default:
          // b, xb, tb — bet stream events (high volume, not decoded here)
          break;
      }
    } catch (err) {
      this.options.log("Failed to decode event", { event: eventName, error: String(err) });
    }

    if (crashEvent) {
      this.dispatch(crashEvent);
    }
  }

  private dispatch(event: CrashEvent): void {
    const handlers = this.handlers.get(event.event);
    if (handlers) {
      for (const handler of handlers) {
        try { handler(event); } catch (err) {
          this.options.log("Handler error", { event: event.event, error: String(err) });
        }
      }
    }
  }

  // ---- Outbound frames ----

  private sendText(data: string): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(data);
    }
  }

  private sendBinary(data: Buffer): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(data);
    }
  }

  /** Send Socket.IO CONNECT for a namespace (binary, type 0) */
  private sendNamespaceConnect(namespace: string): void {
    const packet = encodeBinaryPacket(PacketType.CONNECT, namespace);
    this.options.log("Connecting to namespace", { ns: namespace });
    this.sendBinary(packet);
  }

  /** Send join request on /g/cm (EVENT with ackId=0, empty data) */
  private sendJoinRequest(): void {
    // type=2 (EVENT), ackId=0, ns=/g/cm, event="join", no payload
    const packet = encodeBinaryPacket(PacketType.EVENT, CRASH_NAMESPACE, "join", 0);
    this.options.log("Sending join request on /g/cm");
    this.sendBinary(packet);
  }

  // ---- Heartbeat & staleness ----

  private startPing(): void {
    this.stopPing();
    this.pingTimer = setInterval(() => {
      // EIO v3: client sends "2" (ping), server responds "3" (pong)
      // In BC.Game's capture, client sends "2" every ~5s
      this.sendText("2");
    }, this.state.pingInterval);
  }

  private stopPing(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  private startStaleWatchdog(): void {
    this.stopStaleWatchdog();
    this.staleTimer = setInterval(() => {
      if (this.intentionalShutdown || this.state.status === "stopped") return;
      const lag = Date.now() - this.lastPacketAt;
      if (lag > STALE_AFTER_MS && this.state.status === "connected") {
        this.options.log("Connection stale, forcing reconnect", { lagMs: lag });
        this.forceReconnect();
      }
    }, 5000);
  }

  private stopStaleWatchdog(): void {
    if (this.staleTimer) {
      clearInterval(this.staleTimer);
      this.staleTimer = null;
    }
  }

  // ---- Reconnection ----

  private handleWsClose(event: CloseEvent): void {
    this.options.log("WebSocket closed", { code: event.code, reason: event.reason });
    this.stopPing();
    this.stopStaleWatchdog();
    this.ws = null;

    if (this.intentionalShutdown) {
      this.updateState({ status: "stopped", sid: null });
      return;
    }

    if (this.options.autoReconnect) {
      this.scheduleReconnect();
    }
  }

  private handleWsError(error: Event): void {
    this.options.log("WebSocket error", { error: String(error) });
    // Don't update state to error here — onclose will fire and handle reconnection
  }

  private forceReconnect(): void {
    if (this.ws) {
      try { this.ws.close(); } catch { /* ignore */ }
    }
  }

  private scheduleReconnect(): void {
    if (this.intentionalShutdown) return;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);

    const attempts = this.state.reconnectAttempts;
    const delay = Math.min(RECONNECT_DELAY_MS * Math.pow(2, attempts), RECONNECT_DELAY_MAX_MS);
    this.updateState({ status: "reconnecting", reconnectAttempts: attempts + 1 });
    this.options.log("Scheduling reconnect", { delay, attempt: attempts + 1 });

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (!this.intentionalShutdown) {
        void this.connect();
      }
    }, delay);
  }

  private clearTimers(): void {
    this.stopPing();
    this.stopStaleWatchdog();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private updateState(updates: Partial<TransportState>): void {
    this.state = { ...this.state, ...updates };
  }
}
