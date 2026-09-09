/**
 * Native `ws` BC.Game Crash ingest (from tested workspace).
 * Binary Engine.IO v3 + protobuf /g/cm — not socket.io-client.
 * No SIM path.
 */
import { WebSocket } from "ws";
import { getLogger } from "@/lib/observability/logger";
import {
  decodeProgressElapsed,
  decodeProtobuf,
  encodeConnect,
  encodeJoin,
  fieldsToPayload,
  isEngineOpenFrame,
  isEnginePing,
  isEnginePong,
  multiplierFromElapsed,
  NSP,
  parsePacket,
} from "@/lib/crash/native-protocol";
import { prefetchSign, signSocketQuery } from "@/lib/crash/native-sign";
import { getRealtimePipeline } from "@/lib/realtime/realtime-pipeline";

const logger = getLogger("native-bc-socket");

const SOCKET_HOST = process.env.BCGAME_SOCKET_HOST ?? "socketv4.bc.game";
const RECONNECT_DELAY_MS = 200;
const RECONNECT_DELAY_MAX_MS = 4_000;
const WAF_BACKOFF_MS = Number(process.env.WAF_BACKOFF_MS ?? 20_000);
const DEGRADED_AFTER_MS = 20_000;
/** 5s keepalive (ported from tested workspace) — faster dead-socket detection. */
const PING_MS = 5_000;
const TRACKED = new Set(["pr", "bg", "pg", "ed", "st"]);

export type NativeCrashEvent = {
  event: string;
  gameId: string;
  multiplier?: number | null;
  beginTime?: number | null;
  endTime?: number | null;
  hash?: string | null;
  elapsedMs?: number | null;
  receivedAt: number;
};

type EventHandler = (ev: NativeCrashEvent) => void;
type StatusHandler = (status: string, detail?: string) => void;

function isAuthOrWaf(err: unknown): boolean {
  const text = err instanceof Error ? err.message : String(err);
  const lower = text.toLowerCase();
  return (
    lower.includes("401") ||
    lower.includes("403") ||
    lower.includes("503") ||
    lower.includes("auth failed") ||
    lower.includes("cloudflare") ||
    lower.includes("waf")
  );
}

export class NativeBcGameSocket {
  private socket: WebSocket | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private wafTimer: ReturnType<typeof setTimeout> | null = null;
  private healthTimer: ReturnType<typeof setInterval> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private intentionalStop = false;
  private joined = false;
  private currentGameId: string | null = null;
  private handlers = new Set<EventHandler>();
  private statusHandlers = new Set<StatusHandler>();
  private status: string = "stopped";
  private lastError: string | null = null;
  private lastEventAt: number | null = null;
  private lastEdAt: number | null = null;
  private reconnectAttempts = 0;

  onEvent(handler: EventHandler): () => void {
    this.handlers.add(handler);
    return () => this.handlers.delete(handler);
  }

  onStatus(handler: StatusHandler): () => void {
    this.statusHandlers.add(handler);
    return () => this.statusHandlers.delete(handler);
  }

  getStatus(): string {
    return this.status;
  }

  getLastEdAt(): number | null {
    return this.lastEdAt;
  }

  getLastEventAt(): number | null {
    return this.lastEventAt;
  }

  async start(): Promise<void> {
    this.intentionalStop = false;
    if (this.status === "connected" || this.status === "connecting") return;
    await this.connect();
  }

  async stop(): Promise<void> {
    this.intentionalStop = true;
    this.clearTimers();
    try {
      this.socket?.close();
    } catch {
      /* soft */
    }
    this.socket = null;
    this.setStatus("stopped");
  }

  private setStatus(status: string, detail?: string): void {
    this.status = status;
    if (detail) this.lastError = detail;
    getRealtimePipeline().onStatus(status, detail);
    for (const h of this.statusHandlers) {
      try {
        h(status, detail);
      } catch {
        /* soft */
      }
    }
  }

  private async connect(): Promise<void> {
    if (this.intentionalStop) return;
    const isReconnect = this.reconnectAttempts > 0 || this.status === "reconnecting";
    this.setStatus(isReconnect ? "reconnecting" : "connecting");
    this.joined = false;
    this.currentGameId = null;

    try {
      const { p, t, ua } = await signSocketQuery();
      const url =
        `wss://${SOCKET_HOST}/socket.io/?Accept-Language=en` +
        `&p=${encodeURIComponent(p)}&t=${encodeURIComponent(t)}&EIO=3&transport=websocket`;

      const socket = new WebSocket(url, {
        headers: {
          "User-Agent": ua,
          Origin: "https://bc.game",
          Referer: "https://bc.game/game/crash",
          "Accept-Language": "en",
        },
        handshakeTimeout: 8_000,
        perMessageDeflate: false,
      });
      this.socket = socket;

      socket.on("open", () => {
        logger.info({ component: "native-bc-socket" }, "ws open");
      });

      socket.on("message", (data, isBinary) => {
        if (!isBinary && typeof data === "string") {
          this.onText(data);
          return;
        }
        const buf =
          data instanceof ArrayBuffer
            ? new Uint8Array(data)
            : data instanceof Uint8Array
              ? data
              : new Uint8Array(data as Buffer);
        this.onBinary(buf);
      });

      socket.on("unexpected-response", (_req, res) => {
        const status = res.statusCode ?? 0;
        logger.warn({ component: "native-bc-socket", status }, "unexpected-response");
        if (status === 401 || status === 403 || status === 503) this.handleWaf(`http ${status}`);
        else this.scheduleReconnect();
      });

      socket.on("close", (code, reason) => {
        logger.warn(
          { component: "native-bc-socket", code, reason: reason?.toString?.() },
          "close",
        );
        if (!this.intentionalStop) this.scheduleReconnect();
      });

      socket.on("error", (err) => {
        logger.warn({ component: "native-bc-socket", error: err.message }, "error");
        if (isAuthOrWaf(err)) this.handleWaf(err.message);
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      logger.warn({ component: "native-bc-socket", error: message }, "connect failed");
      if (message.includes("sign unavailable")) {
        this.setStatus("waf_blocked", message);
        return;
      }
      if (isAuthOrWaf(err)) this.handleWaf(message);
      else this.scheduleReconnect();
    }
  }

  private onText(asText: string): void {
    if (asText.charCodeAt(0) === 0x30 && asText.charCodeAt(1) === 0x7b) {
      try {
        JSON.parse(asText.slice(1));
        this.reconnectAttempts = 0;
        this.setStatus("connected");
        this.socket?.send(encodeConnect(NSP));
        this.startPing();
        this.startHealthMonitor();
        prefetchSign();
      } catch {
        /* ignore */
      }
      return;
    }
    if (asText === "2") {
      this.socket?.send("3");
    }
  }

  private onBinary(buf: Uint8Array): void {
    if (isEnginePing(buf)) {
      this.socket?.send("3");
      return;
    }
    if (isEnginePong(buf) || isEngineOpenFrame(buf)) return;

    const packet = parsePacket(buf);
    if (packet.kind === "connect") {
      if (packet.nsp === NSP && !this.joined) {
        this.joined = true;
        this.socket?.send(encodeJoin(NSP));
        logger.info({ component: "native-bc-socket" }, "joined /g/cm");
      }
      return;
    }
    if (packet.kind !== "event") return;
    if (packet.nsp !== NSP && packet.nsp !== "") return;
    if (!TRACKED.has(packet.event)) return;

    let gameId: string | null = null;
    let multiplier: number | null = null;
    let beginTime: number | null = null;
    let endTime: number | null = null;
    let hash: string | null = null;
    let elapsedMs: number | null = null;

    if (packet.event === "pg") {
      elapsedMs = decodeProgressElapsed(packet.payload);
      if (!this.currentGameId) return;
      gameId = this.currentGameId;
      multiplier = elapsedMs === null ? null : multiplierFromElapsed(elapsedMs);
    } else {
      const fields = decodeProtobuf(packet.payload);
      const payload = fieldsToPayload(packet.event, fields);
      gameId = payload.gameId ?? null;
      multiplier = payload.multiplier ?? null;
      beginTime = payload.beginTime ?? null;
      endTime = payload.endTime ?? null;
      hash = payload.hash ?? null;
    }

    if (packet.event === "pr" || packet.event === "bg") {
      if (gameId) this.currentGameId = gameId;
    }
    if (!gameId) return;

    const receivedAt = Date.now();
    this.lastEventAt = receivedAt;
    if (packet.event === "ed") this.lastEdAt = receivedAt;
    if (this.status === "degraded") this.setStatus("connected");

    const ev: NativeCrashEvent = {
      event: packet.event,
      gameId,
      multiplier,
      beginTime,
      endTime,
      hash,
      elapsedMs,
      receivedAt,
    };
    // Latency budget gate (ported realtime layer): drop duplicates,
    // stale end events, and count missed rounds before dispatch.
    if (!getRealtimePipeline().observe(ev)) return;

    for (const h of this.handlers) {
      try {
        h(ev);
      } catch (e) {
        logger.error({ error: String(e) }, "native event handler failed");
      }
    }
  }

  private startPing(): void {
    this.clearPing();
    this.pingTimer = setInterval(() => {
      if (this.socket?.readyState === WebSocket.OPEN) this.socket.send("2");
    }, PING_MS);
  }

  private startHealthMonitor(): void {
    if (this.healthTimer) clearInterval(this.healthTimer);
    this.healthTimer = setInterval(() => {
      const last = this.lastEventAt;
      if (!last) return;
      const lag = Date.now() - last;
      if (lag > DEGRADED_AFTER_MS && this.status === "connected") {
        this.setStatus("degraded", `no events ${lag}ms`);
      } else {
        prefetchSign();
      }
    }, 5_000);
  }

  private scheduleReconnect(): void {
    if (this.intentionalStop || this.status === "waf_blocked") return;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    const attempts = this.reconnectAttempts;
    const jitter = (Math.random() * 80) | 0;
    const delay = Math.min(RECONNECT_DELAY_MS * 2 ** attempts, RECONNECT_DELAY_MAX_MS) + jitter;
    this.reconnectAttempts = attempts + 1;
    this.setStatus("reconnecting");
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.connect();
    }, delay);
  }

  private handleWaf(message: string): void {
    this.setStatus("waf_blocked", message);
    this.clearTimers();
    try {
      this.socket?.close();
    } catch {
      /* soft */
    }
    logger.warn({ component: "native-bc-socket", ms: WAF_BACKOFF_MS }, "auth/waf backoff");
    this.wafTimer = setTimeout(() => {
      this.wafTimer = null;
      if (!this.intentionalStop) {
        this.setStatus("stopped");
        void this.connect();
      }
    }, WAF_BACKOFF_MS);
  }

  private clearPing(): void {
    if (this.pingTimer) clearInterval(this.pingTimer);
    this.pingTimer = null;
  }

  private clearTimers(): void {
    this.clearPing();
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    if (this.wafTimer) clearTimeout(this.wafTimer);
    this.wafTimer = null;
    if (this.healthTimer) clearInterval(this.healthTimer);
    this.healthTimer = null;
  }
}

export const nativeBcGameSocket = new NativeBcGameSocket();
