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
const RECONNECT_DELAY_MS = 150;
const RECONNECT_DELAY_MAX_MS = 1_500;
const WAF_BACKOFF_MS = Number(process.env.WAF_BACKOFF_MS ?? 12_000);
/**
 * WS lifecycle state machine (Diagnosis fix 4/5). Explicit states:
 *   stopped → connecting → socket_open → connected(=namespace joined) →
 *   degraded → reconnecting. `socket_open` means the Engine.IO transport is
 *   open but /g/cm is NOT joined yet — that is NOT a live crash stream.
 * Statuses surfaced to consumers: "connected" only once the namespace is
 * joined AND fresh ED events confirm the stream (see poll/health snapshot).
 */
/** No crash events for this long → status "degraded" (fix 5: event age, not timer cycles). */
const LIVE_EVENT_TIMEOUT_MS = Number(process.env.NATIVE_WS_DEGRADED_MS ?? 15_000);
/** No crash events for this long → force reconnect (keep path hot). */
const RECONNECT_TIMEOUT_MS = Number(process.env.NATIVE_WS_STALE_MS ?? 25_000);
/** 5s keepalive — match BC.Game Engine.IO pingInterval. */
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
  private joinedAt: number | null = null;
  private currentGameId: string | null = null;
  private watchdogTimer: ReturnType<typeof setInterval> | null = null;
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

  /** Fix 4/14: health consumers must know whether /g/cm is actually joined. */
  isJoined(): boolean {
    return this.joined;
  }

  async start(): Promise<void> {
    this.intentionalStop = false;
    if (
      this.status === "connected" ||
      this.status === "connecting" ||
      this.status === "socket_open"
    )
      return;
    await this.connect();
  }

  async stop(): Promise<void> {
    this.intentionalStop = true;
    this.clearTimers();
    if (this.watchdogTimer) {
      clearInterval(this.watchdogTimer);
      this.watchdogTimer = null;
    }
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
    this.joinedAt = null;
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
        // node `ws` delivers text frames as Buffer with isBinary=false.
        // Treating those as binary skipped EIO open (0{sid...}) → never
        // encodeConnect/join → server 1006 close ~60s later (seen in prod).
        if (!isBinary) {
          const text =
            typeof data === "string"
              ? data
              : Buffer.isBuffer(data)
                ? data.toString("utf8")
                : data instanceof ArrayBuffer
                  ? Buffer.from(data).toString("utf8")
                  : String(data);
          this.onText(text);
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
        this.joined = false;
        this.joinedAt = null;
        // 1006 = abnormal; reconnect immediately (not WAF)
        if (!this.intentionalStop) {
          if (code === 1006 || code === 1001) this.reconnectAttempts = 0;
          this.scheduleReconnect();
        }
      });

      socket.on("error", (err) => {
        logger.warn({ component: "native-bc-socket", error: err.message }, "error");
        if (isAuthOrWaf(err)) this.handleWaf(err.message);
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      logger.warn({ component: "native-bc-socket", error: message }, "connect failed");
      // Sign miss is transient — retry quickly; do not mark waf_blocked (blocks reconnect).
      if (message.includes("sign unavailable") || message.includes("sign failed")) {
        this.setStatus("reconnecting", message);
        this.reconnectAttempts = Math.min(this.reconnectAttempts + 1, 5);
        this.scheduleReconnect();
        return;
      }
      if (isAuthOrWaf(err)) this.handleWaf(message);
      else this.scheduleReconnect();
    }
  }

  private onText(asText: string): void {
    if (asText.charCodeAt(0) === 0x30 && asText.charCodeAt(1) === 0x7b) {
      try {
        const payload = JSON.parse(asText.slice(1)) as { sid?: string; pingInterval?: number };
        this.reconnectAttempts = 0;
        // Fix 4: EIO open ≠ live. Transport open is "socket_open"; the
        // crash stream is only "connected" after /g/cm is joined.
        this.setStatus("socket_open");
        logger.info(
          {
            component: "native-bc-socket",
            sid: payload.sid ?? null,
            pingInterval: payload.pingInterval ?? null,
          },
          "EIO open — connecting namespace /g/cm",
        );
        this.socket?.send(encodeConnect(NSP));
        // Match server pingInterval when present (BC.Game typically 5000).
        const pingMs =
          typeof payload.pingInterval === "number" && payload.pingInterval > 0
            ? Math.max(2_000, Math.min(payload.pingInterval, 25_000))
            : PING_MS;
        this.startPing(pingMs);
        this.startHealthMonitor();
        prefetchSign();
      } catch (e) {
        logger.warn(
          { component: "native-bc-socket", error: String(e), head: asText.slice(0, 80) },
          "EIO open parse failed",
        );
      }
      return;
    }
    if (asText === "2") {
      this.socket?.send("3");
      return;
    }
    // Some stacks send ping as "2probe" style — ignore unknowns
  }

  private onBinary(buf: Uint8Array): void {
    if (isEnginePing(buf)) {
      this.socket?.send("3");
      return;
    }
    if (isEnginePong(buf) || isEngineOpenFrame(buf)) return;

    const packet = parsePacket(buf);
    if (packet.kind === "connect") {
      logger.info(
        { component: "native-bc-socket", nsp: packet.nsp, alreadyJoined: this.joined },
        "namespace connect packet",
      );
      if ((packet.nsp === NSP || packet.nsp === "") && !this.joined) {
        this.joined = true;
        this.joinedAt = Date.now();
        this.socket?.send(encodeJoin(NSP));
        // Fix 4: namespace joined → crash stream is now actually live.
        this.setStatus("connected");
        logger.info({ component: "native-bc-socket" }, "joined /g/cm — stream live");
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
    if (packet.event === "ed" || packet.event === "bg") {
      logger.info(
        { component: "native-bc-socket", event: packet.event, gameId, multiplier },
        "crash event from native WS",
      );
    }

    const receivedAt = Date.now();
    this.lastEventAt = receivedAt;
    if (packet.event === "ed") this.lastEdAt = receivedAt;
    // Fix 5: recover from degraded purely on fresh event evidence.
    if (this.status === "degraded" && this.joined) this.setStatus("connected");

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
    // Metrics only — never drop live crash events from the prediction path.
    try {
      getRealtimePipeline().observe(ev);
    } catch (e) {
      logger.warn({ error: String(e) }, "realtime pipeline observe soft-failed");
    }

    for (const h of this.handlers) {
      try {
        h(ev);
      } catch (e) {
        logger.error({ error: String(e) }, "native event handler failed");
      }
    }
  }

  private startPing(intervalMs: number = PING_MS): void {
    this.clearPing();
    this.pingTimer = setInterval(() => {
      if (this.socket?.readyState === WebSocket.OPEN) this.socket.send("2");
    }, intervalMs);
  }

  private startHealthMonitor(): void {
    if (this.healthTimer) clearInterval(this.healthTimer);
    this.healthTimer = setInterval(() => {
      const last = this.lastEventAt;
      if (last) {
        const lag = Date.now() - last;
        // Fix 5: degrade on event AGE (LIVE_EVENT_TIMEOUT_MS), regardless of
        // whether the transport is socket_open or joined.
        const streamState = this.status === "connected" || this.status === "socket_open";
        if (lag > LIVE_EVENT_TIMEOUT_MS && streamState) {
          this.setStatus("degraded", `no events ${lag}ms`);
        }
      }
      prefetchSign();
    }, 5_000);
    this.startWatchdog();
  }

  /**
   * Always-on watchdog: if we never join, or join but receive no crash events,
   * tear down and reconnect so the live path stays primary over poll.
   * Fix 5: the 5s tick is only a SAMPLING interval — the reconnect decision
   * is based on event age (RECONNECT_TIMEOUT_MS), never on timer cycles.
   */
  private startWatchdog(): void {
    if (this.watchdogTimer) return;
    this.watchdogTimer = setInterval(() => {
      if (this.intentionalStop) return;
      if (this.status === "waf_blocked" || this.status === "connecting" || this.status === "reconnecting") {
        return;
      }
      const now = Date.now();
      // Opened but never joined namespace
      if (
        (this.status === "connected" || this.status === "socket_open") &&
        !this.joined
      ) {
        logger.warn({ component: "native-bc-socket" }, "watchdog: transport open but not joined — reconnect");
        this.forceReconnect("not_joined");
        return;
      }
      // Joined but silent too long (missed ed cycle) — event-age decision
      const anchor = this.lastEventAt ?? this.joinedAt;
      if (this.joined && anchor && now - anchor > RECONNECT_TIMEOUT_MS) {
        logger.warn(
          { component: "native-bc-socket", silentMs: now - anchor },
          "watchdog: no crash events — reconnect",
        );
        this.forceReconnect("stale_events");
      }
    }, 5_000);
  }

  private forceReconnect(reason: string): void {
    logger.info({ component: "native-bc-socket", reason }, "force reconnect");
    try {
      this.socket?.close();
    } catch {
      /* soft */
    }
    this.socket = null;
    this.joined = false;
    this.joinedAt = null;
    this.clearPing();
    // Immediate reconnect (bypass exponential if we had a prior session)
    this.reconnectAttempts = Math.min(this.reconnectAttempts, 2);
    this.scheduleReconnect();
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
        this.reconnectAttempts = 0;
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
    // Keep watchdog alive across reconnects while start() is active
  }
}

export const nativeBcGameSocket = new NativeBcGameSocket();
