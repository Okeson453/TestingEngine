/**
 * BC.Game Socket.IO Client
 *
 * Spec: TestingEngine_Comprehensive_Diagnosis_and_Solution.md §2 (P0 Socket.IO)
 *
 * State machine:
 *   STOPPED → CONNECTING → CONNECTED → DEGRADED → RECONNECTING → CONNECTED
 * Only intentional shutdown permanently enters STOPPED.
 *
 * Transport: ["polling", "websocket"] with withCredentials to survive
 * WAF / origin restrictions that break pure WebSocket.
 */
import { io, type Socket, type ManagerOptions, type SocketOptions } from "socket.io-client";
import { getLogger } from "@/lib/observability/logger";
import { getSql } from "@/lib/db";

const logger = getLogger("bcgame-socket");

const SOCKET_URL = process.env.BCGAME_SOCKET_URL ?? "wss://socketv4.bc.game";
const SOCKET_PATH = process.env.BCGAME_SOCKET_PATH ?? "/socket.io";
const RECONNECT_DELAY_MS = 1_000;
const RECONNECT_DELAY_MAX_MS = 30_000;
const CONNECTION_TIMEOUT_MS = 20_000;
const WAF_BACKOFF_MS = Number(process.env.BCGAME_SOCKET_WAF_BACKOFF_MS ?? 60_000) || 60_000;
const DEGRADED_AFTER_MS = 45_000; // no ED/BG within this window → DEGRADED

export type BcGameEvent = "bg" | "pg" | "ed" | string;

export interface BcGameEventPayload {
  gameId: string;
  multiplier?: number;
  beganAt?: number | string;
  crashedAt?: number | string;
  [key: string]: unknown;
}

export interface SocketEvent {
  event: BcGameEvent;
  payload: BcGameEventPayload;
  receivedAt: string;
}

/** Explicit lifecycle states per diagnosis §2 */
export type ConnectionStatus =
  | "stopped"
  | "connecting"
  | "connected"
  | "degraded"
  | "reconnecting"
  | "waf_blocked";

export interface ConnectionState {
  status: ConnectionStatus;
  lastError: string | null;
  lastConnectedAt: string | null;
  lastDisconnectedAt: string | null;
  reconnectAttempts: number;
  socketId: string | null;
  transport: string | null;
  lastEdAt: string | null;
  lastBgAt: string | null;
  lastEventAt: string | null;
  lastEventKind: string | null;
  eventLagMs: number | null;
  totalReconnects: number;
}

export type EventHandler = (payload: BcGameEventPayload, event: BcGameEvent) => Promise<void>;
export type ConnectionHandler = (state: ConnectionState) => Promise<void>;
export type ErrorHandler = (error: Error, context: string) => Promise<void>;

export class BcGameSocketClient {
  private socket: Socket | null = null;
  private eventHandlers: Map<BcGameEvent, Set<EventHandler>> = new Map();
  private connectionHandlers: Set<ConnectionHandler> = new Set();
  private errorHandlers: Set<ErrorHandler> = new Set();
  private state: ConnectionState = {
    status: "stopped",
    lastError: null,
    lastConnectedAt: null,
    lastDisconnectedAt: null,
    reconnectAttempts: 0,
    socketId: null,
    transport: null,
    lastEdAt: null,
    lastBgAt: null,
    lastEventAt: null,
    lastEventKind: null,
    eventLagMs: null,
    totalReconnects: 0,
  };

  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private wafBackoffTimer: ReturnType<typeof setTimeout> | null = null;
  private healthTimer: ReturnType<typeof setInterval> | null = null;
  /** Only true after intentional stop(); never set by reconnect cleanup. */
  private intentionalShutdown = false;
  private discoveredEvents: Set<string> = new Set();
  /** Per-game last ED timestamp. Global lastEdAt is still updated for
   *  backward compatibility, but poll-worker must use Crash-specific lag
   *  to avoid false deferral when Dice/Limbo emit ed events. */
  private lastEdAtByGame: Map<string, string> = new Map();

  constructor() {
    for (const event of ["bg", "pg", "ed"] as const) {
      this.eventHandlers.set(event, new Set());
    }
  }

  on(event: BcGameEvent, handler: EventHandler): () => void {
    if (!this.eventHandlers.has(event)) {
      this.eventHandlers.set(event, new Set());
    }
    this.eventHandlers.get(event)!.add(handler);
    return () => {
      this.eventHandlers.get(event)?.delete(handler);
    };
  }

  onConnection(handler: ConnectionHandler): () => void {
    this.connectionHandlers.add(handler);
    void handler(this.state);
    return () => {
      this.connectionHandlers.delete(handler);
    };
  }

  onError(handler: ErrorHandler): () => void {
    this.errorHandlers.add(handler);
    return () => {
      this.errorHandlers.delete(handler);
    };
  }

  /** Crash-specific last ED time (or null). Prefer this over getState().lastEdAt
   *  when deciding whether the poll worker should defer. */
  getLastEdAtForGame(gameKey: string = "crash"): string | null {
    return this.lastEdAtByGame.get(gameKey) ?? null;
  }

  getState(): ConnectionState {
    return { ...this.state };
  }

  getDiscoveredEvents(): string[] {
    return Array.from(this.discoveredEvents);
  }

  /**
   * Connect (or reconnect). Never treats cleanup as intentional shutdown.
   */
  async connect(): Promise<void> {
    if (this.intentionalShutdown) {
      logger.info({ component: "BcGameSocketClient" }, "connect ignored — intentional shutdown");
      return;
    }
    if (this.socket?.connected && this.state.status === "connected") {
      logger.info({ component: "BcGameSocketClient" }, "Already connected");
      return;
    }
    if (this.state.status === "waf_blocked") {
      logger.info({ component: "BcGameSocketClient" }, "WAF blocked — waiting backoff");
      return;
    }
    if (this.state.status === "connecting" || this.state.status === "reconnecting") {
      return;
    }

    const isReconnect =
      this.state.reconnectAttempts > 0 ||
      this.state.status === "degraded" ||
      this.state.status === "reconnecting";

    this.updateState({
      status: isReconnect ? "reconnecting" : "connecting",
      lastError: null,
    });

    try {
      this.cleanupSocket(/* intentional */ false);

      // BC.Game enforces WebSocket-only (see streaming investigation).
      // Production logs showed "xhr poll error" when polling was preferred.
      // Polling only when BCGAME_SOCKET_ALLOW_POLLING=1.
      const allowPolling = process.env.BCGAME_SOCKET_ALLOW_POLLING === "1";
      const transports: ("websocket" | "polling")[] = allowPolling
        ? ["websocket", "polling"]
        : ["websocket"];

      const socketOptions: Partial<ManagerOptions & SocketOptions> = {
        path: SOCKET_PATH,
        reconnection: false, // manual lifecycle
        timeout: CONNECTION_TIMEOUT_MS,
        autoConnect: false,
        transports,
        // Node workers are not browsers; withCredentials is optional.
        withCredentials: process.env.BCGAME_SOCKET_WITH_CREDENTIALS === "1",
        extraHeaders: {
          "User-Agent":
            process.env.BCGAME_SOCKET_UA ??
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          Origin: process.env.BCGAME_SOCKET_ORIGIN ?? "https://bc.game",
          Referer: process.env.BCGAME_SOCKET_REFERER ?? "https://bc.game/game/crash",
        },
        upgrade: allowPolling,
        rememberUpgrade: true,
      };

      this.socket = io(SOCKET_URL, socketOptions);
      this.setupSocketHandlers();
      this.socket.connect();

      logger.info(
        {
          component: "BcGameSocketClient",
          url: SOCKET_URL,
          path: SOCKET_PATH,
          transports: socketOptions.transports,
          attempt: this.state.reconnectAttempts,
        },
        "Connecting to BC.Game Socket.IO",
      );
    } catch (error) {
      this.handleError(error as Error, "connect");
      this.updateState({
        status: "stopped",
        lastError: (error as Error).message,
      });
      this.scheduleReconnect();
    }
  }

  private setupSocketHandlers(): void {
    if (!this.socket) return;

    this.socket.on("connect", () => this.handleConnect());
    this.socket.on("disconnect", (reason) => this.handleDisconnect(reason));
    this.socket.on("connect_error", (error) => this.handleConnectError(error));
    this.socket.on("error", (error) => this.handleError(error as Error, "socket_error"));
    this.socket.onAny((event, payload) => this.handleIncomingEvent(event, payload));
  }

  private handleConnect(): void {
    if (!this.socket) return;

    const transport =
      (this.socket.io?.engine as { transport?: { name?: string } } | undefined)?.transport
        ?.name ?? null;

    this.updateState({
      status: "connected",
      lastConnectedAt: new Date().toISOString(),
      lastError: null,
      socketId: this.socket.id ?? null,
      transport,
      reconnectAttempts: 0,
    });

    logger.info(
      {
        component: "BcGameSocketClient",
        socketId: this.socket.id,
        transport,
      },
      "Connected to BC.Game Socket.IO",
    );

    this.subscribeToCrashEvents();
    this.startHealthMonitor();
  }

  private subscribeToCrashEvents(): void {
    if (!this.socket) return;
    // BC.Game room join — documented in streaming investigation
    this.socket.emit("join", "crash");
    logger.info({ component: "BcGameSocketClient" }, "Subscribed to crash game events");
  }

  private handleDisconnect(reason: string): void {
    if (this.intentionalShutdown) {
      this.updateState({
        status: "stopped",
        lastDisconnectedAt: new Date().toISOString(),
        lastError: `Shutdown: ${reason}`,
        socketId: null,
        transport: null,
      });
      return;
    }

    this.updateState({
      status: "reconnecting",
      lastDisconnectedAt: new Date().toISOString(),
      lastError: `Disconnected: ${reason}`,
      socketId: null,
      transport: null,
      totalReconnects: this.state.totalReconnects + 1,
    });

    logger.warn(
      { component: "BcGameSocketClient", reason },
      "Disconnected from BC.Game Socket.IO — will reconnect",
    );

    if (reason === "transport error" || reason.includes("403") || reason.includes("forbidden")) {
      this.handleWafBlock();
    } else {
      this.scheduleReconnect();
    }
  }

  private handleConnectError(error: Error): void {
    this.handleError(error, "connect_error");
    const msg = (error.message || String(error)).toLowerCase();
    const desc = String((error as Error & { description?: unknown }).description ?? "").toLowerCase();
    const combined = `${msg} ${desc}`;
    // Cloudflare bot challenge on socketv4.bc.game — common from datacenter IPs.
    if (
      combined.includes("403") ||
      combined.includes("forbidden") ||
      combined.includes("waf") ||
      combined.includes("cloudflare") ||
      combined.includes("cf-") ||
      combined.includes("just a moment") ||
      combined.includes("websocket error") ||
      combined.includes("xhr poll error") ||
      combined.includes("transport error")
    ) {
      logger.warn(
        {
          component: "BcGameSocketClient",
          error: error.message,
          description: (error as Error & { description?: unknown }).description,
        },
        "Socket connect blocked (likely Cloudflare). Poll worker remains primary recovery path.",
      );
      this.handleWafBlock();
    } else {
      this.updateState({
        status: "reconnecting",
        lastError: error.message,
        lastDisconnectedAt: new Date().toISOString(),
      });
      this.scheduleReconnect();
    }
  }

  private wafBlockCount = 0;

  private handleWafBlock(): void {
    this.wafBlockCount += 1;
    this.updateState({
      status: "waf_blocked",
      lastError: "WAF blocked connection",
      lastDisconnectedAt: new Date().toISOString(),
    });
    // Exponential backoff for repeated WAF blocks (cap 5 minutes)
    const backoffMs = Math.min(
      WAF_BACKOFF_MS * Math.pow(2, Math.min(this.wafBlockCount - 1, 4)),
      5 * 60 * 1000,
    );
    logger.error(
      {
        component: "BcGameSocketClient",
        wafBlockCount: this.wafBlockCount,
        backoffMs,
      },
      "WAF blocked connection — backing off then probing recovery",
    );
    this.clearTimers();
    this.wafBackoffTimer = setTimeout(() => {
      this.wafBackoffTimer = null;
      if (!this.intentionalShutdown) {
        logger.info(
          {
            component: "BcGameSocketClient",
            wafBlockCount: this.wafBlockCount,
          },
          "WAF backoff elapsed — active recovery probe (reconnect)",
        );
        this.updateState({ status: "stopped", lastError: null });
        void this.connect();
      }
    }, backoffMs);
  }

  private scheduleReconnect(): void {
    if (this.intentionalShutdown || this.state.status === "waf_blocked") return;
    this.clearReconnectTimer();

    const attempts = this.state.reconnectAttempts;
    const delay = Math.min(
      RECONNECT_DELAY_MS * Math.pow(2, attempts),
      RECONNECT_DELAY_MAX_MS,
    );

    this.updateState({
      reconnectAttempts: attempts + 1,
      status: "reconnecting",
    });

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (!this.intentionalShutdown) {
        void this.connect();
      }
    }, delay);

    logger.info(
      {
        component: "BcGameSocketClient",
        delay,
        attempts: attempts + 1,
      },
      "Scheduling reconnection",
    );
  }

  private startHealthMonitor(): void {
    this.stopHealthMonitor();
    this.healthTimer = setInterval(() => {
      if (this.intentionalShutdown || this.state.status === "stopped") return;
      const last = this.state.lastEventAt ?? this.state.lastConnectedAt;
      if (!last) return;
      const lag = Date.now() - new Date(last).getTime();
      if (
        lag > DEGRADED_AFTER_MS &&
        (this.state.status === "connected" || this.state.status === "degraded")
      ) {
        if (this.state.status !== "degraded") {
          this.updateState({ status: "degraded", eventLagMs: lag });
          logger.warn(
            { component: "BcGameSocketClient", lagMs: lag },
            "No ED/BG events — marking DEGRADED",
          );
        } else {
          this.updateState({ eventLagMs: lag });
        }
      }
    }, 10_000);
    this.healthTimer.unref?.();
  }

  private stopHealthMonitor(): void {
    if (this.healthTimer) {
      clearInterval(this.healthTimer);
      this.healthTimer = null;
    }
  }

  private handleIncomingEvent(event: string, payload: unknown): void {
    if (!this.discoveredEvents.has(event)) {
      this.discoveredEvents.add(event);
      void this.logDiscoveredEvent(event, payload);
    }

    const now = new Date().toISOString();
    const updates: Partial<ConnectionState> = {
      lastEventAt: now,
      lastEventKind: event,
      eventLagMs: 0,
    };
    if (event === "ed") {
      updates.lastEdAt = now;
      // Record Crash-specific ED when payload identifies the crash game.
      // normalizePayload runs after this; extract gameId opportunistically.
      try {
        const p = payload as Record<string, unknown> | null;
        const gid =
          (p && (p["gameId"] ?? p["game_id"] ?? p["id"])) != null
            ? String(p["gameId"] ?? p["game_id"] ?? p["id"])
            : "crash";
        this.lastEdAtByGame.set(String(gid), now);
        // Also always stamp the canonical "crash" key used by poll-worker
        // when the event is on the crash socket namespace.
        this.lastEdAtByGame.set("crash", now);
      } catch {
        this.lastEdAtByGame.set("crash", now);
      }
    }
    if (event === "bg") updates.lastBgAt = now;
    if (this.state.status === "degraded") updates.status = "connected";
    this.updateState(updates);

    if (this.eventHandlers.has(event as BcGameEvent)) {
      const handlers = this.eventHandlers.get(event as BcGameEvent)!;
      const eventPayload = this.normalizePayload(event, payload);
      if (eventPayload) {
        for (const handler of handlers) {
          void handler(eventPayload, event as BcGameEvent).catch((error) => {
            this.handleError(error as Error, `handler_${event}`);
          });
        }
      }
    }
  }

  private normalizePayload(event: string, payload: unknown): BcGameEventPayload | null {
    try {
      if (typeof payload !== "object" || payload === null) {
        logger.warn({ component: "BcGameSocketClient", event }, "Invalid payload type");
        return null;
      }
      const data = payload as Record<string, unknown>;
      const gameId = String(data.gameId ?? data.id ?? "");
      if (!gameId || !/^\d+$/.test(gameId)) {
        logger.warn(
          { component: "BcGameSocketClient", event, payload },
          "Invalid or missing gameId",
        );
        return null;
      }

      let beganAt: number | string | undefined;
      let crashedAt: number | string | undefined;
      let multiplier: number | undefined;

      if (event === "bg") {
        beganAt = (data.beganAt ?? data.beginTime ?? data.startTime) as number | string | undefined;
      } else if (event === "ed") {
        crashedAt = (data.crashedAt ?? data.endTime ?? data.crashTime) as
          | number
          | string
          | undefined;
        multiplier =
          typeof data.multiplier === "number"
            ? data.multiplier
            : typeof data.rate === "number"
              ? data.rate
              : typeof data.multiplier === "string"
                ? parseFloat(data.multiplier)
                : undefined;
      } else if (event === "pg") {
        multiplier =
          typeof data.multiplier === "number"
            ? data.multiplier
            : typeof data.current === "number"
              ? data.current
              : undefined;
      }

      return { gameId, multiplier, beganAt, crashedAt, ...data };
    } catch (error) {
      logger.error(
        { component: "BcGameSocketClient", event, error },
        "Failed to normalize payload",
      );
      return null;
    }
  }

  private async logDiscoveredEvent(event: string, payload: unknown): Promise<void> {
    try {
      const sql = await getSql();
      await sql`
        insert into socket_event_discovery (event_name, payload, received_at)
        values (${event}, ${JSON.stringify(payload)}, now())
        on conflict (event_name) do nothing
      `;
    } catch (error) {
      logger.warn(
        { component: "BcGameSocketClient", error: String(error) },
        "Failed to log discovered event",
      );
    }
  }

  private handleError(error: Error, context: string): void {
    logger.error({ component: "BcGameSocketClient", context, error: error.message }, error.message);
    for (const handler of this.errorHandlers) {
      void handler(error, context).catch(() => undefined);
    }
  }

  private updateState(updates: Partial<ConnectionState>): void {
    this.state = { ...this.state, ...updates };
    for (const handler of this.connectionHandlers) {
      void handler(this.state).catch(() => undefined);
    }
  }

  /**
   * Internal cleanup of the socket instance without marking intentional shutdown.
   * Used by reconnect path so scheduleReconnect still works.
   */
  private cleanupSocket(intentional: boolean): void {
    this.clearTimers();
    this.stopHealthMonitor();
    if (this.socket) {
      try {
        this.socket.removeAllListeners();
        this.socket.disconnect();
      } catch {
        /* ignore */
      }
      this.socket = null;
    }
    if (intentional) {
      this.intentionalShutdown = true;
    }
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private clearTimers(): void {
    this.clearReconnectTimer();
    if (this.wafBackoffTimer) {
      clearTimeout(this.wafBackoffTimer);
      this.wafBackoffTimer = null;
    }
  }

  /**
   * Intentional permanent disconnect (worker shutdown only).
   */
  disconnect(): void {
    this.cleanupSocket(/* intentional */ true);
    this.updateState({
      status: "stopped",
      lastDisconnectedAt: new Date().toISOString(),
      socketId: null,
      transport: null,
    });
    logger.info({ component: "BcGameSocketClient" }, "Intentional disconnect — STOPPED");
  }

  /**
   * Allow a later connect() after an intentional stop (e.g. tests).
   */
  resetShutdownFlag(): void {
    this.intentionalShutdown = false;
  }

  isConnected(): boolean {
    return this.state.status === "connected" || this.state.status === "degraded";
  }

  isActive(): boolean {
    return (
      this.state.status === "connected" ||
      this.state.status === "connecting" ||
      this.state.status === "reconnecting" ||
      this.state.status === "degraded"
    );
  }
}

export const bcGameSocket = new BcGameSocketClient();

export function initializeSocketClient(): BcGameSocketClient {
  return bcGameSocket;
}
