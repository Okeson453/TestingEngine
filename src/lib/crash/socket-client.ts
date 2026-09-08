/**
 * BC.Game Socket Client — production facade over the confirmed transport.
 *
 * Spec: docs/bcgame-crash-transport-report.md §6 (confirmed defects fixed)
 *
 * This module preserves the public API used by ingest / diagnostics / health
 * while delegating the wire protocol to BcGameCrashTransport:
 *   - Engine.IO v3 + custom T8 binary parser
 *   - WASM t1/t2 sign → /test/ → p/t query
 *   - Namespace /g/cm + ack-based join (no "crash" arg)
 *   - Protobuf payloads for pr/bg/pg/e/ed/st
 *
 * State machine:
 *   STOPPED → CONNECTING → CONNECTED → DEGRADED → RECONNECTING → CONNECTED
 *   WAF/auth failures → waf_blocked → backoff → probe
 */
import { getLogger } from "@/lib/observability/logger";
import { getSql } from "@/lib/db";
import {
  BcGameCrashTransport,
  type CrashEvent,
  type CrashEventName,
  type TransportState,
} from "./transport/bcgame-crash-transport";

const logger = getLogger("bcgame-socket");

const RECONNECT_DELAY_MS = 1_000;
const RECONNECT_DELAY_MAX_MS = 30_000;
const WAF_BACKOFF_MS = Number(process.env.BCGAME_SOCKET_WAF_BACKOFF_MS ?? 10_000) || 10_000;
const DEGRADED_AFTER_MS = Number(process.env.BCGAME_SOCKET_DEGRADED_AFTER_MS ?? 30_000) || 30_000;

export type BcGameEvent = "pr" | "bg" | "pg" | "e" | "ed" | "st" | string;

export interface BcGameEventPayload {
  gameId: string;
  multiplier?: number;
  beganAt?: number | string;
  crashedAt?: number | string;
  hash?: string | null;
  elapsedMs?: number | null;
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

function isAuthOrWaf(message: string): boolean {
  const lower = message.toLowerCase();
  return (
    lower.includes("401") ||
    lower.includes("403") ||
    lower.includes("503") ||
    lower.includes("auth") ||
    lower.includes("cloudflare") ||
    lower.includes("waf") ||
    lower.includes("forbidden") ||
    lower.includes("just a moment") ||
    lower.includes("sign")
  );
}

/** Map transport CrashEvent → legacy BcGameEventPayload for existing handlers. */
function crashEventToPayload(ev: CrashEvent): BcGameEventPayload | null {
  const receivedAt = ev.receivedAt;
  switch (ev.event) {
    case "pr":
      return {
        gameId: String(ev.roundId),
        beganAt: ev.startTime || ev.prepareTime || receivedAt,
        multiplier: undefined,
      };
    case "bg":
      return {
        gameId: String(ev.roundId),
        beganAt: ev.startTime || receivedAt,
        multiplier: undefined,
      };
    case "pg":
      if (!ev.roundId) {
        return {
          gameId: "",
          multiplier: ev.multiplier,
          elapsedMs: ev.elapsed,
        };
      }
      return {
        gameId: String(ev.roundId),
        multiplier: ev.multiplier,
        elapsedMs: ev.elapsed,
      };
    case "e":
      return {
        gameId: "",
        userId: ev.userId,
        betId: ev.betId,
        odds: ev.odds,
        force: ev.force,
        betIndex: ev.betIndex,
        multiplier: ev.odds / 100,
      };
    case "ed":
      return {
        gameId: String(ev.roundId),
        multiplier: ev.multiplier,
        hash: ev.hash || null,
        crashedAt: receivedAt,
      };
    case "st":
      return {
        gameId: String(ev.roundId),
        multiplier: ev.multiplier,
        hash: ev.hash || null,
        crashedAt: receivedAt,
        escapes: ev.escapes,
      };
    default:
      return null;
  }
}

export class BcGameSocketClient {
  private transport: BcGameCrashTransport | null = null;
  private eventHandlers: Map<BcGameEvent, Set<EventHandler>> = new Map();
  private connectionHandlers: Set<ConnectionHandler> = new Set();
  private errorHandlers: Set<ErrorHandler> = new Set();
  private unsubs: Array<() => void> = [];
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
  private intentionalShutdown = false;
  private discoveredEvents: Set<string> = new Set();
  private lastEdAtByGame: Map<string, string> = new Map();
  private currentGameId: string | null = null;
  private wafBlockCount = 0;
  private lastPersistedSocketStatus: string | null = null;
  private lastPersistAt = 0;
  private statePollTimer: ReturnType<typeof setInterval> | null = null;

  constructor() {
    for (const event of ["pr", "bg", "pg", "e", "ed", "st"] as const) {
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

  getLastEdAtForGame(gameKey: string = "crash"): string | null {
    return this.lastEdAtByGame.get(gameKey) ?? null;
  }

  getState(): ConnectionState {
    return { ...this.state };
  }

  getDiscoveredEvents(): string[] {
    return Array.from(this.discoveredEvents);
  }

  async connect(): Promise<void> {
    if (this.intentionalShutdown) {
      logger.info({ component: "BcGameSocketClient" }, "connect ignored — intentional shutdown");
      return;
    }
    if (this.state.status === "connected" || this.state.status === "connecting") {
      return;
    }
    if (this.state.status === "waf_blocked") {
      logger.info({ component: "BcGameSocketClient" }, "WAF blocked — waiting backoff");
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
      this.teardownTransport();

      const transport = new BcGameCrashTransport({
        socketHost: process.env.BCGAME_SOCKET_URL ?? "wss://socketv4.bc.game",
        userAgent:
          process.env.BCGAME_SOCKET_UA ??
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        autoReconnect: false,
        log: (msg, data) => {
          logger.info({ component: "BcGameCrashTransport", data }, msg);
        },
      });

      for (const name of ["pr", "bg", "pg", "e", "ed", "st"] as CrashEventName[]) {
        this.unsubs.push(
          transport.on(name, (ev) => {
            void this.handleTransportEvent(ev);
          }),
        );
      }

      this.transport = transport;
      this.startStatePoll();

      await transport.connect();

      const ts = transport.getState();
      if (ts.status === "connected") {
        this.onTransportConnected(ts);
      } else if (ts.lastError && isAuthOrWaf(ts.lastError)) {
        this.handleWafBlock(ts.lastError);
      } else if (ts.status === "error" || ts.lastError) {
        this.updateState({
          status: "reconnecting",
          lastError: ts.lastError,
          lastDisconnectedAt: new Date().toISOString(),
        });
        this.scheduleReconnect();
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      this.handleError(error instanceof Error ? error : new Error(msg), "connect");
      if (isAuthOrWaf(msg)) {
        this.handleWafBlock(msg);
      } else {
        this.updateState({
          status: "reconnecting",
          lastError: msg,
          lastDisconnectedAt: new Date().toISOString(),
        });
        this.scheduleReconnect();
      }
    }
  }

  private onTransportConnected(ts: TransportState): void {
    this.updateState({
      status: "connected",
      lastConnectedAt: new Date().toISOString(),
      lastError: null,
      socketId: ts.sid,
      transport: "websocket",
      reconnectAttempts: 0,
    });
    this.wafBlockCount = 0;
    this.startHealthMonitor();
    logger.info(
      {
        component: "BcGameSocketClient",
        socketId: ts.sid,
        transport: "websocket",
      },
      "Connected to BC.Game (confirmed transport)",
    );
  }

  private handleTransportEvent(ev: CrashEvent): void {
    const name = ev.event as BcGameEvent;
    if (!this.discoveredEvents.has(name)) {
      this.discoveredEvents.add(name);
      logger.info({ component: "BcGameSocketClient", event: name }, "Discovered crash event");
    }

    if (ev.event === "pr" || ev.event === "bg") {
      this.currentGameId = String(ev.roundId);
    }

    let payload = crashEventToPayload(ev);
    if (!payload) return;

    if (!payload.gameId && this.currentGameId) {
      payload = { ...payload, gameId: this.currentGameId };
    }

    if ((name === "pg" || name === "e") && !payload.gameId) {
      return;
    }

    if (["pr", "bg", "ed", "st"].includes(name) && (!payload.gameId || !/^\d+$/.test(payload.gameId))) {
      return;
    }

    const now = new Date().toISOString();
    const updates: Partial<ConnectionState> = {
      lastEventAt: now,
      lastEventKind: name,
      eventLagMs: 0,
    };
    if (name === "ed") updates.lastEdAt = now;
    if (name === "bg") updates.lastBgAt = now;
    if (this.state.status === "degraded") updates.status = "connected";
    this.updateState(updates);

    if (name === "ed" && payload.gameId) {
      this.lastEdAtByGame.set(String(payload.gameId), now);
      this.lastEdAtByGame.set("crash", now);
    }

    const handlers = this.eventHandlers.get(name);
    if (!handlers || handlers.size === 0) return;
    for (const handler of handlers) {
      void handler(payload, name).catch((error) => {
        this.handleError(error as Error, `handler_${name}`);
      });
    }
  }

  private startStatePoll(): void {
    this.stopStatePoll();
    this.statePollTimer = setInterval(() => {
      if (!this.transport || this.intentionalShutdown) return;
      const ts = this.transport.getState();
      if (ts.status === "connected" && this.state.status !== "connected" && this.state.status !== "degraded") {
        this.onTransportConnected(ts);
      } else if (
        (ts.status === "reconnecting" || ts.status === "error") &&
        this.state.status === "connected"
      ) {
        this.updateState({
          status: "reconnecting",
          lastError: ts.lastError,
          lastDisconnectedAt: new Date().toISOString(),
          socketId: null,
          transport: null,
          totalReconnects: this.state.totalReconnects + 1,
        });
        if (ts.lastError && isAuthOrWaf(ts.lastError)) {
          this.handleWafBlock(ts.lastError);
        } else {
          this.scheduleReconnect();
        }
      }
    }, 2_000);
    this.statePollTimer.unref?.();
  }

  private stopStatePoll(): void {
    if (this.statePollTimer) {
      clearInterval(this.statePollTimer);
      this.statePollTimer = null;
    }
  }

  private handleWafBlock(message?: string): void {
    this.wafBlockCount += 1;
    this.updateState({
      status: "waf_blocked",
      lastError: message ?? "WAF blocked connection",
      lastDisconnectedAt: new Date().toISOString(),
    });
    const backoffMs = Math.min(
      WAF_BACKOFF_MS * Math.pow(2, Math.min(this.wafBlockCount - 1, 2)),
      30_000,
    );
    logger.error(
      {
        component: "BcGameSocketClient",
        wafBlockCount: this.wafBlockCount,
        backoffMs,
      },
      "WAF/auth blocked — backing off then probing recovery",
    );
    this.clearTimers();
    this.teardownTransport();
    this.wafBackoffTimer = setTimeout(() => {
      this.wafBackoffTimer = null;
      if (!this.intentionalShutdown) {
        this.updateState({ status: "stopped", lastError: null });
        void this.connect();
      }
    }, backoffMs);
  }

  private scheduleReconnect(): void {
    if (this.intentionalShutdown || this.state.status === "waf_blocked") return;
    this.clearReconnectTimer();

    const attempts = this.state.reconnectAttempts;
    const delay = Math.min(RECONNECT_DELAY_MS * Math.pow(2, attempts), RECONNECT_DELAY_MAX_MS);

    this.updateState({
      reconnectAttempts: attempts + 1,
      status: "reconnecting",
      totalReconnects: this.state.totalReconnects + (attempts === 0 ? 1 : 0),
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
            "No events — marking DEGRADED",
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

  private handleError(error: Error, context: string): void {
    logger.warn(
      { component: "BcGameSocketClient", context, error: error.message },
      "Socket error",
    );
    for (const handler of this.errorHandlers) {
      void handler(error, context).catch(() => {});
    }
  }

  private updateState(updates: Partial<ConnectionState>): void {
    this.state = { ...this.state, ...updates };
    for (const handler of this.connectionHandlers) {
      void handler(this.state).catch(() => {});
    }
    if (
      updates.status === "waf_blocked" ||
      updates.status === "degraded" ||
      updates.status === "reconnecting"
    ) {
      if (updates.status === "waf_blocked" || updates.status === "degraded") {
        logger.warn(
          {
            component: "BcGameSocketClient",
            alert: "socket_path_at_risk",
            status: updates.status,
            lastError: this.state.lastError,
            reconnectAttempts: this.state.reconnectAttempts,
            wafBlockCount: this.wafBlockCount,
          },
          `Socket path at risk (${updates.status}) — poll recovery will dominate latency`,
        );
      }
      this.persistSocketStatus(updates.status);
    } else if (updates.status === "connected" || updates.status === "stopped") {
      this.persistSocketStatus(updates.status);
    }
  }

  private persistSocketStatus(status: ConnectionStatus): void {
    const now = Date.now();
    if (status === this.lastPersistedSocketStatus && now - this.lastPersistAt < 5_000) {
      return;
    }
    this.lastPersistedSocketStatus = status;
    this.lastPersistAt = now;
    void (async () => {
      try {
        const sql = await getSql();
        await sql`
          INSERT INTO worker_state (key, value, updated_at)
          VALUES ('socket_status', ${status}, now())
          ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()
        `;
        await sql`
          INSERT INTO worker_state (key, value, updated_at)
          VALUES ('socket_waf_blocked', ${status === "waf_blocked" ? "1" : "0"}, now())
          ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()
        `;
        await sql`
          INSERT INTO worker_state (key, value, updated_at)
          VALUES ('socket_last_error', ${this.state.lastError ?? ""}, now())
          ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()
        `;
      } catch {
        /* soft */
      }
    })();
  }

  private teardownTransport(): void {
    for (const off of this.unsubs) {
      try {
        off();
      } catch {
        /* ignore */
      }
    }
    this.unsubs = [];
    if (this.transport) {
      try {
        this.transport.disconnect();
      } catch {
        /* ignore */
      }
      this.transport = null;
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
    this.stopHealthMonitor();
    this.stopStatePoll();
    if (this.wafBackoffTimer) {
      clearTimeout(this.wafBackoffTimer);
      this.wafBackoffTimer = null;
    }
  }

  disconnect(): void {
    this.intentionalShutdown = true;
    this.clearTimers();
    this.teardownTransport();
    this.updateState({
      status: "stopped",
      lastDisconnectedAt: new Date().toISOString(),
      socketId: null,
      transport: null,
    });
    logger.info({ component: "BcGameSocketClient" }, "Intentional disconnect — STOPPED");
  }

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
