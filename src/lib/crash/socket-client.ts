/**
 * BC.Game Socket.IO Client
 * Implements the live event-driven connection to BC.Game's WebSocket API
 * for real-time round start (bg), progress (pg), and end (ed) events.
 * 
 * Specification: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §2, §13
 */

import { io, type Socket, type ManagerOptions, type SocketOptions } from "socket.io-client";
import { getLogger } from "@/lib/observability/logger";
import { getSql } from "@/lib/db";
import type { Sql } from "@/lib/db";

const logger = getLogger("bcgame-socket");

// BC.Game Socket.IO configuration
const SOCKET_URL = "wss://socketv4.bc.game/socket.io";
const SOCKET_PATH = "/socket.io";
const RECONNECT_DELAY_MS = 1000;
const RECONNECT_DELAY_MAX_MS = 30000;
const CONNECTION_TIMEOUT_MS = 20000;
const WAF_BACKOFF_MS = 5 * 60 * 1000; // 5 minutes for WAF challenges

// Event names as documented in bcgame-crash-streaming-pipeline-investigation.md
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

// Connection state types
export type ConnectionStatus = 
  | "disconnected"
  | "connecting"
  | "connected"
  | "reconnecting"
  | "waf_blocked";

export interface ConnectionState {
  status: ConnectionStatus;
  lastError: string | null;
  lastConnectedAt: string | null;
  lastDisconnectedAt: string | null;
  reconnectAttempts: number;
  socketId: string | null;
}

// Event handler types
export type EventHandler = (payload: BcGameEventPayload, event: BcGameEvent) => Promise<void>;
export type ConnectionHandler = (state: ConnectionState) => Promise<void>;
export type ErrorHandler = (error: Error, context: string) => Promise<void>;

/**
 * BC.Game Socket.IO Client Manager
 * Manages the WebSocket connection to BC.Game and distributes events to handlers.
 */
export class BcGameSocketClient {
  private socket: Socket | null = null;
  private eventHandlers: Map<BcGameEvent, Set<EventHandler>> = new Map();
  private connectionHandlers: Set<ConnectionHandler> = new Set();
  private errorHandlers: Set<ErrorHandler> = new Set();
  private state: ConnectionState = {
    status: "disconnected",
    lastError: null,
    lastConnectedAt: null,
    lastDisconnectedAt: null,
    reconnectAttempts: 0,
    socketId: null,
  };
  
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private wafBackoffTimer: ReturnType<typeof setTimeout> | null = null;
  private isShuttingDown = false;
  
  // Track discovered events for observability (spec §13.5)
  private discoveredEvents: Set<string> = new Set();

  constructor() {
    // Initialize event handler maps for known event types
    ["bg", "pg", "ed"].forEach(event => {
      this.eventHandlers.set(event, new Set());
    });
  }

  /**
   * Register an event handler for a specific event type
   */
  on(event: BcGameEvent, handler: EventHandler): () => void {
    if (!this.eventHandlers.has(event)) {
      this.eventHandlers.set(event, new Set());
    }
    this.eventHandlers.get(event)!.add(handler);
    
    return () => {
      this.eventHandlers.get(event)?.delete(handler);
    };
  }

  /**
   * Register a connection state change handler
   */
  onConnection(handler: ConnectionHandler): () => void {
    this.connectionHandlers.add(handler);
    // Immediately notify of current state
    void handler(this.state);
    
    return () => {
      this.connectionHandlers.delete(handler);
    };
  }

  /**
   * Register an error handler
   */
  onError(handler: ErrorHandler): () => void {
    this.errorHandlers.add(handler);
    return () => {
      this.errorHandlers.delete(handler);
    };
  }

  /**
   * Get current connection state
   */
  getState(): ConnectionState {
    return { ...this.state };
  }

  /**
   * Get discovered event types
   */
  getDiscoveredEvents(): string[] {
    return Array.from(this.discoveredEvents);
  }

  /**
   * Connect to BC.Game Socket.IO server
   */
  async connect(): Promise<void> {
    if (this.socket && this.state.status === "connected") {
      logger.info({ component: "BcGameSocketClient" }, "Already connected");
      return;
    }

    if (this.state.status === "waf_blocked") {
      logger.info({ component: "BcGameSocketClient" }, "Connection blocked by WAF, waiting for backoff");
      return;
    }

    this.updateState({ status: "connecting", lastError: null });
    
    try {
      // Clear any existing socket
      this.disconnect();

      const socketOptions: Partial<ManagerOptions & SocketOptions> = {
        path: SOCKET_PATH,
        reconnection: false, // We handle reconnection manually
        timeout: CONNECTION_TIMEOUT_MS,
        autoConnect: false,
        transports: ["websocket"], // Force WebSocket only
        // Headers to mimic browser requests
        extraHeaders: {
          "User-Agent": "Mozilla/5.0 (compatible; TestingEngine/1.0)",
          "Origin": "https://bc.game",
          "Referer": "https://bc.game/game/crash",
        },
      };

      this.socket = io(SOCKET_URL, socketOptions);
      
      // Set up connection event handlers
      this.setupSocketHandlers();
      
      // Connect the socket
      this.socket.connect();
      
      logger.info({ component: "BcGameSocketClient" }, `Connecting to ${SOCKET_URL}`);
      
    } catch (error) {
      this.handleError(error as Error, "connect");
      this.updateState({ status: "disconnected", lastError: (error as Error).message });
      this.scheduleReconnect();
    }
  }

  /**
   * Set up all socket event handlers
   */
  private setupSocketHandlers(): void {
    if (!this.socket) return;

    this.socket.on("connect", () => {
      this.handleConnect();
    });

    this.socket.on("disconnect", (reason) => {
      this.handleDisconnect(reason);
    });

    this.socket.on("connect_error", (error) => {
      this.handleConnectError(error);
    });

    this.socket.on("error", (error) => {
      this.handleError(error, "socket_error");
    });

    // Handle all incoming events
    this.socket.onAny((event, payload) => {
      this.handleIncomingEvent(event, payload);
    });
  }

  /**
   * Handle successful connection
   */
  private handleConnect(): void {
    if (!this.socket) return;

    this.reconnectAttempts = 0;
    this.updateState({
      status: "connected",
      lastConnectedAt: new Date().toISOString(),
      lastError: null,
      socketId: this.socket.id,
    });

    logger.info(
      { component: "BcGameSocketClient", socketId: this.socket.id },
      "Connected to BC.Game Socket.IO"
    );

    // Subscribe to crash game events
    this.subscribeToCrashEvents();
  }

  /**
   * Subscribe to crash game events
   * BC.Game uses room-based subscriptions
   */
  private subscribeToCrashEvents(): void {
    if (!this.socket) return;

    // Join the crash game room
    this.socket.emit("join", "crash");
    logger.info({ component: "BcGameSocketClient" }, "Subscribed to crash game events");
  }

  /**
   * Handle disconnection
   */
  private handleDisconnect(reason: string): void {
    this.updateState({
      status: "disconnected",
      lastDisconnectedAt: new Date().toISOString(),
      lastError: `Disconnected: ${reason}`,
    });

    logger.warn(
      { component: "BcGameSocketClient", reason },
      "Disconnected from BC.Game Socket.IO"
    );

    // Check if this is a WAF block (403 Forbidden)
    if (reason === "transport error" || reason.includes("403")) {
      this.handleWafBlock();
    } else {
      this.scheduleReconnect();
    }
  }

  /**
   * Handle connection error
   */
  private handleConnectError(error: Error): void {
    this.handleError(error, "connect_error");
    
    // Check for WAF challenges
    if (error.message.includes("403") || error.message.includes("Forbidden")) {
      this.handleWafBlock();
    } else {
      this.updateState({
        status: "disconnected",
        lastError: error.message,
        lastDisconnectedAt: new Date().toISOString(),
      });
      this.scheduleReconnect();
    }
  }

  /**
   * Handle WAF block with backoff
   */
  private handleWafBlock(): void {
    this.updateState({
      status: "waf_blocked",
      lastError: "WAF blocked connection",
      lastDisconnectedAt: new Date().toISOString(),
    });

    logger.error(
      { component: "BcGameSocketClient" },
      "WAF blocked connection, backing off for 5 minutes"
    );

    // Clear any existing reconnect timer
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    // Set WAF backoff timer
    this.wafBackoffTimer = setTimeout(() => {
      this.wafBackoffTimer = null;
      this.updateState({ status: "disconnected", lastError: null });
      this.connect();
    }, WAF_BACKOFF_MS);
  }

  /**
   * Schedule reconnection with exponential backoff
   */
  private scheduleReconnect(): void {
    if (this.isShuttingDown || this.state.status === "waf_blocked") return;

    // Clear any existing timers
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
    }
    if (this.wafBackoffTimer) {
      clearTimeout(this.wafBackoffTimer);
      this.wafBackoffTimer = null;
    }

    const delay = Math.min(
      RECONNECT_DELAY_MS * Math.pow(2, this.state.reconnectAttempts),
      RECONNECT_DELAY_MAX_MS
    );

    this.reconnectTimer = setTimeout(() => {
      if (!this.isShuttingDown) {
        this.connect();
      }
    }, delay);

    logger.info(
      { component: "BcGameSocketClient", delay, attempts: this.state.reconnectAttempts },
      "Scheduling reconnection"
    );
  }

  /**
   * Handle incoming socket events
   */
  private handleIncomingEvent(event: string, payload: unknown): void {
    // Track discovered events for observability
    if (!this.discoveredEvents.has(event)) {
      this.discoveredEvents.add(event);
      this.logDiscoveredEvent(event, payload);
    }

    // Handle known event types
    if (this.eventHandlers.has(event as BcGameEvent)) {
      const handlers = this.eventHandlers.get(event as BcGameEvent)!;
      const eventPayload = this.normalizePayload(event, payload);
      
      if (eventPayload) {
        for (const handler of handlers) {
          void handler(eventPayload, event as BcGameEvent).catch(error => {
            this.handleError(error, `handler_${event}`);
          });
        }
      }
    }
  }

  /**
   * Normalize event payload to consistent format
   */
  private normalizePayload(event: string, payload: unknown): BcGameEventPayload | null {
    try {
      if (typeof payload !== "object" || payload === null) {
        logger.warn({ component: "BcGameSocketClient", event }, "Invalid payload type");
        return null;
      }

      const data = payload as Record<string, unknown>;
      
      // Extract gameId - this is critical for the temporal invariant
      const gameId = String(data.gameId || data.id || "");
      if (!gameId || !/^\d+$/.test(gameId)) {
        logger.warn({ component: "BcGameSocketClient", event, payload }, "Invalid or missing gameId");
        return null;
      }

      // Extract timestamps
      let beganAt: number | string | undefined;
      let crashedAt: number | string | undefined;
      let multiplier: number | undefined;

      // Handle different payload formats
      if (event === "bg") {
        // Round start event - should have beganAt
        beganAt = data.beganAt ?? data.beginTime ?? data.startTime;
      } else if (event === "ed") {
        // Round end event - should have crashedAt and multiplier
        crashedAt = data.crashedAt ?? data.endTime ?? data.crashTime;
        multiplier = typeof data.multiplier === "number" ? data.multiplier : 
                   typeof data.rate === "number" ? data.rate : 
                   typeof data.multiplier === "string" ? parseFloat(data.multiplier) : 
                   undefined;
      } else if (event === "pg") {
        // Progress event - might have current multiplier
        multiplier = typeof data.multiplier === "number" ? data.multiplier : 
                   typeof data.current === "number" ? data.current :
                   undefined;
      }

      return {
        gameId,
        multiplier,
        beganAt,
        crashedAt,
        ...data, // Include all original data for flexibility
      };
    } catch (error) {
      logger.error({ component: "BcGameSocketClient", event, error }, "Failed to normalize payload");
      return null;
    }
  }

  /**
   * Log discovered events to database for observability (spec §13.5)
   */
  private async logDiscoveredEvent(event: string, payload: unknown): Promise<void> {
    try {
      const sql = await getSql();
      await sql`
        insert into socket_event_discovery (event_name, payload, received_at)
        values (${event}, ${JSON.stringify(payload)}, now())
        on conflict (event_name) do nothing
      `;
    } catch (error) {
      logger.warn({ component: "BcGameSocketClient", error }, "Failed to log discovered event");
    }
  }

  /**
   * Handle errors and notify error handlers
   */
  private handleError(error: Error, context: string): void {
    logger.error({ component: "BcGameSocketClient", context, error }, error.message);
    
    for (const handler of this.errorHandlers) {
      void handler(error, context).catch(() => {
        // Don't let error handlers crash the socket client
      });
    }
  }

  /**
   * Update connection state and notify handlers
   */
  private updateState(updates: Partial<ConnectionState>): void {
    this.state = { ...this.state, ...updates };
    
    for (const handler of this.connectionHandlers) {
      void handler(this.state).catch(() => {
        // Don't let connection handlers crash the socket client
      });
    }
  }

  /**
   * Disconnect from BC.Game Socket.IO
   */
  disconnect(): void {
    this.isShuttingDown = true;
    
    // Clear all timers
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.wafBackoffTimer) {
      clearTimeout(this.wafBackoffTimer);
      this.wafBackoffTimer = null;
    }

    if (this.socket) {
      this.socket.disconnect();
      this.socket = null;
    }

    this.updateState({
      status: "disconnected",
      lastDisconnectedAt: new Date().toISOString(),
      socketId: null,
    });

    logger.info({ component: "BcGameSocketClient" }, "Disconnected from BC.Game Socket.IO");
  }

  /**
   * Check if currently connected
   */
  isConnected(): boolean {
    return this.state.status === "connected";
  }

  /**
   * Check if connection is active (connected or connecting)
   */
  isActive(): boolean {
    return this.state.status === "connected" || this.state.status === "connecting";
  }
}

// Singleton instance
export const bcGameSocket = new BcGameSocketClient();

// Initialize the socket client on module load
// This allows early registration of handlers before connection
export function initializeSocketClient(): BcGameSocketClient {
  return bcGameSocket;
}