export type SourceKind = "socket" | "poll";
export type RoundPhase = "prepare" | "begin" | "progress" | "end";

export type RawSourceEvent = {
  sourceId: string;
  sourceKind: SourceKind;
  event: string;
  payload: unknown;
  receivedAt: number;
  backfill?: boolean;
};

export type NormalizedRoundEvent = {
  sourceId: string;
  sourceKind: SourceKind;
  phase: RoundPhase;
  gameId: string;
  multiplier: number | null;
  hash: string | null;
  salt: string | null;
  beganAt: number | null;
  crashedAt: number | null;
  receivedAt: number;
  rawEvent: string;
  backfill: boolean;
};

export type ValidatedRoundEvent = NormalizedRoundEvent & {
  acceptedAt: number;
};

export type ConnectionStatus =
  | "stopped"
  | "connecting"
  | "connected"
  | "degraded"
  | "reconnecting"
  | "waf_blocked";

export type AdapterHealth = {
  sourceId: string;
  status: ConnectionStatus;
  transport: string | null;
  lastError: string | null;
  lastEventAt: number | null;
  lastEventKind: string | null;
  reconnectAttempts: number;
  totalReconnects: number;
  socketId: string | null;
};

export type MetricsSnapshot = {
  eventsReceived: number;
  eventsAccepted: number;
  duplicates: number;
  stale: number;
  invalid: number;
  missed: number;
  reconnects: number;
  lastArrivalLagMs: number | null;
  lastProcessingMs: number | null;
  lastPredictionMs: number | null;
  lastDeliveryMs: number | null;
  avgProcessingMs: number | null;
  avgPredictionMs: number | null;
  avgDeliveryMs: number | null;
  lastE2eMs: number | null;
};

export type InRoundState = {
  gameId: string;
  multiplier: number | null;
  phase: RoundPhase;
  updatedAt: number;
};

export interface DataSourceAdapter {
  readonly id: string;
  start(): Promise<void>;
  stop(): Promise<void>;
  getHealth(): AdapterHealth;
  onRaw(handler: (event: RawSourceEvent) => void): () => void;
  onHealth(handler: (health: AdapterHealth) => void): () => void;
}
