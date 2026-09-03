# TestingEngine Prediction Pipeline — Production-Grade Solution

**Companion to:** `DIAGNOSIS.md`
**Date:** 2026-09-03
**Subject:** True ahead-of-time next-round prediction for https://github.com/Okeson453/TestingEngine
**Goal:** Guarantee `prediction_generated_at < target_round_started_at < target_round.crashed_at` from persisted data, with non-blocking Telegram delivery, idempotency, and recovery.

---

## Table of Contents

1. [The Required Temporal Invariant](#1-the-required-temporal-invariant)
2. [Architectural Change: REST Polling → Socket.IO Streaming](#2-architectural-change-rest-polling--socketio-streaming)
3. [Database Schema Changes (Migration `0008_next_round_prediction.sql`)](#3-database-schema-changes-migration-0008_next_round_predictionsql)
4. [The Corrected Execution Sequence](#4-the-corrected-execution-sequence)
5. [Code Changes](#5-code-changes)
6. [Removing the Old Path](#6-removing-the-old-path)
7. [Startup, Shutdown, and Recovery](#7-startup-shutdown-and-recovery)
8. [Concurrency and Race-Condition Catalogue](#8-concurrency-and-race-condition-catalogue)
9. [Observability](#9-observability)
10. [Production Acceptance Criteria](#10-production-acceptance-criteria)
11. [Migration and Cutover Plan](#11-migration-and-cutover-plan)
12. [Summary of the Strict Temporal Guarantee](#12-summary-of-the-strict-temporal-guarantee)

---

## 1. The Required Temporal Invariant

For every prediction in the system, the following inequality must hold and must be provable from persisted data:

```
prediction_generated_at  <  target_round_started_at  <  target_round.crashed_at
              (DB row ts)        (BC.Game)                        (BC.Game)
```

### 1.1 Persisted columns that establish the invariant

| Column | Source | Set by |
|---|---|---|
| `pending_predictions.requested_at` | DB `DEFAULT now()` | INSERT (server-side clock) |
| `pending_predictions.target_round_started_at` | BC.Game `bg` event payload | INSERT (BC.Game's authoritative clock) |
| `pending_predictions.target_game_id` | BC.Game `bg` event payload | INSERT |
| `pending_predictions.source_round_id` | previous round's `game_id` | INSERT |
| `pending_predictions.correlation_id` | `randomUUID()` | INSERT |
| `crash_rounds.began_at` | BC.Game `bg` event | reconciler / stream consumer |
| `crash_rounds.crashed_at` | BC.Game `ed` event | reconciler / stream consumer |
| `prediction_validations.resolved_at` | BC.Game `ed` event | INSERT |

The acceptance query that proves the invariant is in §10.1.

### 1.2 What "generated" means in the new architecture

A prediction is "generated" at the moment its `pending_predictions` row is `INSERT`ed. The row's `requested_at` is the canonical timestamp. The outbox rows for Telegram are written in the **same transaction** as the prediction row, so the outbox is durable together with the prediction.

---

## 2. Architectural Change: REST Polling → Socket.IO Streaming

The current REST poll at `src/lib/crash/fetch-bc.ts:104` returns only settled rounds. This is the structural reason the engine cannot guarantee `prediction_generated_at < target_round_started_at`. The fix is a streaming consumer that subscribes to BC.Game's `/game-support` namespace.

### 2.1 New dependency

Add to `package.json`:

```json
"dependencies": {
  ...,
  "socket.io-client": "^4.7.5"
}
```

### 2.2 New module: `src/lib/crash/bc-socket.ts`

This is the **only** file in the codebase that imports `socket.io-client`. The rest of the system subscribes to its `EventEmitter` and never imports the transport.

```ts
/**
 * BC.Game Socket.IO streaming client.
 *
 * Consumes the /game-support namespace at wss://socketv4.bc.game/socket.io
 * and emits internal events:
 *   - 'round_prepare'  ({ gameId, hash, salt, receivedAt })
 *   - 'round_starting' ({ gameId, beganAt, hash, salt, prepareTime, receivedAt })
 *   - 'round_resolved' ({ gameId, crashPoint, endedAt, gameDetail, receivedAt })
 *   - 'connection_state' ('connecting' | 'open' | 'reconnecting' | 'closed')
 *
 * Reconnect is exponential-backoff (1s, 2s, 4s, ... capped at 30s). On
 * reconnect, the consumer calls back into the reconciler to replay any
 * missed rounds via the REST history endpoint, idempotently.
 *
 * The socket layer is the ONLY place in the codebase that touches
 * socket.io-client. The prediction engine subscribes to its EventEmitter
 * — it does not import the transport.
 */
import { EventEmitter } from "node:events";
import { io, Socket } from "socket.io-client";
import { getLogger } from "@/lib/observability/logger";

const BC_SOCKET_URL = process.env.BC_SOCKET_URL ?? "wss://socketv4.bc.game";
const BC_NAMESPACE = "/game-support";

export type RoundPrepare = {
  gameId: string;
  hash: string | null;
  salt: string | null;
  receivedAt: string;
};
export type RoundStarting = {
  gameId: string;
  beganAt: string;
  hash: string | null;
  salt: string | null;
  prepareTime: number | null;
  receivedAt: string;
};
export type RoundResolved = {
  gameId: string;
  crashPoint: number;
  endedAt: string;
  gameDetail: unknown;
  receivedAt: string;
};

declare interface BcSocketClient {
  on(event: "round_prepare",  cb: (e: RoundPrepare)  => void): this;
  on(event: "round_starting", cb: (e: RoundStarting) => void): this;
  on(event: "round_resolved", cb: (e: RoundResolved) => void): this;
  on(event: "connection_state", cb: (s: "connecting"|"open"|"reconnecting"|"closed") => void): this;
  emit(event: string, payload: unknown): boolean;
}

export class BcSocketClient extends EventEmitter {
  private socket: Socket | null = null;
  private logger = getLogger("bc-socket");
  private backoffMs = 1000;
  private readonly maxBackoffMs = 30_000;
  private closed = false;

  start(): void {
    this.closed = false;
    this.connect();
  }

  async stop(): Promise<void> {
    this.closed = true;
    if (this.socket) this.socket.disconnect();
  }

  private connect(): void {
    this.emit("connection_state", "connecting");
    this.socket = io(`${BC_SOCKET_URL}${BC_NAMESPACE}`, {
      transports: ["websocket"],
      reconnection: true,
      reconnectionDelay: 1000,
      reconnectionDelayMax: 30_000,
      timeout: 20_000,
      autoConnect: true,
    });
    this.socket.on("connect", () => {
      this.backoffMs = 1000;
      this.emit("connection_state", "open");
      this.logger.info({ component: "bc-socket" }, "socket open");
    });
    this.socket.on("disconnect", () => {
      this.emit("connection_state", "closed");
      this.logger.warn({ component: "bc-socket" }, "socket disconnected");
    });
    this.socket.on("reconnect_attempt", () => {
      this.emit("connection_state", "reconnecting");
    });

    // Event names per the BC.Game investigation report:
    //   'pr' -> prepare, 'bg' -> begin, 'pg' -> progress, 'ed' -> end, 'st' -> settle
    this.socket.on("pr", (msg: { gameId?: string; hash?: string; salt?: string; prepareTime?: number }) => {
      if (!msg?.gameId) return;
      this.emit("round_prepare", {
        gameId: String(msg.gameId),
        hash: msg.hash ?? null,
        salt: msg.salt ?? null,
        receivedAt: new Date().toISOString(),
      } as RoundPrepare);
    });
    this.socket.on("bg", (msg: { gameId?: string; beginTime?: number; hash?: string; salt?: string; prepareTime?: number }) => {
      if (!msg?.gameId) return;
      const beganAt = typeof msg.beginTime === "number" ? new Date(msg.beginTime).toISOString() : new Date().toISOString();
      this.emit("round_starting", {
        gameId: String(msg.gameId),
        beganAt,
        hash: msg.hash ?? null,
        salt: msg.salt ?? null,
        prepareTime: msg.prepareTime ?? null,
        receivedAt: new Date().toISOString(),
      } as RoundStarting);
    });
    this.socket.on("ed", (msg: { gameId?: string; rate?: number; endTime?: number }) => {
      if (!msg?.gameId) return;
      const endedAt = typeof msg.endTime === "number" ? new Date(msg.endTime).toISOString() : new Date().toISOString();
      this.emit("round_resolved", {
        gameId: String(msg.gameId),
        crashPoint: Number(msg.rate ?? 0),
        endedAt,
        gameDetail: msg,
        receivedAt: new Date().toISOString(),
      } as RoundResolved);
    });
  }
}
```

---

## 3. Database Schema Changes (Migration `0008_next_round_prediction.sql`)

```sql
-- 0008_next_round_prediction.sql
-- Next-round prediction contract:
--   1) Generation happens on receipt of a `round_starting` event for round N+1.
--   2) The prediction row carries target_game_id = N+1.gameId at INSERT time.
--   3) target_round_started_at = N+1.beganAt (BC.Game's authoritative start time).
--   4) Validation later confirms the actual result via the durable UNIQUE on
--      prediction_validations(game_id) (already in 0007).

ALTER TABLE pending_predictions
  ADD COLUMN IF NOT EXISTS target_round_started_at timestamptz,
  ADD COLUMN IF NOT EXISTS source_round_id text,
  ADD COLUMN IF NOT EXISTS correlation_id text,
  ADD COLUMN IF NOT EXISTS model_name text,
  ADD COLUMN IF NOT EXISTS prediction_status text NOT NULL DEFAULT 'pending'
    CHECK (prediction_status IN ('pending','sent','validated','cancelled','expired'));

-- Generated_at must be set server-side; never trust client clock.
ALTER TABLE pending_predictions
  ALTER COLUMN requested_at SET DEFAULT now();

-- Idempotency: one prediction per (target_game_id, model_name).
-- The 0007 partial index already enforces one unmatched pending per game_id;
-- extend to also include model_name so a model swap never produces a second
-- row for the same target.
CREATE UNIQUE INDEX IF NOT EXISTS pending_predictions_target_model_unmatched_uidx
  ON pending_predictions (target_game_id, model_name)
  WHERE matched = false;

-- Notification outbox (transactional with the prediction INSERT).
-- Decouples the prediction engine from the Telegram transport.
CREATE TABLE IF NOT EXISTS notification_outbox (
  id              bigserial primary key,
  prediction_id   text NOT NULL,
  correlation_id  text NOT NULL,
  chat_id         text NOT NULL,
  message_kind    text NOT NULL CHECK (message_kind IN ('prediction','validation')),
  payload         jsonb NOT NULL,
  status          text NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued','sending','sent','failed','dead_letter')),
  enqueued_at     timestamptz NOT NULL DEFAULT now(),
  sent_at         timestamptz,
  attempts        int NOT NULL DEFAULT 0,
  last_error      text,
  next_retry_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS notification_outbox_queued_idx
  ON notification_outbox (next_retry_at) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS notification_outbox_dead_letter_idx
  ON notification_outbox (status) WHERE status = 'dead_letter';
CREATE INDEX IF NOT EXISTS notification_outbox_correlation_idx
  ON notification_outbox (correlation_id);

-- Add began_at column to crash_rounds if not already present
-- (it is, per migration 0002), but ensure it is the authoritative start
-- time coming from the streaming 'bg' event, not a fallback.
COMMENT ON COLUMN crash_rounds.began_at IS
  'Authoritative BC.Game round start time. Populated by the streaming consumer on receipt of the bg event; falls back to crashed_at - 5s only if missing.';
```

---

## 4. The Corrected Execution Sequence

The strict sequence is:

```
[T_bc]     BC.Game emits 'bg' (round_starting) for gameId G
[T_recv]   bc-socket.ts emits 'round_starting' to listeners
[T_pg]     INSERT pending_predictions(target_game_id=G,
                                       target_round_started_at=T_bc,
                                       source_round_id=G-1,
                                       model_name=...,
                                       prediction_status='pending',
                                       correlation_id=UUID)
           — single transaction
[T_pn]     INSERT notification_outbox(prediction_id, correlation_id,
                                       chat_id=each, message_kind='prediction',
                                       payload=full_message_body, status='queued')
           — same transaction
[T_comm]   COMMIT  (atomic guarantee: prediction row + queued notifications
                    are persisted together or not at all)
[T_emit]   async dispatch_loop sees new 'queued' rows; POSTs to Telegram
[T_send]   on HTTP 2xx: UPDATE notification_outbox SET status='sent', sent_at=now();
           on 4xx/5xx: schedule retry with backoff; max 3 attempts then 'dead_letter'
[T_ed]     BC.Game emits 'ed' (round_resolved) for gameId G
[T_val]    validate_target_round(G) runs: lookup pending with target_game_id=G
           (must exist since T_pg < T_ed, by construction),
           check actual_multiplier >= target_multiplier, INSERT prediction_validations,
           UPDATE pending_predictions SET matched=true, prediction_status='validated'
[T_vn]     INSERT notification_outbox(validation kind, same correlation_id)
[T_emit2]  outbox dispatcher sends validation Telegram
```

### 4.1 Per-step sync/async classification

| Step | Synchronous? | Reason |
|---|---|---|
| `round_starting` → `INSERT pending_predictions + notification_outbox` | **SYNCHRONOUS, single transaction** | This is the only place the temporal contract is established. Must complete before any Telegram send. |
| `INSERT pending_predictions` itself | **SYNCHRONOUS, awaited** | The prediction row is the source of truth. Telegram send is a side-effect. |
| `INSERT notification_outbox` | **SYNCHRONOUS, same transaction** | Outbox + prediction must be atomic. |
| `dispatch_loop` polling `notification_outbox WHERE status='queued' AND next_retry_at <= now()` | **ASYNCHRONOUS, polled** | Decouples prediction latency from Telegram latency. |
| `fetch(sendMessage)` | **ASYNCHRONOUS, per-chat, 2s AbortController** | Telegram failures never block the prediction engine. |
| `validate_target_round(G)` | **SYNCHRONOUS on receipt of `round_resolved`** | The lookup is `WHERE target_game_id = G` and uses the partial UNIQUE index. The row is guaranteed to exist (or the system is in an error state to be alerted on). |
| `INSERT notification_outbox` (validation) | **SYNCHRONOUS, same transaction as the `prediction_validations` INSERT** | Same atomicity argument. |
| REST reconciliation on socket reconnect | **ASYNCHRONOUS, backoff, idempotent ON CONFLICT DO NOTHING** | Defensive; not the primary path. |

### 4.2 Transactional boundaries

- **Atomic:** `pending_predictions` INSERT + `notification_outbox` INSERT (prediction) — single transaction.
- **Atomic:** `prediction_validations` INSERT + `UPDATE pending_predictions (matched=true)` + `notification_outbox` INSERT (validation) — single transaction.
- **Idempotent on retry:** every `INSERT` uses `ON CONFLICT DO NOTHING` against a `UNIQUE` constraint or partial `UNIQUE` index.
- **No half-states:** a crash mid-transaction rolls back both the prediction and the outbox; a crash after commit leaves the outbox in `queued` (or `sending` if mid-fetch, see §7.4).

### 4.3 Idempotency controls

| Layer | Control |
|---|---|
| Prediction uniqueness | `pending_predictions_target_model_unmatched_uidx` (partial UNIQUE on `(target_game_id, model_name) WHERE matched = false`) |
| Validation uniqueness | `prediction_validations_game_id_key` (UNIQUE on `game_id`) from migration `0007` |
| Outbox uniqueness | `notification_outbox(id)` (primary key) + dispatcher uses `FOR UPDATE SKIP LOCKED` |
| Reconnect replay | `INSERT ... ON CONFLICT DO NOTHING` on `crash_rounds` and `pending_predictions` |
| Telegram | Outbox row is per-chat; re-running the dispatcher for a `sent` row is a no-op (filter on `status='queued'`) |

---

## 5. Code Changes

### 5.1 New module: `src/lib/prediction/next-round-generator.ts`

This is the **only** function that creates a `pending_predictions` row in the new architecture.

```ts
import { randomUUID } from "node:crypto";
import { getSql, type Sql } from "@/lib/db";
import { PredictionEngine } from "./prediction-engine.ts";
import type { ThresholdTarget } from "./types.ts";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("next-round-generator");

export interface NextRoundInput {
  /** BC.Game gameId of the next round (the one we're predicting). */
  targetGameId: string;
  /** BC.Game authoritative begin time of the next round. */
  targetBeganAt: string;
  /** gameId of the most recently observed, settled round (Round N). */
  sourceRoundGameId: string;
  /** Engine correlation id for end-to-end tracing. */
  correlationId: string;
  /** Optional model override; default is the registered default. */
  modelName?: string;
  modelVersion?: string;
  target?: ThresholdTarget;
}

export interface NextRoundResult {
  predictionId: string;
  targetGameId: string;
  targetBeganAt: string;
  sourceRoundGameId: string;
  correlationId: string;
  generatedAt: string;
  alreadyExisted: boolean;
}

const TARGET_DEFAULT: ThresholdTarget = 1.3;
const MAX_HISTORY = 100;

/**
 * Generate and atomically persist the prediction for the next round.
 *
 * Called synchronously on receipt of a 'round_starting' event. Writes
 * the pending_predictions row and notification_outbox rows in a SINGLE
 * transaction so the prediction and its notification queue are durable
 * together.
 *
 * Idempotency: UNIQUE(target_game_id, model_name) WHERE matched=false
 * (see migration 0008) — re-entering the same target is a no-op and
 * returns the existing prediction_id.
 */
export async function generateNextRoundPrediction(
  input: NextRoundInput,
  sql: Sql,
): Promise<NextRoundResult | null> {
  const target = input.target ?? TARGET_DEFAULT;
  const modelName = input.modelName ?? "default";
  const modelVersion = input.modelVersion ?? "v1";
  const correlationId = input.correlationId || randomUUID();

  // Fast idempotency check: if a pending row already exists for this
  // target game_id + model, return it without recomputing.
  const existing = await sql<{ prediction_id: string; generated_at: string; target_round_started_at: string }>`
    SELECT prediction_id, requested_at AS generated_at, target_round_started_at
    FROM pending_predictions
    WHERE target_game_id = ${input.targetGameId}
      AND model_name = ${modelName}
      AND matched = false
    LIMIT 1
  `;
  if (existing.length > 0) {
    return {
      predictionId: existing[0]!.prediction_id,
      targetGameId: input.targetGameId,
      targetBeganAt: existing[0]!.target_round_started_at,
      sourceRoundGameId: input.sourceRoundGameId,
      correlationId,
      generatedAt: existing[0]!.generated_at,
      alreadyExisted: true,
    };
  }

  // Load history: only rounds whose outcome is fully known BEFORE
  // targetBeganAt. This is the data-availability cutoff.
  const rows = await sql<{
    game_id: string; multiplier: string | number;
    began_at: string | Date | null; crashed_at: string | Date;
  }>`
    SELECT game_id, multiplier, began_at, crashed_at
    FROM crash_rounds
    WHERE crashed_at <= ${input.targetBeganAt}::timestamptz
    ORDER BY crashed_at DESC, game_id DESC
    LIMIT ${MAX_HISTORY}
  `;
  if (rows.length < 20) {
    logger.warn({ component: "next-round-generator", correlationId, targetGameId: input.targetGameId, n: rows.length },
      "insufficient history; skipping prediction");
    return null;
  }

  const priorRounds = rows.reverse().map((r) => ({
    id: String(r.game_id),
    externalRoundId: String(r.game_id),
    sessionId: null,
    startedAt: r.began_at instanceof Date ? r.began_at.toISOString() : (r.began_at ?? null),
    crashedAt: r.crashed_at instanceof Date ? r.crashed_at.toISOString() : String(r.crashed_at),
    crashPoint: Number(r.multiplier),
    observationSource: "bc-game-api",
    dataQuality: "high" as const,
    createdAt: r.crashed_at instanceof Date ? r.crashed_at.toISOString() : String(r.crashed_at),
  }));

  const engine = new PredictionEngine();
  const requestedAt = new Date().toISOString();
  const signal = engine.predict({
    priorRounds,
    targetRoundId: input.targetGameId,  // NOW the real id, not "next"
    timestamp: requestedAt,
    target,
  });

  // Atomic: prediction row + outbox rows for every configured chat id
  // commit together.
  const predictionId = signal.predictionId;
  const { getConfiguredChatIds } = await import("@/lib/notifications/telegram");
  const chatIds = getConfiguredChatIds();

  try {
    await sql.transaction(async (tx) => {
      const insertResult = await tx`
        INSERT INTO pending_predictions (
          prediction_id, target_multiplier, probability, confidence,
          regime_name, regime_confidence, reasoning, feature_summary,
          model_version, requested_at,
          target_game_id, target_round_started_at, source_round_id,
          correlation_id, model_name, prediction_status
        ) VALUES (
          ${predictionId}, ${target}, ${signal.probability}, ${signal.confidence},
          ${signal.regimeId}, ${signal.regimeId ? 0.5 : null},
          ${signal.reasoning}, ${JSON.stringify(signal.featureSummary)},
          ${modelVersion}, ${requestedAt},
          ${input.targetGameId}, ${input.targetBeganAt}, ${input.sourceRoundGameId},
          ${correlationId}, ${modelName}, 'pending'
        )
        ON CONFLICT ON CONSTRAINT pending_predictions_target_model_unmatched_uidx DO NOTHING
        RETURNING prediction_id
      `;
      if (insertResult.length === 0) {
        // Lost a race; another process already wrote the row.
        throw new Error("PREDICTION_ALREADY_EXISTS");
      }
      for (const chatId of chatIds) {
        await tx`
          INSERT INTO notification_outbox
            (prediction_id, correlation_id, chat_id, message_kind, payload, status)
          VALUES (
            ${predictionId}, ${correlationId}, ${chatId}, 'prediction',
            ${JSON.stringify({
              predictionId,
              targetGameId: input.targetGameId,
              targetBeganAt: input.targetBeganAt,
              sourceRoundGameId: input.sourceRoundGameId,
              targetMultiplier: Number(target),
              probability: signal.probability,
              confidence: signal.confidence,
              regimeName: signal.regimeId,
              requestedAt,
            })},
            'queued'
          )
        `;
      }
    });
  } catch (e: unknown) {
    if (e instanceof Error && e.message === "PREDICTION_ALREADY_EXISTS") {
      const ex = await sql<{ prediction_id: string; requested_at: string; target_round_started_at: string }>`
        SELECT prediction_id, requested_at, target_round_started_at
        FROM pending_predictions
        WHERE target_game_id = ${input.targetGameId}
          AND model_name = ${modelName}
          AND matched = false
        LIMIT 1
      `;
      if (ex.length === 0) throw new Error("prediction_deduped_but_not_readable");
      return {
        predictionId: ex[0]!.prediction_id,
        targetGameId: input.targetGameId,
        targetBeganAt: ex[0]!.target_round_started_at,
        sourceRoundGameId: input.sourceRoundGameId,
        correlationId,
        generatedAt: ex[0]!.requested_at,
        alreadyExisted: true,
      };
    }
    throw e;
  }

  return {
    predictionId,
    targetGameId: input.targetGameId,
    targetBeganAt: input.targetBeganAt,
    sourceRoundGameId: input.sourceRoundGameId,
    correlationId,
    generatedAt: requestedAt,
    alreadyExisted: false,
  };
}
```

### 5.2 New module: `src/lib/prediction/validator.ts`

Replaces `validateAgainstNewRounds`. Lookup is now by `target_game_id` (which is set at generation time, not at validation time).

```ts
import { randomUUID } from "node:crypto";
import { getSql, type Sql } from "@/lib/db";
import type { RoundResolved } from "@/lib/crash/bc-socket";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("next-round-validator");

export interface ValidationResult {
  predictionId: string;
  gameId: string;
  result: "WIN" | "LOSS";
  targetMultiplier: number;
  actualMultiplier: number;
  probability: number;
  confidence: number;
  resolvedAt: string;
  correlationId: string;
  alreadyValidated: boolean;
}

/**
 * Validate the pending prediction for the given target_game_id.
 *
 * Called synchronously on receipt of a 'round_resolved' event.
 * The pending row is guaranteed to exist (by the partial UNIQUE index
 * and the fact that 'round_starting' preceded 'round_resolved' on the
 * stream), but we still defend with NOT_FOUND so an out-of-order
 * delivery is observable.
 */
export async function validateTargetRound(
  resolved: RoundResolved,
  sql: Sql,
): Promise<ValidationResult | null> {
  const pending = await sql<{
    prediction_id: string; target_multiplier: string | number;
    probability: string | number; confidence: string | number;
    correlation_id: string; requested_at: string;
  }>`
    SELECT prediction_id, target_multiplier, probability, confidence,
           correlation_id, requested_at
    FROM pending_predictions
    WHERE target_game_id = ${resolved.gameId} AND matched = false
    LIMIT 1
    FOR UPDATE SKIP LOCKED
  `;
  if (pending.length === 0) {
    logger.warn({ component: "next-round-validator", gameId: resolved.gameId },
      "no pending prediction for resolved gameId — recording orphan round");
    return null;
  }
  const p = pending[0]!;
  const target = Number(p.target_multiplier);
  const result = resolved.crashPoint >= target ? "WIN" : "LOSS";
  const now = new Date().toISOString();

  const { getConfiguredChatIds } = await import("@/lib/notifications/telegram");
  const chatIds = getConfiguredChatIds();

  let alreadyValidated = false;
  await sql.transaction(async (tx) => {
    const inserted = await tx<{ prediction_id: string }>`
      INSERT INTO prediction_validations (
        prediction_id, game_id, target_multiplier, predicted_probability,
        predicted_confidence, actual_multiplier, result, model_version,
        requested_at, resolved_at
      ) VALUES (
        ${p.prediction_id}, ${resolved.gameId}, ${target},
        ${Number(p.probability)}, ${Number(p.confidence)},
        ${resolved.crashPoint}, ${result}, 'v1',
        ${p.requested_at}, ${resolved.endedAt}
      )
      ON CONFLICT ON CONSTRAINT prediction_validations_game_id_key DO NOTHING
      RETURNING prediction_id
    `;
    alreadyValidated = inserted.length === 0;
    if (alreadyValidated) return;  // existing row, do not write outbox again
    await tx`
      UPDATE pending_predictions
      SET matched = true, matched_game_id = ${resolved.gameId},
          matched_at = ${resolved.endedAt}, prediction_status = 'validated'
      WHERE prediction_id = ${p.prediction_id}
    `;
    for (const chatId of chatIds) {
      await tx`
        INSERT INTO notification_outbox
          (prediction_id, correlation_id, chat_id, message_kind, payload, status)
        VALUES (
          ${p.prediction_id}, ${p.correlation_id}, ${chatId}, 'validation',
          ${JSON.stringify({
            predictionId: p.prediction_id,
            gameId: resolved.gameId,
            targetMultiplier: target,
            actualMultiplier: resolved.crashPoint,
            probability: Number(p.probability),
            result,
            resolvedAt: resolved.endedAt,
          })},
          'queued'
        )
      `;
    }
  });

  return {
    predictionId: p.prediction_id,
    gameId: resolved.gameId,
    result,
    targetMultiplier: target,
    actualMultiplier: resolved.crashPoint,
    probability: Number(p.probability),
    confidence: Number(p.confidence),
    resolvedAt: resolved.endedAt,
    correlationId: p.correlation_id,
    alreadyValidated,
  };
}
```

### 5.3 New module: `src/lib/notifications/outbox-dispatcher.ts`

Replaces `fireTelegram`. The dispatcher is the **only** component that touches `api.telegram.org`. It polls the outbox, sends, and updates status atomically.

```ts
import { getSql } from "@/lib/db";
import {
  sendTelegramMessage,
  formatPredictionMessage,
  formatValidationMessage,
} from "./telegram";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("outbox-dispatcher");

const MAX_ATTEMPTS = 3;
const BASE_BACKOFF_MS = 1_000;
const POLL_INTERVAL_MS = 200;  // fast inner loop; 1 row per pass
const BATCH_SIZE = 16;
const STALE_SENDING_RECOVERY_MS = 30_000;

type OutboxRow = {
  id: string;
  prediction_id: string;
  correlation_id: string;
  chat_id: string;
  message_kind: "prediction" | "validation";
  payload: any;
  attempts: number;
};

/**
 * Outbox dispatcher loop. Run as a long-lived worker (one per Node process,
 * locked by worker_locks). Drains notification_outbox in small batches,
 * respecting per-row attempt count and exponential backoff.
 *
 * Failures: per-chat AbortController (2s) inside sendTelegramMessage.
 * On HTTP 2xx -> 'sent'. On 4xx (other than 429) -> 'dead_letter' immediately
 * (chat not found, bot blocked, etc. won't fix themselves). On 429/5xx/network
 * -> 'queued' with next_retry_at = now() + 2^attempts seconds, up to 3 attempts.
 *
 * Stale 'sending' rows are requeued every tick (recovered from a crashed worker).
 */
export class OutboxDispatcher {
  private timer: NodeJS.Timeout | null = null;
  private running = false;

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    this.tick();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
  }

  private async tick(): Promise<void> {
    if (!this.running) return;
    try {
      await this.recoverStaleSending();
      await this.drainOnce();
    } catch (e) {
      logger.error({ component: "outbox-dispatcher", error: String(e) }, "drain error");
    }
    if (this.running) {
      this.timer = setTimeout(() => this.tick(), POLL_INTERVAL_MS);
      this.timer.unref?.();
    }
  }

  private async recoverStaleSending(): Promise<void> {
    const sql = await getSql();
    await sql`
      UPDATE notification_outbox
      SET status = 'queued',
          last_error = COALESCE(last_error, '') || ' [recovered from sending]'
      WHERE status = 'sending'
        AND enqueued_at < now() - (${STALE_SENDING_RECOVERY_MS}::int * interval '1 millisecond')
    `;
  }

  private async drainOnce(): Promise<void> {
    const sql = await getSql();
    const rows = await sql.transaction(async (tx) => {
      const claimed = await tx<OutboxRow>`
        SELECT id::text, prediction_id, correlation_id, chat_id, message_kind, payload, attempts
        FROM notification_outbox
        WHERE status = 'queued' AND next_retry_at <= now()
        ORDER BY next_retry_at ASC, id ASC
        LIMIT ${BATCH_SIZE}
        FOR UPDATE SKIP LOCKED
      `;
      if (claimed.length === 0) return [];
      for (const r of claimed) {
        await tx`UPDATE notification_outbox SET status = 'sending' WHERE id = ${r.id}::bigint`;
      }
      return claimed;
    });
    if (rows.length === 0) return;

    for (const row of rows) {
      await this.sendOne(row);
    }
  }

  private async sendOne(row: OutboxRow): Promise<void> {
    const text = row.message_kind === "prediction"
      ? formatPredictionMessage({
          predictionId: row.payload.predictionId,
          targetMultiplier: row.payload.targetMultiplier,
          probability: row.payload.probability,
          confidence: row.payload.confidence,
          regimeName: row.payload.regimeName,
          lastRoundMultiplier: row.payload.lastRoundMultiplier ?? null,
          generatedAt: row.payload.requestedAt,
        })
      : formatValidationMessage({
          predictionId: row.payload.predictionId,
          gameId: row.payload.gameId,
          targetMultiplier: row.payload.targetMultiplier,
          actualMultiplier: row.payload.actualMultiplier,
          probability: row.payload.probability,
          result: row.payload.result,
          resolvedAt: row.payload.resolvedAt,
        });

    const t0 = Date.now();
    const results = await sendTelegramMessage(text);
    const r = results[0];  // single chat per row
    const sql = await getSql();
    if (!r) return;
    const latencyMs = Date.now() - t0;
    if (r.ok) {
      await sql`
        UPDATE notification_outbox
        SET status = 'sent', sent_at = now(), last_error = NULL
        WHERE id = ${row.id}::bigint
      `;
      logger.info({
        component: "outbox-dispatcher",
        correlationId: row.correlation_id,
        predictionId: row.prediction_id,
        messageKind: row.message_kind,
        chatId: row.chat_id,
        latencyMs,
      }, "telegram sent");
    } else {
      const attempts = row.attempts + 1;
      const isPermanent = r.status >= 400 && r.status < 500 && r.status !== 429;
      const nextStatus = isPermanent || attempts >= MAX_ATTEMPTS ? "dead_letter" : "queued";
      const backoff = isPermanent ? 0 : BASE_BACKOFF_MS * Math.pow(2, attempts - 1);
      await sql`
        UPDATE notification_outbox
        SET status = ${nextStatus}, attempts = ${attempts},
            last_error = ${r.error ?? `http_${r.status}`},
            next_retry_at = now() + (${backoff}::int * interval '1 millisecond')
        WHERE id = ${row.id}::bigint
      `;
      logger.warn({
        component: "outbox-dispatcher",
        correlationId: row.correlation_id,
        predictionId: row.prediction_id,
        messageKind: row.message_kind,
        chatId: row.chat_id,
        attempts,
        nextStatus,
        error: r.error,
        latencyMs,
      }, "telegram send failed");
    }
  }
}
```

### 5.4 Rewrite `src/lib/prediction/worker.ts` to be a thin orchestrator

```ts
import { randomUUID } from "node:crypto";
import { getSql } from "@/lib/db";
import {
  BcSocketClient,
  type RoundStarting,
  type RoundResolved,
  type RoundPrepare,
} from "@/lib/crash/bc-socket";
import { fetchCrashHistory, type FetchedRound } from "@/lib/crash/fetch-bc";
import { insertNewRounds } from "@/lib/crash/ingest";
import { generateNextRoundPrediction } from "./next-round-generator";
import { validateTargetRound } from "./validator";
import { OutboxDispatcher } from "@/lib/notifications/outbox-dispatcher";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("prediction-worker");

const RECONCILE_INTERVAL_MS = 60_000;

/**
 * Top-level orchestrator. Replaces the REST-poll + idle-cycle architecture
 * with a streaming-driven one.
 *
 *   BcSocket emits 'round_starting' (for round N+1)
 *     -> generateNextRoundPrediction(): atomic INSERT pending_predictions +
 *        INSERT notification_outbox (prediction)
 *     -> return; OutboxDispatcher drains the outbox asynchronously
 *
 *   BcSocket emits 'round_resolved' (for round N+1)
 *     -> validateTargetRound(): atomic INSERT prediction_validations +
 *        INSERT notification_outbox (validation) + UPDATE pending_predictions
 *     -> return; OutboxDispatcher drains the outbox asynchronously
 *
 *   Rest reconcile runs on a 60s timer; it NEVER generates predictions,
 *   only backfills missing crash_rounds.
 */
export class PredictionWorker {
  private readonly socket = new BcSocketClient();
  private readonly dispatcher = new OutboxDispatcher();
  private latestResolved: { gameId: string; crashedAt: string } | null = null;
  private reconciler: NodeJS.Timeout | null = null;
  private getSqlFn: typeof getSql = getSql;

  async start(): Promise<void> {
    this.socket.on("round_starting", (e) => this.onRoundStarting(e));
    this.socket.on("round_resolved", (e) => this.onRoundResolved(e));
    this.socket.on("round_prepare",  (e) => this.onRoundPrepare(e));
    this.socket.start();
    await this.dispatcher.start();
    this.reconciler = setInterval(
      () => this.reconcile().catch((e) => logger.error({ error: String(e) }, "reconcile")),
      RECONCILE_INTERVAL_MS,
    );
    this.reconciler.unref?.();
  }

  async stop(): Promise<void> {
    if (this.reconciler) clearTimeout(this.reconciler);
    await this.socket.stop();
    await this.dispatcher.stop();
  }

  private async onRoundStarting(e: RoundStarting): Promise<void> {
    const correlationId = randomUUID();
    const sql = await this.getSqlFn();
    const t0 = Date.now();
    try {
      const result = await generateNextRoundPrediction({
        targetGameId: e.gameId,
        targetBeganAt: e.beganAt,
        sourceRoundGameId: this.latestResolved?.gameId ?? "",
        correlationId,
      }, sql);
      if (result) {
        await sql`INSERT INTO worker_state (key, value) VALUES (${"last_prediction_generated_at"}, ${result.generatedAt})
                  ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()`;
        await sql`INSERT INTO worker_state (key, value) VALUES (${"last_prediction_correlation_id"}, ${correlationId})
                  ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()`;
        logger.info({
          component: "prediction-worker",
          correlationId,
          predictionId: result.predictionId,
          targetGameId: e.gameId,
          targetBeganAt: e.beganAt,
          sourceRoundGameId: result.sourceRoundGameId,
          alreadyExisted: result.alreadyExisted,
          latencyMs: Date.now() - t0,
        }, "prediction generated and persisted for next round");
      }
    } catch (err) {
      logger.error({
        component: "prediction-worker",
        correlationId,
        targetGameId: e.gameId,
        error: String(err),
      }, "onRoundStarting failed");
    }
  }

  private async onRoundResolved(e: RoundResolved): Promise<void> {
    const sql = await this.getSqlFn();
    this.latestResolved = { gameId: e.gameId, crashedAt: e.endedAt };
    try {
      const result = await validateTargetRound(e, sql);
      if (result) {
        logger.info({
          component: "prediction-worker",
          correlationId: result.correlationId,
          predictionId: result.predictionId,
          gameId: e.gameId,
          result: result.result,
          alreadyValidated: result.alreadyValidated,
        }, "target round validated");
      } else {
        logger.warn({
          component: "prediction-worker",
          gameId: e.gameId,
        }, "no pending prediction for resolved round — orphan?");
      }
    } catch (err) {
      logger.error({
        component: "prediction-worker",
        error: String(err),
        gameId: e.gameId,
      }, "onRoundResolved failed");
    }
  }

  private onRoundPrepare(e: RoundPrepare): void {
    // No-op: prepare is informational. Real work happens on round_starting.
    logger.debug({ component: "prediction-worker", gameId: e.gameId }, "round prepare observed");
  }

  /**
   * Defensive reconciliation. Runs every RECONCILE_INTERVAL_MS. Fetches
   * recent settled rounds via REST and writes any not already present.
   * Does NOT generate predictions — that path is exclusively streaming-driven.
   * On socket reconnect, this is what fills in any missed rounds so the
   * history table is current.
   */
  private async reconcile(): Promise<void> {
    const sql = await this.getSqlFn();
    let rounds: FetchedRound[] = [];
    try { rounds = await fetchCrashHistory(2); } catch { return; }
    if (rounds.length === 0) return;
    const ins = await insertNewRounds(rounds);
    if (ins.inserted > 0) {
      logger.info({
        component: "prediction-worker",
        inserted: ins.inserted,
      }, "reconciler filled in missed rounds");
      for (const r of ins.rounds) {
        const pending = await sql<{ prediction_id: string }>`
          SELECT prediction_id FROM pending_predictions
          WHERE target_game_id = ${r.gameId} LIMIT 1
        `;
        if (pending.length === 0) {
          logger.warn({
            component: "prediction-worker",
            gameId: r.gameId,
            missedRound: true,
          }, "reconciled round has no prediction — socket was likely offline during bg event");
        }
      }
    }
  }
}
```

---

## 6. Removing the Old Path

The following must be **removed** to prevent regressions:

| Remove | File / Line | Reason |
|---|---|---|
| `if (today.remaining > 0 && !pending.hasPending && insertedRounds.length === 0) { ... }` | `src/lib/prediction/worker.ts:364` | The idle-cycle generation guard. Replaced by the streaming path. |
| `fireTelegram` function | `src/lib/prediction/worker.ts:204` | Replaced by the outbox dispatcher (transactional outbox). |
| `recordTelegramResult` writes to `worker_state` for Telegram | `src/lib/prediction/worker.ts:240` | Replaced by `notification_outbox` status reads. |
| `generateAndQueuePrediction` | `src/lib/prediction/service.ts:113` | Replaced by `generateNextRoundPrediction` in `next-round-generator.ts`. |
| `validateAgainstNewRounds` | `src/lib/prediction/service.ts:184` | Replaced by `validateTargetRound` in `validator.ts`. |
| `getTodayStats`, `getPendingStatus` (used in the cycle guard) | `src/lib/prediction/service.ts` | The cycle no longer needs daily-remaining or pending status checks (the trigger is event-driven). Keep `getTodayStats` only if the dashboard needs it. |
| The `runCycleWork` function | `src/lib/prediction/worker.ts:280` | Replaced by `PredictionWorker` class methods. |
| The `PENDING_POLL_INTERVAL_MS` adaptive logic | `src/lib/prediction/worker.ts` | No longer needed; the trigger is the streaming event. |

### 6.1 What is **kept**

- `formatPredictionMessage` and `formatValidationMessage` in `telegram.ts` — used by the outbox dispatcher.
- `sendTelegramMessage` in `telegram.ts` — used by the outbox dispatcher.
- `getConfiguredChatIds` in `telegram.ts` — used by the generator and validator to fan out outbox rows.
- `telegramConfigured` in `telegram.ts` — used to short-circuit outbox writes when no chat ids are configured.
- The `worker_locks` table and `acquireLock` / `heartbeat` / `releaseLock` functions — used to ensure only one dispatcher runs at a time.
- The `validation_config` table and `getDailyTarget` / `setDailyTarget` — used by the dashboard.

### 6.2 What the `WorkerStatus` dashboard now reads

The `getWorkerStatus` function in `worker.ts` should be rewritten to read from `notification_outbox` for Telegram fields:

```ts
telegramEnabled: telegramConfigured(),
telegramLastSentAt: <max(sent_at) from notification_outbox>,
telegramLastError: <max(last_error) from notification_outbox WHERE status IN ('failed','dead_letter')>,
```

---

## 7. Startup, Shutdown, and Recovery

### 7.1 Boot sequence

1. Migrate (`npm run db:migrate`) — applies `0008_next_round_prediction.sql`.
2. `PredictionWorker.start()` is called from `scripts/worker.mjs` (production) and `vite.config.ts:predictionWorkerPlugin` (dev).
3. `BcSocketClient.start()` — opens the WebSocket; on `connect` it emits `connection_state='open'`.
4. `OutboxDispatcher.start()` — begins polling the outbox at 200 ms intervals.
5. `reconciler` is scheduled for `RECONCILE_INTERVAL_MS` (60 s) to fill any gaps the socket may have missed.

### 7.2 On socket disconnect

- `socket.io-client` auto-reconnects with exponential backoff (1 s → 30 s cap).
- The `reconciler` continues to run, so missed rounds get backfilled via REST and inserted with `ON CONFLICT DO NOTHING`.
- A backfilled round whose `bg` event was missed will have no pending prediction; the `reconciler` logs a `missedRound` warning. The `validateTargetRound` call for a backfilled round that *did* have a `bg` event will find the pending row (the partial UNIQUE index guarantees it).
- The `generateNextRoundPrediction` path is purely event-driven: when the next `round_starting` event arrives after reconnect, a new prediction is generated. The `reconciler` does **not** generate predictions, so there is no double-generation.

### 7.3 On worker process crash

- Outbox rows remain in `notification_outbox` with `status='queued'` (or `'sending'` if the crash was mid-fetch — these are recovered, see §7.4).
- Pending predictions remain in `pending_predictions` with `matched=false` and `target_game_id` set.
- On restart, the `OutboxDispatcher` resumes draining the outbox; the `validator` resumes pairing on the next `round_resolved` event.

### 7.4 Mid-send crash recovery

A row left in `status='sending'` indefinitely indicates a crashed worker. The dispatcher runs a recovery pass on every tick:

```ts
// In OutboxDispatcher.tick(), before the SELECT FOR UPDATE:
await sql`
  UPDATE notification_outbox
  SET status = 'queued',
      last_error = COALESCE(last_error, '') || ' [recovered from sending]'
  WHERE status = 'sending'
    AND enqueued_at < now() - interval '30 seconds'
`;
```

This sets a 30-second ceiling on lost sends; the row goes back to `queued` and is retried.

### 7.5 Graceful shutdown

The `PredictionWorker.stop()` method is called on `SIGINT` and `SIGTERM` (existing behavior in `scripts/worker.mjs`):

1. Clear the reconciler timer.
2. Stop the socket (calls `socket.disconnect()`).
3. Stop the dispatcher (waits for the current tick to finish, then clears the timer).

The dispatcher's in-flight HTTP fetch is bounded by the per-chat `AbortController` (5 s default) so shutdown never takes more than 5 s.

---

## 8. Concurrency and Race-Condition Catalogue

| Race | Mitigation |
|---|---|
| Two `round_starting` events for the same `gameId` (reconnect replay) | `pending_predictions_target_model_unmatched_uidx` partial UNIQUE index. Second INSERT is a no-op; `alreadyExisted=true` is returned. |
| `round_resolved` arrives before `round_starting` (out-of-order) | `validateTargetRound` logs `orphan` and returns null. The reconciler will eventually insert the round; no double-validation because `prediction_validations_game_id_key` is UNIQUE. |
| Concurrent `generateNextRoundPrediction` for the same target | `FOR UPDATE SKIP LOCKED` in the lookup; partial UNIQUE on the INSERT. |
| Two outbox dispatcher instances (e.g. dev + worker both running) | Lock via `worker_locks` (existing pattern); `FOR UPDATE SKIP LOCKED` on the outbox SELECT. |
| Telegram 429 | `next_retry_at = now() + 2^attempts seconds`; status stays `queued`. |
| Telegram 4xx (chat not found, bot blocked) | `status = 'dead_letter'` immediately; no retries. |
| Telegram network timeout (5 s) | Same as 429: queued with backoff. |
| DB transaction conflict | `INSERT ... ON CONFLICT DO NOTHING` + the partial UNIQUE index makes every INSERT idempotent. |
| Worker crash mid-INSERT | Transaction is rolled back; `pending_predictions` has no row, `notification_outbox` has no row. Idempotency on next attempt is automatic. |
| Two socket connections (e.g. dev server + Railway) | `worker_locks` (existing `prediction_worker` key) ensures only one cycles at a time. The new `next-round-generator` should be wrapped in the same lock acquisition pattern. |
| Replay of the same `round_resolved` after a crash | The `INSERT INTO prediction_validations ... ON CONFLICT ON CONSTRAINT prediction_validations_game_id_key DO NOTHING` is idempotent. The first call wins; subsequent calls detect `alreadyValidated` and skip the outbox write. |

---

## 9. Observability

### 9.1 Correlation ID propagation

Every event carries a `correlationId` (UUIDv4) and is logged in JSON with the following fields:

```ts
{
  component: "prediction-worker" | "next-round-generator" | "next-round-validator" | "outbox-dispatcher" | "bc-socket",
  correlationId: string,
  predictionId?: string,
  targetGameId?: string,
  sourceRoundGameId?: string,
  latencyMs?: number,
  // ... stage-specific
}
```

The same `correlationId` is stored in `pending_predictions.correlation_id`, `notification_outbox.correlation_id`, and `prediction_validations` (via the join). A full trace can be reconstructed with:

```sql
SELECT pp.correlation_id, pp.prediction_id, pp.target_game_id,
       pp.requested_at, pp.target_round_started_at, pp.matched_at,
       no.id AS outbox_id, no.status AS outbox_status, no.attempts,
       no.sent_at, pv.game_id, pv.resolved_at, pv.result
FROM pending_predictions pp
LEFT JOIN notification_outbox no USING (prediction_id)
LEFT JOIN prediction_validations pv USING (prediction_id)
WHERE pp.correlation_id = '<uuid>';
```

### 9.2 Latency metric points (each independently measurable)

| Metric | Where it is recorded | What it measures |
|---|---|---|
| `bc_recv_lag_ms` | `bc-socket.ts` `receivedAt - msg.beginTime` | Time between BC.Game emitting the event and our process receiving it. |
| `prediction_generation_latency_ms` | `PredictionWorker.onRoundStarting` (Date.now() - receivedAt) | Time from receiving the event to committing the prediction + outbox. |
| `db_commit_latency_ms` | duration of the `sql.transaction` call | Time spent in the DB transaction. |
| `outbox_enqueue_latency_ms` | same transaction; near-zero | Time to write the outbox row. |
| `outbox_drain_latency_ms` | `OutboxDispatcher.sendOne` (sent_at - enqueued_at) | Time from outbox enqueue to Telegram send. |
| `telegram_send_latency_ms` | `OutboxDispatcher.sendOne` (`Date.now() - t0`) | HTTP RTT to Telegram. |
| `total_pipeline_latency_ms` | `sent_at (prediction) - target_round_started_at` | End-to-end. **Must be < 0** for the temporal invariant. |

### 9.3 Latency targets

| Metric | p50 | p99 | Alert threshold |
|---|---|---|---|
| `prediction_generation_latency_ms` | < 50 ms | < 200 ms | > 500 ms |
| `outbox_drain_latency_ms` | < 1 s | < 3 s | > 10 s |
| `socket_recv_lag_ms` | < 300 ms | < 1 s | > 2 s |
| `total_pipeline_latency_ms` | < 2 s | < 5 s | > 8 s |

### 9.4 Invariant-violation alert (always-on)

Run this query every minute; alert if any rows:

```sql
SELECT count(*) FROM pending_predictions
WHERE prediction_status IN ('validated')
  AND requested_at > target_round_started_at
  AND matched_at > target_round_started_at
  AND requested_at < target_round_started_at + interval '5 seconds';
```

Should always be 0. If non-zero, the invariant is violated.

---

## 10. Production Acceptance Criteria

### 10.1 Temporal invariant (hard requirement)

For every row in `prediction_validations` produced in the last 24h:

```
prediction_generated_at  <  target_round_started_at  <  target_round.crashed_at
```

**SQL proof query** (must return 0 rows in production):

```sql
SELECT pv.prediction_id, pp.requested_at, pp.target_round_started_at, cr.crashed_at
FROM prediction_validations pv
JOIN pending_predictions pp USING (prediction_id)
JOIN crash_rounds cr ON cr.game_id = pv.game_id
WHERE pv.resolved_at > now() - interval '24 hours'
  AND NOT (pp.requested_at < pp.target_round_started_at
           AND pp.target_round_started_at < cr.crashed_at);
```

**Margin query** (must return positive values):

```sql
SELECT
  percentile_cont(0.5) within group (order by extract(epoch from (target_round_started_at - requested_at))) AS p50_seconds_ahead,
  percentile_cont(0.99) within group (order by extract(epoch from (target_round_started_at - requested_at))) AS p99_seconds_ahead
FROM pending_predictions
WHERE prediction_status = 'validated'
  AND requested_at > now() - interval '24 hours';
```

Target: p50 > 1 s, p99 > 0 s.

### 10.2 Correctness invariants

1. **One prediction per target round per model**: enforced by `pending_predictions_target_model_unmatched_uidx`. Verify with: `SELECT target_game_id, model_name, count(*) FROM pending_predictions WHERE matched=false GROUP BY 1,2 HAVING count(*) > 1;` — must return 0 rows.
2. **No prediction is generated without a target game_id**: `SELECT count(*) FROM pending_predictions WHERE target_game_id IS NULL;` — must be 0.
3. **No prediction is generated without `target_round_started_at`**: `SELECT count(*) FROM pending_predictions WHERE target_round_started_at IS NULL;` — must be 0.
4. **Every validated round had a pending prediction**: `SELECT pv.game_id FROM prediction_validations pv LEFT JOIN pending_predictions pp ON pp.prediction_id = pv.prediction_id WHERE pp.prediction_id IS NULL;` — must be 0.
5. **No orphan predictions older than 5 minutes**: `SELECT prediction_id, requested_at FROM pending_predictions WHERE matched=false AND requested_at < now() - interval '5 minutes';` — should be 0 in steady state (legitimate only if the round is genuinely still in flight).
6. **Telegram send rate matches outbox drain rate within 1%**: `SELECT count(*) FROM notification_outbox WHERE status='sent' AND sent_at > now() - interval '5 minutes';` should equal `SELECT count(*) FROM notification_outbox WHERE enqueued_at > now() - interval '5 minutes';` within 1%.
7. **No stuck `sending` rows**: `SELECT count(*) FROM notification_outbox WHERE status = 'sending' AND enqueued_at < now() - interval '1 minute';` — must be 0.

### 10.3 Reliability targets

- Outbox `dead_letter` count: < 0.1% of total sends (chat config errors).
- Worker recovery on crash: < 60 s (reconciler backfill + outbox drain resume).
- Socket reconnect attempts: < 5 in any 10-minute window under stable network.
- Idempotency under retry: re-delivering the same `round_starting` event produces no duplicate prediction (verified by partial UNIQUE index + `alreadyExisted` return).
- Outbox `sending → sent` transition: < 1 s p99 under normal load.

### 10.4 Failure-mode behavior

| Failure | Expected behavior | Verified by |
|---|---|---|
| `DATABASE_URL` unset | Worker logs error and exits non-zero (no silent no-op) | unit test |
| `TELEGRAM_BOT_TOKEN` unset | Outbox rows accumulate in `queued`; warning emitted; worker continues | integration test |
| BC.Game socket disconnects for 30 s | Reconciler fills missed rounds via REST; new rounds after reconnect drive prediction generation as normal; outbox drain unaffected | chaos test |
| Telegram API 5xx | Outbox row stays `queued`, `next_retry_at` set to exponential backoff, up to 3 attempts | unit test with stubbed fetch |
| Telegram API 4xx (chat not found) | Outbox row → `dead_letter` immediately; other chats unaffected | unit test |
| DB transaction failure on INSERT | Transaction rolled back; `pending_predictions` and `notification_outbox` both remain unchanged; alert logged | unit test with mocked failure |
| Worker process killed mid-send | `notification_outbox` row left in `sending` for > 30 s; recovery query requeues it | chaos test |
| `round_starting` received twice for the same gameId | `alreadyExisted=true` returned; no duplicate prediction row, no duplicate outbox row | unit test with stubbed socket |
| `round_resolved` received twice for the same gameId | `alreadyValidated=true` returned; no duplicate `prediction_validations` row, no duplicate outbox row | unit test with stubbed socket |

### 10.5 Observability criteria

- Every log line emitted by the prediction engine includes `correlationId`.
- Every `notification_outbox` row includes `correlationId` and a payload that contains the full set of message parameters.
- Every `pending_predictions` row includes `correlation_id`, `target_round_started_at`, `source_round_id`, `model_name`, and `prediction_status`.
- The dashboard's `WorkerStatus` exposes `last_prediction_generated_at`, `last_prediction_correlation_id`, and the counts of `notification_outbox` rows by status.

---

## 11. Migration and Cutover Plan

The change is non-trivial because it changes the primary trigger for prediction generation. Recommended order:

1. **Deploy migration `0008_next_round_prediction.sql`** (additive, no destructive changes).
2. **Deploy the new modules** (`bc-socket.ts`, `next-round-generator.ts`, `validator.ts`, `outbox-dispatcher.ts`) behind a feature flag `BC_PREDICTION_V2` (env var).
3. **Run both paths in parallel** for 24 hours:
   - Old path: REST polling → `generateAndQueuePrediction` → `validateAgainstNewRounds` (unchanged).
   - New path: socket events → `generateNextRoundPrediction` → `validateTargetRound` (writes with `model_name='v2'`).
4. **Compare** via the acceptance SQL queries: count of `pending_predictions` rows from each model_name; count of `prediction_validations`; latency p50/p99; outbox dead-letter rate.
5. **Flip the default**: set `BC_PREDICTION_V2=true` and disable the old path's `generateAndQueuePrediction` call in `worker.ts`.
6. **Remove the old code** in a follow-up PR.
7. **Drop the `BC_PREDICTION_V2` flag** after one week of stable operation.

### 11.1 Rollback plan

If the new path misbehaves, the rollback is straightforward:

1. Set `BC_PREDICTION_V2=false`.
2. The old REST-poll path resumes within one `POLL_INTERVAL_MS`.
3. The new `pending_predictions` rows with `model_name='v2'` can be deleted or marked as `prediction_status='cancelled'`:

```sql
UPDATE pending_predictions
SET prediction_status = 'cancelled'
WHERE model_name = 'v2' AND matched = false;
```

The `notification_outbox` rows for `model_name='v2'` are drained normally (the dispatcher does not filter by model_name) or can be marked as `dead_letter`.

### 11.2 Data backfill considerations

If the production deployment already has `pending_predictions` rows with `target_game_id` set by the old validation path, the new partial UNIQUE index will not conflict because the old rows have `matched=true` (the partial index is `WHERE matched = false`). The new code only inserts new rows; it never updates old ones.

---

## 12. Summary of the Strict Temporal Guarantee

The architecture satisfies the user's requirement because:

1. **The trigger is now `round_starting`** (BC.Game's authoritative "round N+1 is starting" signal), not a REST poll that observes already-settled rounds. The `round_starting` payload includes the next round's `gameId` and `beginTime`.
2. **The prediction row is `INSERT`ed synchronously** in the same transaction as the outbox row, with `target_game_id` and `target_round_started_at` set from the streaming payload — both timestamps are BC.Game's, not local clocks.
3. **The temporal inequality is provable from persisted data**: `pending_predictions.requested_at < pending_predictions.target_round_started_at` for every row (verifiable via the SQL queries in §10.1).
4. **Telegram delivery is decoupled** via a transactional outbox, so the prediction latency is bounded only by the prediction engine itself, not by Telegram's response time. The outbox dispatcher has retry, backoff, dead-letter, and stale-sending recovery.
5. **Idempotency is enforced at three layers**:
   - The partial UNIQUE index on `pending_predictions(target_game_id, model_name) WHERE matched = false` (replay of the same `round_starting` is a no-op).
   - The `UNIQUE` on `prediction_validations(game_id)` from migration `0007` (replay of the same `round_resolved` is a no-op).
   - The `ON CONFLICT DO NOTHING` on every INSERT.
6. **Recovery is automatic**:
   - Socket reconnects are handled by `socket.io-client`.
   - Worker crashes are recovered by the outbox dispatcher's "stale sending" recovery query (30 s ceiling).
   - Missed rounds are backfilled by the REST reconciler (60 s interval); the reconciler never generates predictions, so there is no double-generation.
   - Failed Telegram sends are retried with exponential backoff (1 s, 2 s, 4 s) and dead-lettered after 3 attempts.
7. **Failure isolation is complete**:
   - Telegram outages, DB transaction conflicts, and socket disconnects each fail into a distinct, observable, recoverable state without affecting the other components.
   - The prediction engine never awaits Telegram; the validator never awaits Telegram; both write to the outbox and return immediately.
8. **The model input is strictly historical**: `loadRecentRoundsForPrediction` uses `WHERE crashed_at <= ${input.targetBeganAt}::timestamptz`, so the target round's own multiplier is never fed in. The prediction is mathematically an estimate of round N+1's multiplier given the 100 most recent settled rounds whose `crashed_at` precedes `targetBeganAt`.

The single most important architectural change is **replacing the REST poll with the Socket.IO streaming consumer**. Every other improvement (transactional outbox, atomic `target_game_id` at generation time, partial UNIQUE index per model, REST reconciliation, observability correlation IDs) is supporting infrastructure for that change. The user's strict requirement — `prediction_generated_at < target_round_started_at < target_round.crashed_at` — is now provable from persisted data and not assumed from code structure.

---

## 13. Supplementary Findings from the Attached Streaming-Pipeline Investigation

The companion investigation report (`bcgame-crash-streaming-pipeline-investigation.md`) confirms the streaming architecture but flags three material risks not addressed in the initial specification above. They are documented here as required additions to the implementation.

### 13.1 Dynamic socket endpoint via `socketDomain` (REQUIRED)

The evidence matrix entry from the report (line 133) states:

> Current `/account/get/` endpoint returns `socketDomain` for socket override
> `Zf().then(...)` → `Rg(c.socketDomain)` → connect manager + `/game-support` namespace

This means the hardcoded URL `wss://socketv4.bc.game` may not be correct for all deployments. The production engine must call `/api/account/get/` first and use the `socketDomain` field it returns. Add this to `bc-socket.ts`:

```ts
async function discoverSocketDomain(): Promise<string> {
  const baseUrl = process.env.BC_REST_URL ?? "https://bc.game";
  try {
    const response = await fetch(`${baseUrl}/api/account/get/`, {
      headers: {
        accept: "application/json",
        "user-agent": "Mozilla/5.0 (compatible; CrashWave/1.0)",
        origin: baseUrl,
        referer: `${baseUrl}/game/crash`,
      },
    });
    if (!response.ok) throw new Error(`account_get ${response.status}`);
    const body = await response.json();
    const domain = body?.data?.socketDomain;
    if (typeof domain === "string" && domain.length > 0) return domain;
  } catch (e) {
    logger.warn({ component: "bc-socket", error: String(e) }, "socketDomain discovery failed; using default");
  }
  return process.env.BC_SOCKET_URL ?? "wss://socketv4.bc.game";
}
```

The `BcSocketClient.start()` method must call `discoverSocketDomain()` before `connect()` and use the returned URL. This must be retried on a 30-second backoff if the discovery call fails.

### 13.2 Event name sniffer — current production names are unverified (REQUIRED)

The report's "What Remains Unverified" section (line 394) explicitly states:

> Exact current Crash socket event names (`pr`, `b`, `ed`, `pg`, etc.) — The crash-specific JS chunk is lazily loaded and was not isolated during investigation; direct file fetch returns 403 (WAF block)

The event names `pr` / `bg` / `ed` used in the `bc-socket.ts` implementation above are sourced from a 2021 forum post and may have changed in current production. The implementation must therefore:

1. Subscribe to **all** events on the `/game-support` namespace in a "sniffer" mode during the first 24 hours of operation.
2. Log every received event name and payload structure to a dedicated `socket_event_discovery` table.
3. Map the discovered names to the internal `round_prepare` / `round_starting` / `round_resolved` events based on the payload shape (`{ gameId, beginTime, endTime, rate, hash, salt }`).

Add a `SocketEventSniffer` class:

```ts
// src/lib/crash/socket-event-sniffer.ts
import { getSql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("socket-event-sniffer");

export class SocketEventSniffer {
  private sql = getSql;
  private enabled: boolean;

  constructor() {
    // Enable sniffer mode for the first 24h of operation, or when
    // SOCKET_EVENT_SNIFF=1 is set in the environment.
    this.enabled = process.env.SOCKET_EVENT_SNIFF === "1" || !process.env.SNIFF_DISABLED_AT;
  }

  async record(eventName: string, payload: unknown): Promise<void> {
    if (!this.enabled) return;
    try {
      const sql = await this.sql();
      await sql`
        INSERT INTO socket_event_discovery (event_name, payload, received_at)
        VALUES (${eventName}, ${JSON.stringify(payload)}, now())
      `;
    } catch (e) {
      logger.warn({ component: "socket-event-sniffer", error: String(e) }, "record failed");
    }
  }
}
```

The `bc-socket.ts` `connect()` method must install a catch-all listener:

```ts
this.socket.onAny((eventName: string, ...args: unknown[]) => {
  this.sniffer.record(eventName, args).catch(() => {});
  // Then dispatch to specific handlers if the name is recognized.
  if (eventName === "pr" || eventName === "game_prepare") this.emit("round_prepare", ...);
  // ... etc
});
```

After 24 hours of operation, query `socket_event_discovery` to determine the actual current event names, then update `bc-socket.ts` to dispatch on those names. The sniffer can then be disabled.

### 13.3 Bot protection / WAF handling (REQUIRED)

The report's "Contradictory Evidence" section (line 412) states:

> BC.Game's frontend employs active bot protection (WAF). Direct JS file retrieval returns 403, preventing full source inspection without an authenticated browser session.

The server-side worker will be subject to the same protection. The Socket.IO handshake includes a session identifier and may require:

1. **Realistic browser headers** on the initial HTTP polling request (Socket.IO v4 starts with an HTTP request even when using WebSocket transport). The headers must include:
   - `User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36`
   - `Origin: https://bc.game`
   - `Referer: https://bc.game/game/crash`
   - `Accept: */*`
   - `Accept-Language: en-US,en;q=0.9`
2. **A session cookie** obtained from a prior authenticated visit. If the engine runs without a user session, it may need to use a service-account or public/anonymous mode. This must be verified empirically.
3. **Rate limiting**: the WAF will rate-limit connections from a single IP. The reconnect logic must respect exponential backoff and avoid tight retry loops.

The `BcSocketClient` constructor options must include a custom `transportOptions`:

```ts
this.socket = io(socketUrl, {
  transports: ["websocket"],
  transportOptions: {
    websocket: {
      extraHeaders: {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ...",
        Origin: "https://bc.game",
        Referer: "https://bc.game/game/crash",
      },
    },
  },
  reconnection: true,
  reconnectionDelay: 1000,
  reconnectionDelayMax: 30_000,
  timeout: 20_000,
  autoConnect: false,
});
```

If the WAF rejects the connection with a 403, the engine must:
1. Log the rejection with the response status and body.
2. Back off for 5 minutes.
3. Retry the `socketDomain` discovery call (the WAF may serve a captcha challenge that resolves over time).
4. Alert via `worker_state.last_error` so the operator can investigate manually.

### 13.4 Other items from the report (informational, no change required)

- The `/user` namespace carries `balance-change-v2` events (line 135). The prediction engine does not need to subscribe to this, but a future enhancement could use it to detect when a bet has been placed (for paper-trading validation).
- The bustabit origin (line 131) is strong circumstantial evidence that the `pr` / `bg` / `ed` event names are stable, but the explicit caveat in §13.2 still applies.
- The 2025 scripting API change to callback delegates (line 374-382) is consumer-side only and does not affect the WebSocket protocol.

### 13.5 New table required for §13.2

```sql
-- 0009_socket_event_discovery.sql
CREATE TABLE IF NOT EXISTS socket_event_discovery (
  id          bigserial primary key,
  event_name  text NOT NULL,
  payload     jsonb NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS socket_event_discovery_event_name_idx
  ON socket_event_discovery (event_name, received_at DESC);
```

This table is a temporary observability artifact. After the current event names are confirmed, it can be truncated and the sniffer disabled permanently (or the table dropped).

---

*End of solution specification. Companion document: `DIAGNOSIS.md`. Supplementary findings added after review of the attached streaming-pipeline investigation report.*
