/**
 * Notification Outbox Pattern Implementation
 *
 * Provides durable, asynchronous Telegram notification delivery with:
 * - Idempotency (no duplicate notifications)
 * - Retry with exponential backoff
 * - Dead-letter handling
 * - Delivery latency tracking
 * - Transactional atomicity with prediction persistence
 *
 * Specification: UNIFIED_PREDICTION_PIPELINE_SOLUTION.md §10
 */

import { getSql } from "@/lib/db";
import type { Sql } from "@/lib/db";
import { sendTelegramMessage, telegramConfigured, type SendResult } from "./telegram";
import { getLogger } from "@/lib/observability/logger";
import { randomUUID } from "node:crypto";

const logger = getLogger("notification-outbox");

// Notification state types
export type NotificationStatus = "pending" | "delivered" | "failed" | "dead_letter";

export type NotificationType = "prediction" | "validation" | "alert" | "summary";

export interface OutboxNotification {
  id: string;
  notification_id: string;
  type: NotificationType;
  content: string;
  metadata: Record<string, unknown>;
  status: NotificationStatus;
  attempt_count: number;
  next_attempt_at: string | null;
  last_error: string | null;
  created_at: string;
  delivered_at: string | null;
  priority: number;
}

export interface CreateNotificationOptions {
  type: NotificationType;
  content: string;
  metadata?: Record<string, unknown>;
  priority?: number;
  delayMs?: number;
}

// Retry configuration
const MAX_ATTEMPTS = 5;
const BASE_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 300_000; // 5 minutes
const HIGH_PRIORITY_THRESHOLD = 2;
const HIGH_PRIORITY_BACKOFF_MS = 100;

/**
 * Create a new notification in the outbox
 * This is transactional with the prediction creation
 */
export async function createNotification(
  sql: Sql,
  options: CreateNotificationOptions,
): Promise<string> {
  const notificationId = randomUUID();
  const priority = options.priority ?? 0;
  const now = new Date().toISOString();

  // Calculate next attempt time based on priority
  const delayMs = options.delayMs ?? (priority >= HIGH_PRIORITY_THRESHOLD ? 0 : 0);
  const nextAttemptAt = delayMs > 0 ? new Date(Date.now() + delayMs).toISOString() : now;
  // Per-row Telegram delivery deadline (P0 / 6.12). Default 5s from now;
  // dispatcher will not claim rows past this timestamp.
  const deadlineMs = Number(process.env.TELEGRAM_DEADLINE_MS ?? 5_000);
  const telegramDeadlineAt = new Date(Date.now() + deadlineMs).toISOString();

  await sql`
    insert into notification_outbox (
      notification_id, type, content, metadata, status,
      attempt_count, next_attempt_at, created_at, priority, telegram_deadline_at
    ) values (
      ${notificationId}, ${options.type}, ${options.content}, 
      ${JSON.stringify(options.metadata ?? {})}, 'pending',
      0, ${nextAttemptAt}, ${now}, ${priority}, ${telegramDeadlineAt}::timestamptz
    )
  `;

  logger.debug(
    { component: "NotificationOutbox", notificationId, type: options.type, priority },
    "Created notification in outbox",
  );

  return notificationId;
}

/**
 * Create notification transactionally with prediction
 * This ensures that if prediction creation fails, notification is not created
 */
export async function createNotificationWithPrediction(
  sql: Sql,
  predictionId: string,
  options: Omit<CreateNotificationOptions, "metadata"> & {
    metadata?: Record<string, unknown> & { predictionId?: string };
  },
): Promise<string> {
  // Ensure metadata includes predictionId for correlation
  const metadata = {
    predictionId,
    ...options.metadata,
  };

  return createNotification(sql, {
    ...options,
    metadata,
  });
}

/**
 * Get next batch of notifications to deliver
 */
export async function getPendingNotifications(sql: Sql, limit = 10): Promise<OutboxNotification[]> {
  const now = new Date().toISOString();

  const rows = await sql<OutboxNotification>`
    select id, notification_id, type, content, metadata, status,
           attempt_count, next_attempt_at, last_error, created_at,
           delivered_at, priority
    from notification_outbox
    where status = 'pending' and next_attempt_at <= ${now}
    order by 
      case when priority >= ${HIGH_PRIORITY_THRESHOLD} then 0 else 1 end,
      next_attempt_at asc,
      created_at asc
    limit ${limit}
    for update skip locked
  `;

  return rows.map((row) => ({
    ...row,
    metadata: row.metadata as Record<string, unknown>,
  }));
}

/**
 * Mark notification as delivered
 */
export async function markDelivered(
  sql: Sql,
  notificationId: string,
  deliveredAt: string = new Date().toISOString(),
): Promise<void> {
  await sql`
    update notification_outbox
    set status = 'delivered', 
        delivered_at = ${deliveredAt},
        attempt_count = attempt_count + 1,
        last_error = null,
        next_attempt_at = null
    where notification_id = ${notificationId} and status = 'pending'
  `;

  logger.debug(
    { component: "NotificationOutbox", notificationId },
    "Marked notification as delivered",
  );
}

/**
 * Update notification attempt status
 */
export async function updateAttempt(
  sql: Sql,
  notificationId: string,
  error: string | null,
  nextAttemptAt: string | null,
): Promise<void> {
  await sql`
    update notification_outbox
    set attempt_count = attempt_count + 1,
        last_error = ${error},
        next_attempt_at = ${nextAttemptAt},
        updated_at = now()
    where notification_id = ${notificationId} and status = 'pending'
  `;

  if (error) {
    logger.warn(
      { component: "NotificationOutbox", notificationId, error },
      "Notification delivery attempt failed",
    );
  }
}

/**
 * Move notification to dead letter queue
 */
export async function moveToDeadLetter(
  sql: Sql,
  notificationId: string,
  error: string,
): Promise<void> {
  await sql`
    update notification_outbox
    set status = 'dead_letter',
        last_error = ${error},
        next_attempt_at = null,
        updated_at = now()
    where notification_id = ${notificationId} and status = 'pending'
  `;

  logger.error(
    { component: "NotificationOutbox", notificationId, error },
    "Notification moved to dead letter queue",
  );
}

/**
 * Calculate next backoff delay
 */
function calculateBackoff(attemptCount: number, priority: number): number {
  const baseBackoff =
    priority >= HIGH_PRIORITY_THRESHOLD ? HIGH_PRIORITY_BACKOFF_MS : BASE_BACKOFF_MS;

  const exponentialBackoff = baseBackoff * Math.pow(2, attemptCount - 1);
  return Math.min(exponentialBackoff, MAX_BACKOFF_MS);
}

/**
 * Deliver a single notification
 */
async function deliverNotification(sql: Sql, notification: OutboxNotification): Promise<boolean> {
  if (!telegramConfigured()) {
    logger.debug(
      { component: "NotificationOutbox", notificationId: notification.notification_id },
      "Telegram not configured, skipping delivery",
    );
    await markDelivered(sql, notification.notification_id);
    return true;
  }

  try {
    const results = await sendTelegramMessage(notification.content);
    const allSuccessful = results.every((r) => r.ok);

    if (allSuccessful) {
      await markDelivered(sql, notification.notification_id);
      logger.info(
        {
          component: "NotificationOutbox",
          notificationId: notification.notification_id,
          type: notification.type,
        },
        "Notification delivered successfully",
      );
      return true;
    } else {
      // Partial failure - consider as failed for retry
      const errorSummary = results
        .filter((r) => !r.ok)
        .map((r) => `${r.chatId || "unknown"}:${r.error || "unknown_error"}`)
        .join("; ");

      throw new Error(`Partial delivery failure: ${errorSummary}`);
    }
  } catch (error) {
    const errorMessage = (error as Error).message;
    const nextBackoffMs = calculateBackoff(notification.attempt_count + 1, notification.priority);
    const nextAttemptAt = new Date(Date.now() + nextBackoffMs).toISOString();

    if (notification.attempt_count + 1 >= MAX_ATTEMPTS) {
      await moveToDeadLetter(sql, notification.notification_id, errorMessage);
      return false;
    } else {
      await updateAttempt(sql, notification.notification_id, errorMessage, nextAttemptAt);
      logger.info(
        {
          component: "NotificationOutbox",
          notificationId: notification.notification_id,
          attempt: notification.attempt_count + 1,
          nextAttemptAt,
          backoffMs: nextBackoffMs,
        },
        "Scheduled notification retry",
      );
      return false;
    }
  }
}

/**
 * Process pending notifications in the outbox
 * This is called periodically to deliver notifications
 */
export async function processOutbox(
  sql: Sql,
  batchSize = 10,
): Promise<{
  processed: number;
  delivered: number;
  failed: number;
  deadLetter: number;
}> {
  const result = { processed: 0, delivered: 0, failed: 0, deadLetter: 0 };

  const pending = await getPendingNotifications(sql, batchSize);

  for (const notification of pending) {
    result.processed++;

    try {
      const success = await deliverNotification(sql, notification);
      if (success) {
        result.delivered++;
      } else {
        result.failed++;
      }
    } catch (error) {
      result.failed++;
      logger.error(
        {
          component: "NotificationOutbox",
          notificationId: notification.notification_id,
          error: error as Error,
        },
        "Failed to process notification",
      );
    }
  }

  return result;
}

/**
 * Create prediction notification in outbox
 * This is the new way to queue prediction notifications
 */
export async function createPredictionNotification(
  sql: Sql,
  options: {
    predictionId: string;
    targetMultiplier: number;
    probability: number;
    confidence: number;
    regimeName: string | null;
    lastRoundMultiplier: number | null;
    generatedAt: string;
    correlationId?: string;
  },
): Promise<string> {
  const content = formatPredictionMessageForOutbox(options);

  return createNotificationWithPrediction(sql, options.predictionId, {
    type: "prediction",
    content,
    metadata: {
      predictionId: options.predictionId,
      targetMultiplier: options.targetMultiplier,
      probability: options.probability,
      confidence: options.confidence,
      regimeName: options.regimeName,
      lastRoundMultiplier: options.lastRoundMultiplier,
      correlationId: options.correlationId ?? null,
      generatedAt: options.generatedAt,
    },
    priority: HIGH_PRIORITY_THRESHOLD + 1, // Predictions preempt validations (rec 5.6)
  });
}

/**
 * Create validation notification in outbox
 */
export async function createValidationNotification(
  sql: Sql,
  options: {
    predictionId: string;
    gameId: string;
    targetMultiplier: number;
    actualMultiplier: number;
    probability: number;
    result: "WIN" | "LOSS";
    resolvedAt: string;
  },
): Promise<string> {
  const content = formatValidationMessageForOutbox(options);

  return createNotificationWithPrediction(sql, options.predictionId, {
    type: "validation",
    content,
    metadata: {
      predictionId: options.predictionId,
      gameId: options.gameId,
      targetMultiplier: options.targetMultiplier,
      actualMultiplier: options.actualMultiplier,
      probability: options.probability,
      result: options.result,
    },
    priority: HIGH_PRIORITY_THRESHOLD, // Validations below predictions
  });
}

/**
 * Format prediction message for outbox
 */
function formatPredictionMessageForOutbox(options: {
  predictionId: string;
  targetMultiplier: number;
  probability: number;
  confidence: number;
  regimeName: string | null;
  lastRoundMultiplier: number | null;
  generatedAt: string;
}): string {
  const regimeText = options.regimeName ? ` (${options.regimeName})` : "";
  const lastRoundText = options.lastRoundMultiplier
    ? `Last round: ${options.lastRoundMultiplier.toFixed(2)}x`
    : "";

  return [
    `🎯 NEW PREDICTION${regimeText}`,
    ``,
    `Target: ${options.targetMultiplier.toFixed(2)}x`,
    `Probability: ${(options.probability * 100).toFixed(1)}%`,
    `Confidence: ${(options.confidence * 100).toFixed(1)}%`,
    ``,
    lastRoundText,
    ``,
    `Prediction ID: ${options.predictionId}`,
    `Generated: ${options.generatedAt}`,
  ]
    .filter(Boolean)
    .join("\n");
}

/**
 * Format validation message for outbox
 */
function formatValidationMessageForOutbox(options: {
  predictionId: string;
  gameId: string;
  targetMultiplier: number;
  actualMultiplier: number;
  probability: number;
  result: "WIN" | "LOSS";
  resolvedAt: string;
}): string {
  const resultEmoji = options.result === "WIN" ? "🎉" : "💥";
  const resultText = options.result === "WIN" ? "WIN" : "LOSS";
  const multiplierText =
    options.actualMultiplier >= options.targetMultiplier
      ? `Actual: ${options.actualMultiplier.toFixed(2)}x`
      : `Crashed: ${options.actualMultiplier.toFixed(2)}x`;

  return [
    `${resultEmoji} PREDICTION ${resultText}`,
    ``,
    `Target: ${options.targetMultiplier.toFixed(2)}x`,
    multiplierText,
    `Probability: ${(options.probability * 100).toFixed(1)}%`,
    ``,
    `Game ID: ${options.gameId}`,
    `Prediction ID: ${options.predictionId}`,
    `Resolved: ${options.resolvedAt}`,
  ].join("\n");
}

/**
 * Get outbox statistics
 */
export async function getOutboxStats(sql: Sql): Promise<{
  pending: number;
  delivered: number;
  failed: number;
  deadLetter: number;
  oldestPendingAt: string | null;
  avgDeliveryLatencyMs: number | null;
}> {
  const rows = await sql<{
    status: string;
    count: number;
    oldest_pending: string | null;
    avg_latency: number | null;
  }>`
    select 
      status,
      count(*)::int as count,
      min(created_at) filter (where status = 'pending') as oldest_pending,
      avg(extract(epoch from (delivered_at - created_at)) * 1000) 
        filter (where status = 'delivered' and delivered_at is not null) as avg_latency
    from notification_outbox
    group by status
  `;

  const stats: Record<string, number> = {};
  let oldestPendingAt: string | null = null;
  let avgDeliveryLatencyMs: number | null = null;

  for (const row of rows) {
    stats[row.status] = row.count;
    if (row.status === "pending" && row.oldest_pending) {
      oldestPendingAt = row.oldest_pending;
    }
    if (row.status === "delivered" && row.avg_latency) {
      avgDeliveryLatencyMs = Math.round(row.avg_latency);
    }
  }

  return {
    pending: stats.pending ?? 0,
    delivered: stats.delivered ?? 0,
    failed: stats.failed ?? 0,
    deadLetter: stats.dead_letter ?? 0,
    oldestPendingAt,
    avgDeliveryLatencyMs,
  };
}

/**
 * Get recent dead letter notifications for debugging
 */
export async function getDeadLetterNotifications(
  sql: Sql,
  limit = 10,
): Promise<OutboxNotification[]> {
  const rows = await sql<OutboxNotification>`
    select id, notification_id, type, content, metadata, status,
           attempt_count, next_attempt_at, last_error, created_at,
           delivered_at, priority
    from notification_outbox
    where status = 'dead_letter'
    order by created_at desc
    limit ${limit}
  `;

  return rows.map((row) => ({
    ...row,
    metadata: row.metadata as Record<string, unknown>,
  }));
}

export { MAX_ATTEMPTS, BASE_BACKOFF_MS, MAX_BACKOFF_MS, HIGH_PRIORITY_THRESHOLD };
