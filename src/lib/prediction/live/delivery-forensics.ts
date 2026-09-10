/**
 * Prediction Delivery Forensics — authoritative per-prediction temporal outcome.
 *
 * Answers: "Was prediction P accepted by Telegram before target N+1 started?"
 * Aggregates distributed timestamps into one durable classification.
 *
 * Outcomes:
 *   ON_TIME  — telegram_accepted_at < target_round_started_at
 *   LATE     — telegram_accepted_at >= target_round_started_at
 *   EXPIRED  — never delivered; killed by BG or deadline (dead_letter)
 *   FAILED   — terminal failure / dead without delivery
 *   UNKNOWN  — delivered but target start not yet known
 */

import type { Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("delivery-forensics");

export type DeliveryOutcome =
  | "ON_TIME"
  | "LATE"
  | "EXPIRED"
  | "FAILED"
  | "UNKNOWN";

export interface DeliveryForensicsRecord {
  predictionId: string | null;
  notificationId: string;
  correlationId: string | null;
  sourceGameId: string | null;
  targetGameId: string | null;
  queuedAt: string | null;
  dispatchStartedAt: string | null;
  telegramSendStartedAt: string | null;
  telegramAcceptedAt: string | null;
  targetRoundStartedAt: string | null;
  queueWaitMs: number | null;
  dispatchMs: number | null;
  telegramSendMs: number | null;
  totalDeliveryMs: number | null;
  leadTimeMs: number | null;
  outcome: DeliveryOutcome;
}

export function classifyDelivery(args: {
  telegramAcceptedAtMs: number | null;
  targetStartedAtMs: number | null;
  outboxStatus: string;
}): { outcome: DeliveryOutcome; leadTimeMs: number | null } {
  const { telegramAcceptedAtMs, targetStartedAtMs, outboxStatus } = args;

  if (outboxStatus === "dead_letter") {
    return { outcome: "EXPIRED", leadTimeMs: null };
  }
  if (outboxStatus === "failed") {
    return { outcome: "FAILED", leadTimeMs: null };
  }
  if (telegramAcceptedAtMs == null || !Number.isFinite(telegramAcceptedAtMs)) {
    return { outcome: "UNKNOWN", leadTimeMs: null };
  }
  // P0: target not started yet at acceptance ⇒ still on-time relative to known state.
  // BG reclassify corrects if we later learn acceptance was after began_at.
  if (targetStartedAtMs == null || !Number.isFinite(targetStartedAtMs)) {
    if (outboxStatus === "delivered") {
      return { outcome: "ON_TIME", leadTimeMs: null };
    }
    return { outcome: "UNKNOWN", leadTimeMs: null };
  }
  const leadTimeMs = targetStartedAtMs - telegramAcceptedAtMs;
  return {
    outcome: leadTimeMs > 0 ? "ON_TIME" : "LATE",
    leadTimeMs: Math.round(leadTimeMs),
  };
}

function msDiff(a: string | Date | null | undefined, b: string | Date | null | undefined): number | null {
  if (a == null || b == null) return null;
  const ta = new Date(a).getTime();
  const tb = new Date(b).getTime();
  if (!Number.isFinite(ta) || !Number.isFinite(tb)) return null;
  return Math.round(ta - tb);
}

/** Persist outcome on outbox row (best-effort columns from migration 0029). */
export async function persistDeliveryOutcome(
  sql: Sql,
  notificationId: string,
  outcome: DeliveryOutcome,
  leadTimeMs: number | null,
): Promise<void> {
  try {
    await sql`
      UPDATE notification_outbox
      SET delivery_outcome = ${outcome},
          lead_time_ms = ${leadTimeMs}
      WHERE notification_id = ${notificationId}::uuid
    `;
  } catch (e) {
    // Column may not exist until migration runs — soft fail.
    logger.debug(
      { notificationId, error: String(e) },
      "persistDeliveryOutcome soft-failed (migration 0029 pending?)",
    );
  }
}

/**
 * After successful Telegram finalize: classify using known target start if any.
 */
export async function recordDeliveredForensics(
  sql: Sql,
  args: {
    notificationId: string;
    predictionId: string | null;
    correlationId: string | null;
    sourceGameId: string | null;
    targetGameId: string | null;
    createdAt: string;
    dispatchClaimedAt?: string | null;
    sendStartedAtMs: number | null;
    telegramAcceptedAtMs: number;
  },
): Promise<DeliveryForensicsRecord> {
  let targetStartedAt: string | null = null;
  if (args.targetGameId) {
    try {
      const rows = await sql<{ began_at: string | Date | null }>`
        SELECT began_at FROM live_round_state WHERE game_id = ${args.targetGameId} LIMIT 1
      `;
      if (rows[0]?.began_at) {
        targetStartedAt = new Date(rows[0].began_at).toISOString();
      }
    } catch {
      /* soft */
    }
    if (!targetStartedAt) {
      try {
        const rows = await sql<{ began_at: string | Date | null }>`
          SELECT began_at FROM crash_rounds WHERE game_id = ${args.targetGameId} LIMIT 1
        `;
        if (rows[0]?.began_at) {
          targetStartedAt = new Date(rows[0].began_at).toISOString();
        }
      } catch {
        /* soft */
      }
    }
  }

  const targetMs = targetStartedAt ? new Date(targetStartedAt).getTime() : null;
  const { outcome, leadTimeMs } = classifyDelivery({
    telegramAcceptedAtMs: args.telegramAcceptedAtMs,
    targetStartedAtMs: targetMs,
    outboxStatus: "delivered",
  });

  const acceptedIso = new Date(args.telegramAcceptedAtMs).toISOString();
  const sendIso =
    args.sendStartedAtMs != null ? new Date(args.sendStartedAtMs).toISOString() : null;

  const rec: DeliveryForensicsRecord = {
    predictionId: args.predictionId,
    notificationId: args.notificationId,
    correlationId: args.correlationId,
    sourceGameId: args.sourceGameId,
    targetGameId: args.targetGameId,
    queuedAt: args.createdAt,
    dispatchStartedAt: args.dispatchClaimedAt ?? null,
    telegramSendStartedAt: sendIso,
    telegramAcceptedAt: acceptedIso,
    targetRoundStartedAt: targetStartedAt,
    queueWaitMs: msDiff(args.dispatchClaimedAt ?? null, args.createdAt),
    dispatchMs: msDiff(sendIso, args.dispatchClaimedAt ?? null),
    telegramSendMs: msDiff(acceptedIso, sendIso),
    totalDeliveryMs: msDiff(acceptedIso, args.createdAt),
    leadTimeMs,
    outcome,
  };

  await persistDeliveryOutcome(sql, args.notificationId, outcome, leadTimeMs);

  const logLevel = outcome === "LATE" ? "warn" : "info";
  logger[logLevel](
    {
      component: "delivery-forensics",
      ...rec,
    },
    `PREDICTION_DELIVERY_FORENSICS ${outcome}`,
  );

  return rec;
}

/**
 * On BG(target): reclassify any delivered prediction for this target with real began_at.
 * Also mark still-pending kills as EXPIRED.
 */
export async function reclassifyOnTargetStart(
  sql: Sql,
  targetGameId: string,
  beganAt: string | Date,
): Promise<void> {
  const beganIso = new Date(beganAt).toISOString();
  const beganMs = new Date(beganAt).getTime();
  if (!Number.isFinite(beganMs)) return;

  try {
    // Delivered rows: compute lead time vs authoritative start
    const delivered = await sql<{
      notification_id: string;
      telegram_accepted_at: string | Date | null;
      metadata: Record<string, unknown> | null;
      created_at: string;
      dispatch_claimed_at: string | Date | null;
      send_started_at: string | Date | null;
      target_game_id: string | null;
    }>`
      SELECT notification_id, telegram_accepted_at, metadata, created_at,
             dispatch_claimed_at, send_started_at, target_game_id
      FROM notification_outbox
      WHERE type = 'prediction'
        AND target_game_id = ${targetGameId}
        AND status = 'delivered'
        AND telegram_accepted_at IS NOT NULL
    `;

    for (const row of delivered) {
      const acceptedMs = row.telegram_accepted_at
        ? new Date(row.telegram_accepted_at).getTime()
        : null;
      const { outcome, leadTimeMs } = classifyDelivery({
        telegramAcceptedAtMs: acceptedMs,
        targetStartedAtMs: beganMs,
        outboxStatus: "delivered",
      });
      const meta = (row.metadata ?? {}) as Record<string, unknown>;
      await persistDeliveryOutcome(sql, row.notification_id, outcome, leadTimeMs);
      const logLevel = outcome === "LATE" ? "warn" : "info";
      logger[logLevel](
        {
          component: "delivery-forensics",
          predictionId: typeof meta.predictionId === "string" ? meta.predictionId : null,
          notificationId: row.notification_id,
          correlationId: typeof meta.correlationId === "string" ? meta.correlationId : null,
          sourceGameId: typeof meta.sourceGameId === "string" ? meta.sourceGameId : null,
          targetGameId,
          telegramAcceptedAt: row.telegram_accepted_at
            ? new Date(row.telegram_accepted_at).toISOString()
            : null,
          targetRoundStartedAt: beganIso,
          leadTimeMs,
          outcome,
          totalDeliveryMs: msDiff(
            row.telegram_accepted_at
              ? new Date(row.telegram_accepted_at).toISOString()
              : null,
            row.created_at,
          ),
        },
        `PREDICTION_DELIVERY_FORENSICS ${outcome} (BG reclassify)`,
      );
    }

    // Dead-letter rows for this target → EXPIRED
    await sql`
      UPDATE notification_outbox
      SET delivery_outcome = 'EXPIRED'
      WHERE type = 'prediction'
        AND target_game_id = ${targetGameId}
        AND status = 'dead_letter'
        AND (delivery_outcome IS NULL OR delivery_outcome = 'UNKNOWN')
    `.catch(() => undefined);
  } catch (e) {
    logger.warn(
      { targetGameId, error: String(e) },
      "reclassifyOnTargetStart failed (soft)",
    );
  }
}
