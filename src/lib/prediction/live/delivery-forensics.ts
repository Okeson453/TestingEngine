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
import { getRoundStartedAtMs } from "@/lib/prediction/live/live-round-registry";

const logger = getLogger("delivery-forensics");

export type DeliveryOutcome =
  | "EARLY"
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

export const EARLY_LEAD_MS = Number(process.env.DELIVERY_EARLY_LEAD_MS ?? 4_000);

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
  // DELIVERED + NO TARGET START YET = UNKNOWN (documented semantics).
  // This used to optimistically return ON_TIME, which silently converts
  // "outcome not yet determinable" into a healthy count — and if the BG
  // reclassification later fails (best-effort), the row stays miscast as
  // ON_TIME forever. UNKNOWN is truthful; the forensic reconciliation
  // sweep upgrades it to EARLY/ON_TIME/LATE once target start is known.
  if (targetStartedAtMs == null || !Number.isFinite(targetStartedAtMs)) {
    return { outcome: "UNKNOWN", leadTimeMs: null };
  }
  const leadTimeMs = targetStartedAtMs - telegramAcceptedAtMs;
  // EARLY: delivered with comfortable margin — operationally distinct from a
  // 1-2ms hairline ON_TIME delivery (same delivery, different health).
  if (leadTimeMs >= EARLY_LEAD_MS) {
    return { outcome: "EARLY", leadTimeMs: Math.round(leadTimeMs) };
  }
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
let forensicWriteFailures = 0;

/** Durable-failure telemetry (remediation §6): forensic writes never break
 * delivery, but failures are COUNTED and the reconciliation sweep retries
 * the work from authoritative timestamps — nothing is silently discarded. */
export function getForensicFailureCount(): number {
  return forensicWriteFailures;
}

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
    forensicWriteFailures += 1;
    // Column may not exist until migration runs, or the pool may be under
    // pressure — soft fail for delivery, but NOT silent: counted, warned,
    // and later repaired by reconcileForensicOutcomes() from raw timestamps.
    logger.warn(
      { notificationId, error: String(e) },
      "persistDeliveryOutcome failed — queued for forensic reconciliation sweep",
    );
  }
}

/**
 * DURABLE FORENSIC RECONCILIATION (remediation §5/§6/§7/§8).
 *
 * Delivered rows whose stored delivery_outcome is NULL/UNKNOWN are
 * re-derived from the AUTHORITATIVE raw timestamps (telegram_accepted_at vs
 * target start from live_round_state / crash_rounds / pending_predictions)
 * and the stored outcome corrected. This is the retry mechanism that makes
 * best-effort forensic writes safe: any failure (pool timeout, crash) is
 * repaired on the next sweep. Idempotent; safe to run repeatedly.
 *
 * Also reports rows where the stored outcome DISAGREES with the raw
 * timeline (the "masked LATE" audit) via the returned counts.
 */
export interface ForensicReconcileResult {
  scanned: number;
  reclassified: number;
  maskedLate: number;
  mismatches: number;
}

export async function reconcileForensicOutcomes(
  sql: Sql,
  batchSize = 200,
): Promise<ForensicReconcileResult> {
  const result: ForensicReconcileResult = {
    scanned: 0,
    reclassified: 0,
    maskedLate: 0,
    mismatches: 0,
  };
  try {
    // Delivered rows with incomplete/stale classification. Raw timestamps
    // are authoritative; delivery_outcome is only a cache.
    //
    // AUDIT 2026-09-11 (general-pool latency — the recurrent ~725ms query):
    // Three waste patterns fixed across revisions:
    //
    // 1. 'LATE' was in the re-check set, but LATE is TERMINAL: accepted_at
    //    is frozen and target_started_at backfills only move EARLIER
    //    (COALESCE picks the first resolved begin time; BG/lrs/cr all stamp
    //    the same event), so the lead can only shrink — a stored LATE can
    //    never recompute to non-LATE. Excluded entirely.
    // 2. No recency bound originally: the sweep rescanned the newest 200
    //    delivered rows (plus their 3-join fan-out) every ~60s forever.
    // 3. PRODUCTION 2026-09-11 (slow_query_ms=700-757 persisted even after
    //    the 2h window): the masked-late audit re-checked ON_TIME/EARLY rows
    //    for the FULL 2h window. Classification stabilizes within ~1 minute
    //    of delivery (BG(N) backfills started_at right after the signal), so
    //    re-checking a stable ON_TIME row for 2h is ~120x wasted work — and
    //    that standing scan occupied a general-pool connection (max 5) while
    //    the BG prediction path was waiting on the same pool.
    //
    // FIX: split into TWO bounded scans, each served by its own partial
    // index (migration 0040):
    //   REPAIR — NULL/UNKNOWN outcomes, 24h window (recovery of failed
    //            forensic writes; idempotent, rare).
    //   AUDIT  — ON_TIME/EARLY outcomes, 15-minute window (masked-late
    //            detection; outcome is immutable after target start
    //            backfills, which happens ≤1 min post-delivery).
    const repairHours = (() => {
      const raw = Number(process.env.FORENSIC_REPAIR_WINDOW_HOURS ?? 24);
      return Number.isFinite(raw) && raw >= 1 ? Math.floor(raw) : 24;
    })();
    const auditMinutes = (() => {
      const raw = Number(process.env.FORENSIC_AUDIT_WINDOW_MINUTES ?? 15);
      return Number.isFinite(raw) && raw >= 1 ? Math.floor(raw) : 15;
    })();
    const selectColumns = `
      SELECT o.notification_id, o.telegram_accepted_at, o.delivery_outcome,
             o.metadata, o.created_at, o.dispatch_claimed_at,
             o.send_started_at, o.target_game_id,
             COALESCE(p.target_round_started_at, lrs.began_at, cr.began_at) AS target_started_at
      FROM notification_outbox o
      LEFT JOIN pending_predictions p
        ON p.prediction_id = o.metadata->>'predictionId'
      LEFT JOIN live_round_state lrs
        ON lrs.game_id = coalesce(o.target_game_id, o.metadata->>'targetGameId')
      LEFT JOIN LATERAL (
        SELECT began_at FROM crash_rounds
        WHERE game_id = coalesce(o.target_game_id, o.metadata->>'targetGameId')
        ORDER BY began_at DESC LIMIT 1
      ) cr ON true
    `;
    type ForensicRow = {
      notification_id: string;
      telegram_accepted_at: string | Date | null;
      delivery_outcome: string | null;
      target_started_at: string | Date | null;
      metadata: Record<string, unknown> | null;
      created_at: string;
      dispatch_claimed_at: string | Date | null;
      send_started_at: string | Date | null;
      target_game_id: string | null;
    };

    // REPAIR scan: rows whose forensic write never landed (crash between
    // delivery and persist). Bounded to 24h — older NULLs are noise.
    // sql.query (raw text + params) — the tagged wrapper does not support
    // nested fragments, so the shared SELECT body is inlined verbatim.
    const repairRows = await sql.query<ForensicRow>(
      `${selectColumns}
      WHERE o.type = 'prediction'
        AND o.status = 'delivered'
        AND o.telegram_accepted_at IS NOT NULL
        AND o.delivered_at > now() - ($1::int * interval '1 hour')
        AND (o.delivery_outcome IS NULL OR o.delivery_outcome = 'UNKNOWN')
      ORDER BY o.delivered_at DESC NULLS LAST
      LIMIT $2`,
      [repairHours, batchSize],
    );

    // AUDIT scan: recently-delivered, optimistically-classified rows —
    // the only place a "masked LATE" can still be hiding. 15 minutes.
    const auditRows = await sql.query<ForensicRow>(
      `${selectColumns}
      WHERE o.type = 'prediction'
        AND o.status = 'delivered'
        AND o.telegram_accepted_at IS NOT NULL
        AND o.delivered_at > now() - ($1::int * interval '1 minute')
        AND o.delivery_outcome IN ('ON_TIME', 'EARLY')
      ORDER BY o.delivered_at DESC NULLS LAST
      LIMIT $2`,
      [auditMinutes, batchSize],
    );

    const rows = [...repairRows, ...auditRows];

    for (const row of rows) {
      result.scanned += 1;
      const acceptedMs = row.telegram_accepted_at
        ? new Date(row.telegram_accepted_at).getTime()
        : null;
      const targetMs = row.target_started_at
        ? new Date(row.target_started_at).getTime()
        : null;
      if (acceptedMs == null || !Number.isFinite(acceptedMs)) continue;

      const { outcome, leadTimeMs } = classifyDelivery({
        telegramAcceptedAtMs: acceptedMs,
        targetStartedAtMs: targetMs != null && Number.isFinite(targetMs) ? targetMs : null,
        outboxStatus: "delivered",
      });

      // MASKED LATE (remediation §9/§10): the raw timeline says the signal
      // was accepted at/after target start, but the stored outcome does not
      // say LATE. This is exactly how real late deliveries used to vanish.
      if (outcome === "LATE" && row.delivery_outcome !== "LATE") {
        result.maskedLate += 1;
      }
      if (row.delivery_outcome != null && row.delivery_outcome !== outcome) {
        result.mismatches += 1;
      }

      if (row.delivery_outcome === outcome) continue;

      await persistDeliveryOutcome(sql, row.notification_id, outcome, leadTimeMs);
      result.reclassified += 1;
      const logLevel = outcome === "LATE" ? "warn" : "info";
      logger[logLevel](
        {
          component: "delivery-forensics",
          event: "PREDICTION_DELIVERY_RECONCILE",
          notificationId: row.notification_id,
          previousOutcome: row.delivery_outcome,
          outcome,
          leadTimeMs,
        },
        `FORENSIC_RECONCILE ${row.delivery_outcome ?? "NULL"} -> ${outcome}`,
      );
    }
    return result;
  } catch (e) {
    forensicWriteFailures += 1;
    logger.warn(
      { error: String(e) },
      "reconcileForensicOutcomes sweep failed — will retry next interval",
    );
    return result;
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
    /** Server-stamped send_started_at (auth RETURNING) — preferred clock. */
    sendStartedAtServerIso?: string | null;
    telegramAcceptedAtMs: number;
    /** Server-stamped telegram_accepted_at (finalize RETURNING) — preferred clock. */
    serverAcceptedAtIso?: string | null;
  },
): Promise<DeliveryForensicsRecord> {
  let targetStartedAt: string | null = null;
  if (args.targetGameId) {
    // Zero-RTT: same-process BG already noted target start — avoids UNKNOWN
    // when evidence exists in the registry while DB lag would leave NULL.
    const memMs = getRoundStartedAtMs(args.targetGameId);
    if (memMs != null) {
      targetStartedAt = new Date(memMs).toISOString();
    }
  }
  if (args.targetGameId && !targetStartedAt) {
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
  // CLOCK HYGIENE (forensic report Issue 3): server stamps are authoritative;
  // the client clock is a fallback for rows finalized before the server-stamp
  // path existed. Each duration below is derived from exactly one clock:
  //   queueWaitMs    server (dispatch_claimed_at − created_at)
  //   dispatchMs     server (send_started_at − dispatch_claimed_at)
  //   telegramSendMs client pair, or server pair when both server stamps exist
  //   totalDeliveryMs accepted − created (same clock basis as acceptedIso)
  const acceptedMsAuthoritative = args.serverAcceptedAtIso
    ? new Date(args.serverAcceptedAtIso).getTime()
    : args.telegramAcceptedAtMs;
  const { outcome, leadTimeMs } = classifyDelivery({
    telegramAcceptedAtMs: acceptedMsAuthoritative,
    targetStartedAtMs: targetMs,
    outboxStatus: "delivered",
  });

  const acceptedIso = args.serverAcceptedAtIso
    ? args.serverAcceptedAtIso
    : new Date(args.telegramAcceptedAtMs).toISOString();
  const sendIso =
    args.sendStartedAtServerIso ??
    (args.sendStartedAtMs != null ? new Date(args.sendStartedAtMs).toISOString() : null);
  // Telegram-send duration must never mix clocks: prefer the server pair
  // (send_started_at → telegram_accepted_at), fall back to the client pair.
  const telegramSendMs =
    args.serverAcceptedAtIso && args.sendStartedAtServerIso
      ? msDiff(args.serverAcceptedAtIso, args.sendStartedAtServerIso)
      : msDiff(
          new Date(args.telegramAcceptedAtMs).toISOString(),
          args.sendStartedAtMs != null ? new Date(args.sendStartedAtMs).toISOString() : null,
        );

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
    telegramSendMs,
    totalDeliveryMs: msDiff(acceptedIso, args.createdAt),
    leadTimeMs,
    outcome,
  };

  await persistDeliveryOutcome(sql, args.notificationId, outcome, leadTimeMs);

  const logLevel = outcome === "LATE" ? "warn" : "info";
  logger[logLevel](
    {
      component: "delivery-forensics",
      // Authoritative per-prediction delivery record: one structured line
      // with the complete timestamp chain (queued -> claimed -> send started
      // -> Telegram accepted -> target start) so production latency can be
      // attributed leg-by-leg instead of inferred from adjacent messages.
      event: "PREDICTION_DELIVERY",
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
