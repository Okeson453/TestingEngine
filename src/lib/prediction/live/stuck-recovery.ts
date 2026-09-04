/**
 * Safe recovery for stuck PENDING predictions.
 *
 * Spec: TestingEngine_Comprehensive_Diagnosis_and_Solution.md §9
 *
 *   stuck → inspect target → if target crashed, reconcile
 *         → if still live, retain
 *         → if unrecoverable, cancel
 *
 * Never blindly cancel every stale row.
 */
import type { Sql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("stuck-recovery");

export const STALE_MS = 15 * 60 * 1_000;

export interface StuckRecoveryResult {
  inspected: number;
  reconciled: number;
  cancelled: number;
  stillLive: number;
}

export async function reconcileStuckPredictions(
  sql: Sql,
  staleMs: number = STALE_MS,
): Promise<StuckRecoveryResult> {
  const result: StuckRecoveryResult = {
    inspected: 0,
    reconciled: 0,
    cancelled: 0,
    stillLive: 0,
  };

  const stuck = await sql<{
    prediction_id: string;
    target_game_id: string;
    generated_at: string | Date | null;
    target_round_started_at: string | Date | null;
    requested_at: string | Date;
  }>`
    SELECT prediction_id, target_game_id, generated_at, target_round_started_at, requested_at
    FROM pending_predictions
    WHERE status = 'PENDING'
      AND matched = false
      AND requested_at < now() - (${staleMs}::int * interval '1 millisecond')
    ORDER BY requested_at ASC
    LIMIT 50
  `;

  result.inspected = stuck.length;
  if (stuck.length === 0) return result;

  for (const row of stuck) {
    const target = row.target_game_id;
    try {
      // Has the target already crashed in historical storage?
      const crashed = await sql<{
        game_id: string;
        multiplier: number;
        crashed_at: string | Date;
      }>`
        SELECT game_id, multiplier, crashed_at
        FROM crash_rounds
        WHERE game_id = ${target}
        LIMIT 1
      `;

      if (crashed.length > 0) {
        // Target finished — mark prediction LOST/resolved without inventing outcome
        // if validator already should have run; cancel the stuck PENDING so the
        // unique active-target constraint is released.
        await sql`
          UPDATE pending_predictions
          SET status = 'CANCELLED',
              matched = true,
              reasoning = COALESCE(reasoning, ARRAY[]::text[]) ||
                ARRAY['stuck-recovery: target already in crash_rounds']::text[]
          WHERE prediction_id = ${row.prediction_id}
            AND status = 'PENDING'
        `;
        result.reconciled += 1;
        logger.info(
          {
            component: "stuck-recovery",
            predictionId: row.prediction_id,
            target,
          },
          "stuck PENDING reconciled — target already crashed",
        );
        continue;
      }

      // Live state: still running?
      const live = await sql<{ lifecycle: string }>`
        SELECT lifecycle FROM live_round_state WHERE game_id = ${target} LIMIT 1
      `.catch(() => [] as { lifecycle: string }[]);

      if (live.length > 0) {
        const lc = live[0]!.lifecycle;
        if (lc === "STARTED" || lc === "RUNNING") {
          result.stillLive += 1;
          logger.info(
            {
              component: "stuck-recovery",
              predictionId: row.prediction_id,
              target,
              lifecycle: lc,
            },
            "stuck PENDING retained — target still live",
          );
          continue;
        }
        if (lc === "ENDED" || lc === "RECONCILED") {
          await sql`
            UPDATE pending_predictions
            SET status = 'CANCELLED',
                matched = true,
                reasoning = COALESCE(reasoning, ARRAY[]::text[]) ||
                  ARRAY['stuck-recovery: live state ended']::text[]
            WHERE prediction_id = ${row.prediction_id}
              AND status = 'PENDING'
          `;
          result.reconciled += 1;
          continue;
        }
      }

      // Unrecoverable: no target row, very old, cancel
      const ageMs =
        Date.now() - new Date(row.requested_at).getTime();
      if (ageMs > staleMs * 2) {
        await sql`
          UPDATE pending_predictions
          SET status = 'CANCELLED',
              matched = true,
              reasoning = COALESCE(reasoning, ARRAY[]::text[]) ||
                ARRAY['stuck-recovery: unrecoverable stale']::text[]
          WHERE prediction_id = ${row.prediction_id}
            AND status = 'PENDING'
        `;
        result.cancelled += 1;
        logger.warn(
          {
            component: "stuck-recovery",
            predictionId: row.prediction_id,
            target,
            ageMs,
          },
          "stuck PENDING cancelled as unrecoverable",
        );
      } else {
        result.stillLive += 1;
      }
    } catch (e) {
      logger.warn(
        {
          component: "stuck-recovery",
          predictionId: row.prediction_id,
          error: String(e),
        },
        "stuck recovery item failed",
      );
    }
  }

  return result;
}
