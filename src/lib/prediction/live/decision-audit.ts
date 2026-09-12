/**
 * Pass 19: durable decision audit for every evaluated round (including
 * ≥65% predictions that are NOT betting signals).
 *
 * Emitted signals persist full provenance (pending_predictions.feature_summary).
 * REJECTED / non-eligible decisions used to persist nothing — so band
 * backtests could never answer "is the edge threshold miscalibrated?".
 * One detached general-pool row per evaluated round, keyed unique on
 * game_id (idempotent under re-runs), joined against crash_rounds.multiplier
 * for the realized 1.30x outcome.
 *
 * ISOLATION: getSql lives HERE so predictor.ts keeps its Zero-DB hot-path
 * invariant (zero-db-regression.test.ts). Detached behind setImmediate —
 * the BG path never awaits it. Table is NOT in boot REQUIRED_TABLES:
 * before migrations/0044 runs, the audit degrades to log-only (never an
 * error on the live path).
 *
 * Directive 2026-09-12 (persistence ≥65%): every prediction with
 * probability >= PREDICTION_FLOOR is recorded with its taxonomy tier
 * (PREDICTION_65_PLUS / WATCH / BREAK_EVEN_ZONE / BET_CANDIDATE). Only
 * NO_BET (<0.65) is excluded by construction. Success is logged inline
 * so Railway (which strips JSON context fields) still shows the audit.
 */
import { getSql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger();

let warnedMissingTable = false;

export interface NoBetDecisionRecord {
  gameId: string;
  sourceGameId: string | null;
  targetMultiplier: number;
  probability: number;
  confidence: number;
  fairProbability: number;
  minEdge: number;
  needProbability: number;
  edge: number;
  vetoReason: string | null;
  /**
   * Taxonomy tier from classifyPredictionTier:
   *   PREDICTION_65_PLUS | WATCH | BREAK_EVEN_ZONE | BET_CANDIDATE | NO_BET
   * BET_ELIGIBLE never lands here (persisted via pending_predictions).
   */
  decision?: string;
  mode: string | null;
  regime: string | null;
  modelProbabilities: Record<string, number> | null;
  ensembleDisagreement: number | null;
  usedCalibrated: boolean | null;
}

/** Fire-and-forget: schedules the insert, never blocks or throws. */
export function recordNoBetDecision(rec: NoBetDecisionRecord): void {
  const tier = rec.decision ?? "NO_BET";
  const pPct = (rec.probability * 100).toFixed(2);
  // Immediate visibility: the schedule itself is logged so operators can
  // distinguish "betting signal not enqueued" from "decision audit lost".
  // Railway strips structured fields — keep the facts in the message text.
  logger.info(
    {
      component: "decision-audit",
      gameId: rec.gameId,
      tier,
      probability: rec.probability,
      vetoReason: rec.vetoReason,
    },
    `decision audit scheduled gameId=${rec.gameId} tier=${tier} p=${pPct}% veto=${rec.vetoReason ?? "none"}`,
  );

  setImmediate(() => {
    void (async () => {
      try {
        const gsql = await getSql();
        await gsql`
          insert into prediction_decisions (
            game_id, source_game_id, target_multiplier,
            probability, confidence, fair_probability, min_edge,
            need_probability, edge, veto_reason, decision, mode,
            regime, model_probabilities, ensemble_disagreement,
            used_calibrated
          ) values (
            ${rec.gameId}, ${rec.sourceGameId}, ${rec.targetMultiplier},
            ${rec.probability}, ${rec.confidence}, ${rec.fairProbability},
            ${rec.minEdge}, ${rec.needProbability}, ${rec.edge},
            ${rec.vetoReason}, ${tier}, ${rec.mode}, ${rec.regime},
            ${JSON.stringify(rec.modelProbabilities ?? null)}::jsonb,
            ${rec.ensembleDisagreement}, ${rec.usedCalibrated}
          )
          on conflict (game_id) do nothing
        `;
        // Confirm durable write for ≥65% coverage rows (band-backtest feed).
        if (rec.probability >= 0.65) {
          logger.info(
            {
              component: "decision-audit",
              gameId: rec.gameId,
              tier,
              probability: rec.probability,
            },
            `decision audit persisted gameId=${rec.gameId} tier=${tier} p=${pPct}%`,
          );
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        // Always surface failures for ≥65% rows — silent degradation blocks
        // empirical band evaluation. Missing-table stays warn-once to avoid
        // log storms before migrations/0044 is applied.
        const missingTable =
          /prediction_decisions/i.test(msg) &&
          /(does not exist|undefined_table|relation)/i.test(msg);
        if (missingTable) {
          if (!warnedMissingTable) {
            warnedMissingTable = true;
            logger.warn(
              { component: "decision-audit", error: msg },
              "prediction_decisions missing — run migrations/0044; decision audit degraded to log-only",
            );
          }
          return;
        }
        logger.warn(
          { component: "decision-audit", gameId: rec.gameId, tier, error: msg },
          `decision audit INSERT failed gameId=${rec.gameId} tier=${tier} p=${pPct}% error=${msg}`,
        );
      }
    })();
  });
}
