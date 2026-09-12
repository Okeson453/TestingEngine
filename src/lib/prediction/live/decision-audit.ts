/**
 * Pass 19: durable NO_BET decision audit.
 *
 * Emitted signals persist full provenance (pending_predictions.feature_summary),
 * but REJECTED decisions persisted nothing — so "is the edge threshold
 * miscalibrated?" could never be answered from data. One detached
 * general-pool row per evaluated round, keyed unique on game_id
 * (idempotent under re-runs), joined against crash_rounds.multiplier for
 * the realized 1.30x outcome. Walk-forward / threshold-sweep evaluation
 * is then a pure SQL query.
 *
 * ISOLATION: this module exists so predictor.ts keeps its Zero-DB hot-path
 * invariant (see zero-db-regression.test.ts) — the getSql call lives HERE,
 * detached behind setImmediate; the BG path never awaits it. The table is
 * NOT in boot REQUIRED_TABLES: before migrations/0044 runs, the audit
 * degrades to log-only (warn-once, never an error on the live path).
 */
import { getSql } from "@/lib/db";
import { getLogger } from "@/lib/observability/logger";

const logger = getLogger();

let warned = false;

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
  mode: string | null;
  regime: string | null;
  modelProbabilities: Record<string, number> | null;
  ensembleDisagreement: number | null;
  usedCalibrated: boolean | null;
}

/** Fire-and-forget: schedules the insert, never blocks or throws. */
export function recordNoBetDecision(rec: NoBetDecisionRecord): void {
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
            ${rec.vetoReason}, 'NO_BET', ${rec.mode}, ${rec.regime},
            ${JSON.stringify(rec.modelProbabilities ?? null)}::jsonb,
            ${rec.ensembleDisagreement}, ${rec.usedCalibrated}
          )
          on conflict (game_id) do nothing
        `;
      } catch (err) {
        if (!warned) {
          warned = true;
          logger.warn(
            { component: "live-predictor", error: String(err) },
            "prediction_decisions audit insert failed once — decision audit degraded to log-only (run migrations/0044)",
          );
        }
      }
    })();
  });
}
