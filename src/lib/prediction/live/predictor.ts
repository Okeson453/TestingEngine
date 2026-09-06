/**
 * Synchronous-on-event predictor.
 *
 * Spec: TestingEngine_Comprehensive_Diagnosis_and_Solution.md §5–§7
 *
 * PRIMARY production path (ahead-of-time N+1):
 *   ED(N) -> onGameEndPredict -> persist pending for N+1 with generated_at
 *   BG(N+1) -> backfill target_round_started_at only (never creates prediction)
 *
 * Hard temporal invariant:
 *   prediction_generated_at < target_round_started_at < target_round_crashed_at
 *
 * `onGameStart` is retained only for tests / emergency recovery. Production
 * Socket.IO handlers must NOT call it to create predictions.
 */
import { randomUUID } from "node:crypto";
import { getSql, getPgPool, type Sql } from "@/lib/db";
import { runInTransaction } from "@/lib/prediction/live/tx";
import { PredictionEngine } from "@/lib/prediction/prediction-engine";
import type { HistoricalRound, ThresholdTarget } from "@/lib/prediction/types";
import { getConfiguredChatIds } from "@/lib/notifications/telegram";
import { getLogger } from "@/lib/observability/logger";

import {
  evaluateSheath,
  recordPredictionOutcome,
} from "@/lib/core/sheath-mode";

const logger = getLogger("live-predictor");
// SYNTAX_GUARD_20260906: file must parse under node --experimental-strip-types

/** Prediction-related constants. */
const DEFAULT_TARGET: ThresholdTarget = 1.3;
const MIN_HISTORY = 20;
/** Reduced 100->50: halves history query cost on the hot ED path while
 *  remaining well above MIN_HISTORY for model stability. */
const MAX_HISTORY = 50;
/** SLA gate: if the bg payload's `beginTime` is older than this, the
 *  prediction is still persisted (correctness preserved) but the Telegram
 *  outbox writes are skipped to avoid the "predicts the past" operator
 *  symptom. */
export const SLA_LAG_MS = Number(process.env.SLA_LAG_MS ?? 2_000);
/** Residual window below which we skip prediction entirely (no row written).
 *  Lowered 800->250: prior floor systematically skipped hot-ED predictions when
 *  elapsedSinceEd + gate latency consumed a normal 3-5s inter-round gap,
 *  forcing poll recovery 1-3 rounds later (the observed signal lag). */
export const MIN_REQUIRED_WINDOW_MS = Number(process.env.MIN_REQUIRED_WINDOW_MS ?? 250);
/** Stronger short-circuit: only abandon when the window is truly gone. */
export const SKIP_BELOW_MS = Number(process.env.SKIP_BELOW_MS ?? 150);
/** Hard timeout for PredictionEngine.predict (ms). */
export const PREDICT_TIMEOUT_MS = Number(process.env.PREDICT_TIMEOUT_MS ?? 80);
/** Source-event staleness ceiling. If the crash event we're reacting to is
 *  older than this, the live round has almost certainly advanced past the
 *  target. Increased from 15s to 30s to accommodate poll worker recovery path.
 */
export const MAX_SOURCE_ROUND_AGE_MS = Number(process.env.MAX_SOURCE_ROUND_AGE_MS ?? 30_000);

/** DB-level CHECK constraint cap: a bg payload whose `beginTime` is in the
 *  future of the prediction row's `prediction_generated_at` is rejected. */
/** Default 500ms (was 100) - P1 recommendation. */
export const TEMPORAL_TOLERANCE_MS = Number(process.env.TEMPORAL_TOLERANCE_MS ?? 500);

// P2.10: SLA Alert threshold for prediction timing
export const PREDICTION_SLA_THRESHOLD_MS = Number(process.env.PREDICTION_SLA_THRESHOLD_MS ?? 2000);
