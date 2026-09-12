/**
 * Signal funnel telemetry — daily stage counters for the prediction path.
 *
 * Directive (2026-09-12): candidate_rounds, eligible_rounds,
 * predictions_generated, calibrated_predictions, edge/confidence/quality/
 * risk/temporal passes, signals_created, signals_persisted,
 * signals_dispatched, no_bet_by_reason, and the final daily signal count.
 *
 * Design constraints:
 *  - In-memory, UTC-day keyed (no DB writes, zero latency added to the
 *    BG prediction path — same pattern as wakeStats).
 *  - Railway strips ALL JSON log fields, so the funnel is emitted as a
 *    plain-text console line: every FUNNEL_LOG_EVERY candidate rounds and
 *    at UTC day rollover.
 *  - Gate counters record ALL gates per round (marginal counts), while
 *    no_bet_by_reason records the terminal (first-hit) veto — both views
 *    are needed to see whether an AND-chain of individually reasonable
 *    gates is collectively over-filtering.
 *  - Per-round veto VALUES (p, needP, edge, minEdge) are already inlined
 *    into the NO_BET message by edgeDiagText (pass 19/20); this module
 *    only aggregates.
 */

type DayCounters = {
  date: string;
  candidate_rounds: number;
  eligible_rounds: number;
  predictions_generated: number;
  calibrated_predictions: number;
  edge_pass: number;
  confidence_pass: number;
  quality_pass: number;
  risk_pass: number;
  temporal_pass: number;
  signals_created: number;
  signals_persisted: number;
  signals_dispatched: number;
  watch_rounds: number;
  no_bet_total: number;
  no_bet_by_reason: Record<string, number>;
};

const FUNNEL_LOG_EVERY = Math.max(
  1,
  Number(process.env.FUNNEL_LOG_EVERY ?? 25),
);

let day: DayCounters = newDay();

function newDay(): DayCounters {
  return {
    date: new Date().toISOString().slice(0, 10),
    candidate_rounds: 0,
    eligible_rounds: 0,
    predictions_generated: 0,
    calibrated_predictions: 0,
    edge_pass: 0,
    confidence_pass: 0,
    quality_pass: 0,
    risk_pass: 0,
    temporal_pass: 0,
    signals_created: 0,
    signals_persisted: 0,
    signals_dispatched: 0,
    watch_rounds: 0,
    no_bet_total: 0,
    no_bet_by_reason: {},
  };
}

function rolloverIfNeeded(): void {
  const today = new Date().toISOString().slice(0, 10);
  if (today !== day.date) {
    // Final line for the closed day, then reset.
    logFunnelLine(`day-close ${day.date}`);
    day = newDay();
  }
}

function funnelLine(prefix: string): string {
  const d = day;
  const reasons = Object.entries(d.no_bet_by_reason)
    .sort((a, b) => b[1] - a[1])
    .map(([k, v]) => `${k}=${v}`)
    .join(" ");
  return (
    `[funnel] ${prefix} date=${d.date} ` +
    `candidate_rounds=${d.candidate_rounds} eligible_rounds=${d.eligible_rounds} ` +
    `predictions_generated=${d.predictions_generated} calibrated_predictions=${d.calibrated_predictions} ` +
    `edge_pass=${d.edge_pass} confidence_pass=${d.confidence_pass} ` +
    `quality_pass=${d.quality_pass} risk_pass=${d.risk_pass} temporal_pass=${d.temporal_pass} ` +
    `signals_created=${d.signals_created} signals_persisted=${d.signals_persisted} ` +
    `signals_dispatched=${d.signals_dispatched} watch_rounds=${d.watch_rounds} ` +
    `no_bet_total=${d.no_bet_total} ` +
    `no_bet_by_reason: ${reasons || "none"}`
  );
}

/** Plain-text funnel line (console.log — survives Railway's JSON stripping). */
export function logFunnelLine(prefix = "snapshot"): void {
  try {
    console.log(funnelLine(prefix));
  } catch {
    /* telemetry must never throw */
  }
}

function bump<K extends keyof DayCounters>(key: K, by = 1): void {
  rolloverIfNeeded();
  const cur = day[key];
  if (typeof cur === "number") {
    (day[key] as number) = cur + by;
  }
}

/** WS round observed / attempt started (superset of eligible). */
export function recordCandidateRound(): void {
  bump("candidate_rounds");
  if (day.candidate_rounds % FUNNEL_LOG_EVERY === 0) logFunnelLine("periodic");
}

/** Round passed ownership claim (BG/ED owns the N+1 target). */
export function recordEligibleRound(): void {
  bump("eligible_rounds");
}

/** Probability produced for the target (any kind). */
export function recordPredictionGenerated(): void {
  bump("predictions_generated");
}

/** Platt/isotonic calibration was applied to the emitted probability. */
export function recordCalibratedPrediction(): void {
  bump("calibrated_predictions");
}

/** Gate results for one evaluated round — pass `false` per failing gate. */
export function recordGateResults(gates: {
  edge: boolean;
  confidence: boolean;
  quality: boolean;
  risk: boolean;
  temporal: boolean;
}): void {
  if (gates.edge) bump("edge_pass");
  if (gates.confidence) bump("confidence_pass");
  if (gates.quality) bump("quality_pass");
  if (gates.risk) bump("risk_pass");
  if (gates.temporal) bump("temporal_pass");
}

/** Terminal (first-hit) NO_BET veto for the round. */
export function recordNoBet(reason: string): void {
  bump("no_bet_total");
  rolloverIfNeeded();
  day.no_bet_by_reason[reason] = (day.no_bet_by_reason[reason] ?? 0) + 1;
}

/** Signal crossed the full gate chain (kind=predicted). */
export function recordSignalCreated(): void {
  bump("signals_created");
}

/** Durable outbox handoff enqueued (outboxEnqueued > 0). */
export function recordSignalPersisted(): void {
  bump("signals_persisted");
}

/** Dispatcher delivered the notification (type=prediction). */
export function recordSignalDispatched(): void {
  bump("signals_dispatched");
}

/** 65%+ prediction recorded for band backtest (WATCH tier, never delivered). */
export function recordWatch(): void {
  bump("watch_rounds");
}

/** Snapshot of the current UTC day's counters (tests / metrics endpoint). */
export function getFunnelSnapshot(): DayCounters {
  rolloverIfNeeded();
  return { ...day, no_bet_by_reason: { ...day.no_bet_by_reason } };
}

/** Test helper. */
export function _resetFunnelForTests(): void {
  day = newDay();
}
