/**
 * Per-model online performance — EWMA log-loss / Brier for weighting.
 */

import { getLogger } from "@/lib/observability/logger";

const logger = getLogger("model-performance");

// P3.7: Add Alert for Constant Predictions
// Track recent predictions to detect constant output
let recentPredictions: number[] = [];
const MAX_RECENT_PREDICTIONS = 100;
const CONSTANT_THRESHOLD = 0.01; // If all predictions within 1% range

function checkForConstantPredictions(prediction: number): void {
  recentPredictions.push(prediction);
  if (recentPredictions.length > MAX_RECENT_PREDICTIONS) {
    recentPredictions.shift();
  }
  
  if (recentPredictions.length >= 20) {
    const min = Math.min(...recentPredictions);
    const max = Math.max(...recentPredictions);
    const range = max - min;
    
    if (range < CONSTANT_THRESHOLD) {
      logger.error(
        {
          component: "model-performance",
          recentPredictionsCount: recentPredictions.length,
          minPrediction: min,
          maxPrediction: max,
          range: range,
        },
        "ALERT: Constant predictions detected — model may be stuck",
      );
    }
  }
}

export interface ModelPerf {
  ewmaLogLoss: number;
  ewmaBrier: number;
  count: number;
  recentCorrect: number;
  recentTotal: number;
}

const ALPHA = 0.05;

export class ModelPerformanceTracker {
  private readonly models = new Map<string, ModelPerf>();

  ensure(name: string): ModelPerf {
    let p = this.models.get(name);
    if (!p) {
      p = { ewmaLogLoss: 0.5, ewmaBrier: 0.25, count: 0, recentCorrect: 0, recentTotal: 0 };
      this.models.set(name, p);
    }
    return p;
  }

  observe(name: string, probability: number, actual: 0 | 1): void {
    const p = this.ensure(name);
    const q = Math.min(0.999, Math.max(0.001, probability));
    const ll = actual === 1 ? -Math.log(q) : -Math.log(1 - q);
    const br = (q - actual) ** 2;
    p.ewmaLogLoss = ALPHA * ll + (1 - ALPHA) * p.ewmaLogLoss;
    p.ewmaBrier = ALPHA * br + (1 - ALPHA) * p.ewmaBrier;
    p.count += 1;
    p.recentTotal += 1;
    if ((q >= 0.5 && actual === 1) || (q < 0.5 && actual === 0)) p.recentCorrect += 1;
    if (p.recentTotal > 200) {
      p.recentCorrect = Math.floor(p.recentCorrect * 0.9);
      p.recentTotal = Math.floor(p.recentTotal * 0.9);
    }

    // P2.8: Add Metrics Emission for Model Performance
    // Emit structured metrics after each update
    const accuracy = p.count > 0 ? p.recentCorrect / Math.max(1, p.recentTotal) : 0;
    const accuracyPct = (accuracy * 100).toFixed(2);
    logger.info({
      component: "model-performance",
      modelName: name,
      ewmaLogLoss: Number(p.ewmaLogLoss.toFixed(4)),
      ewmaBrier: Number(p.ewmaBrier.toFixed(4)),
      sampleCount: p.count,
      recentAccuracy: accuracyPct,
      recentCorrect: p.recentCorrect,
      recentTotal: p.recentTotal,
    }, "model performance updated");

    // P3.7: Check for constant predictions
    checkForConstantPredictions(probability);
  }

  get(name: string): ModelPerf | undefined {
    return this.models.get(name);
  }

  all(): Map<string, ModelPerf> {
    return this.models;
  }
}

export const globalModelPerformance = new ModelPerformanceTracker();
