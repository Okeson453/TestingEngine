/**
 * Pipeline cold/warm state metrics for dashboard monitoring.
 *
 * P3.6: Add Dashboard Metric for Pipeline Cold/Warm State
 */

import { getLogger } from "../logger";

const logger = getLogger("pipeline-metrics");

// Track pipeline state
let pipelineState: "cold" | "warming" | "warm" = "cold";
let lastStateChange: number = Date.now();
let observationCount: number = 0;
let lastWarmCheck: number = 0;

// Thresholds for state transitions
const COLD_TO_WARMING_THRESHOLD = 10; // observations
const WARMING_TO_WARM_THRESHOLD = 50; // observations
const WARM_MIN_DURATION_MS = 30_000; // 30 seconds

/**
 * Update pipeline observation count and state
 */
export function recordPipelineObservation(): void {
  observationCount++;
  updatePipelineState();
}

/**
 * Update pipeline state based on observation count and time
 */
function updatePipelineState(): void {
  const now = Date.now();
  const timeSinceChange = now - lastStateChange;

  if (pipelineState === "cold" && observationCount >= COLD_TO_WARMING_THRESHOLD) {
    pipelineState = "warming";
    lastStateChange = now;
    logger.info(
      { component: "pipeline-metrics", state: pipelineState, observationCount },
      "Pipeline state transitioned to warming",
    );
  } else if (pipelineState === "warming" && observationCount >= WARMING_TO_WARM_THRESHOLD) {
    pipelineState = "warm";
    lastStateChange = now;
    logger.info(
      { component: "pipeline-metrics", state: pipelineState, observationCount },
      "Pipeline state transitioned to warm",
    );
  } else if (pipelineState === "warm" && timeSinceChange < WARM_MIN_DURATION_MS) {
    // Stay warm
  } else if (pipelineState === "warm" && timeSinceChange >= 60_000) {
    // Check if we've gone cold (no observations for a while)
    if (observationCount < WARMING_TO_WARM_THRESHOLD * 0.5) {
      pipelineState = "cold";
      lastStateChange = now;
      logger.info(
        { component: "pipeline-metrics", state: pipelineState, observationCount },
        "Pipeline state transitioned to cold (inactivity)",
      );
    }
  }

  // Log state periodically
  if (now - lastWarmCheck > 30_000) {
    lastWarmCheck = now;
    logger.info(
      { component: "pipeline-metrics", state: pipelineState, observationCount },
      "Pipeline state metrics",
    );
  }
}

/**
 * Get current pipeline state for dashboard
 */
export function getPipelineState(): {
  state: "cold" | "warming" | "warm";
  observationCount: number;
  timeInStateMs: number;
} {
  return {
    state: pipelineState,
    observationCount,
    timeInStateMs: Date.now() - lastStateChange,
  };
}

/**
 * Reset pipeline state (useful for testing)
 */
export function resetPipelineState(): void {
  pipelineState = "cold";
  lastStateChange = Date.now();
  observationCount = 0;
  lastWarmCheck = 0;
}

/**
 * Initialize pipeline metrics
 */
export function initPipelineMetrics(): void {
  // Reset on module load
  resetPipelineState();
  
  // Log initial state
  logger.info(
    { component: "pipeline-metrics", state: pipelineState, observationCount },
    "Pipeline metrics initialized",
  );
}

// Initialize on import
initPipelineMetrics();
