/**
 * Metrics registry placeholder.
 * DEFERRED: real prom-client wiring is optional ops polish (issue B5).
 * Hot-path latency uses src/lib/observability/performance/latency.ts instead.
 */
export const metricsRegistry = {
  counter: () => ({ inc: () => undefined }),
  histogram: () => ({ observe: () => undefined }),
  gauge: () => ({ set: () => undefined }),
};
