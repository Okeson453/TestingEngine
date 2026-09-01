export const metricsRegistry = {
  counter: () => ({ inc: () => undefined }),
  histogram: () => ({ observe: () => undefined }),
  gauge: () => ({ set: () => undefined }),
};
