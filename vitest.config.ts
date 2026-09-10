import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

// Vitest runner for the test files written against the vitest API.
// The node:test files (scripts/*.test.mjs and the live acceptance list in
// package.json) run under `node --test` — see the "test" script.
export default defineConfig({
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  test: {
    include: [
      "src/lib/prediction/calibration/calibration-honesty.test.ts",
      "src/lib/prediction/lifecycle/*.test.ts",
      "src/lib/prediction/live/durable-handoff-ordering.test.ts",
      "src/lib/prediction/live/tx-pool-routing.test.ts",
      "src/lib/prediction/live/zero-db-regression.test.ts",
      "src/lib/prediction/live/outbox-toctou-race.test.ts",
      "src/lib/prediction/live/feedback.test.ts",
      "src/lib/prediction/live/delivery-forensics.test.ts",
      "src/lib/prediction/live/delivery-correlation.test.ts",
      "src/lib/prediction/models/baseline-calibration.test.ts",
      "src/lib/prediction/state/*.test.ts",
    ],
  },
});
