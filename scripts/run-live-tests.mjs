#!/usr/bin/env node
/**
 * Run live prediction suites independently. Exit 1 if any file fails.
 */
import { spawnSync } from "node:child_process";

const files = [
  "src/lib/prediction/live/integration.test.ts",
  "src/lib/prediction/live/predictor.test.ts",
  "src/lib/prediction/live/validator.test.ts",
  "src/lib/prediction/live/cold-start-seeder.test.ts",
  "src/lib/prediction/live/notification-worker.test.ts",
  "src/lib/prediction/live/concurrency.test.ts",
  "src/lib/prediction/live/acceptance.test.ts",
  "src/lib/prediction/live/spec-acceptance.test.ts",
  "src/lib/prediction/live/temporal-invariant.test.ts",
  "src/lib/prediction/live/handlers-path.test.ts",
  "src/lib/prediction/live/feedback.test.ts",
  "src/lib/prediction/live/edge-ingest.test.ts",
  "src/lib/prediction/acie/acie-upgrade.test.ts",
  "src/lib/prediction/fix-verification.test.ts",
];

let failed = 0;
for (const t of files) {
  const r = spawnSync(
    process.execPath,
    ["--experimental-strip-types", "--import", "./scripts/paths-loader.mjs", "--test", t],
    { stdio: "inherit" },
  );
  if (r.status !== 0) {
    console.error(`LIVE TEST FAILURE: ${t}`);
    failed += 1;
  }
}
if (failed > 0) {
  console.error(`${failed} live suite(s) failed`);
  process.exit(1);
}
