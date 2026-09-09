#!/usr/bin/env node
/**
 * Run live prediction suites independently. Exit 1 if any file fails.
 * CI used to swallow failures with `|| echo LIVE TEST FAILURE`.
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

const failed = [];
for (const t of files) {
  const result = spawnSync(
    process.execPath,
    [
      "--experimental-strip-types",
      "--import",
      "./scripts/paths-loader.mjs",
      "--test",
      t,
    ],
    { stdio: "inherit" },
  );
  if (result.status !== 0) {
    console.error(`LIVE TEST FAILURE: ${t}`);
    failed.push(t);
  }
}

if (failed.length > 0) {
  console.error(`\n${failed.length} live suite(s) failed:\n${failed.map((f) => `  ${f}`).join("\n")}`);
  process.exit(1);
}
console.log("All live suites passed.\n");
