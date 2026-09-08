#!/usr/bin/env node
/**
 * Run live prediction suites one file at a time.
 * Exit 1 if any file fails. Do not swallow failures with || echo.
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
  "src/lib/prediction/acie/acie-upgrade.test.ts",
  "src/lib/prediction/fix-verification.test.ts",
];

const failed = [];

for (const file of files) {
  const result = spawnSync(
    process.execPath,
    [
      "--experimental-strip-types",
      "--import",
      "./scripts/paths-loader.mjs",
      "--test",
      file,
    ],
    { stdio: "inherit", env: process.env },
  );
  if (result.status !== 0) {
    console.error(`LIVE TEST FAILURE: ${file}`);
    failed.push(file);
  }
}

if (failed.length > 0) {
  console.error(`\n${failed.length} live suite(s) failed:\n${failed.map((f) => `  - ${f}`).join("\n")}`);
  process.exit(1);
}
