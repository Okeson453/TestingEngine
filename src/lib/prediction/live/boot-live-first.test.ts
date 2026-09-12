/**
 * Live-first boot invariants locked in source.
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const bootSrc = readFileSync(join(here, "boot.ts"), "utf8");
const workerSrc = readFileSync(join(here, "../../../../scripts/worker.mjs"), "utf8");

describe("live-first boot architecture", () => {
  it("defers cold-start seeder to background hydration", () => {
    assert.match(bootSrc, /runBackgroundHydration/);
    assert.match(bootSrc, /LIVE PATH READY/);
    assert.match(bootSrc, /withBootStage\("cold-start-seeder"/);
    assert.match(bootSrc, /BACKGROUND hydration/i);
  });

  it("acquires worker lease before starting the live subscriber", () => {
    const leaseIdx = bootSrc.indexOf("distributed worker lease acquired");
    const subIdx = bootSrc.indexOf("subscriber-start");
    assert.ok(leaseIdx > 0 && subIdx > leaseIdx, `lease=${leaseIdx} sub=${subIdx}`);
  });

  it("worker pays Neon TLS before strip-types app imports", () => {
    assert.match(workerSrc, /neon-preconnect/);
    const pre = workerSrc.indexOf("neonPreconnect");
    const bootImport = workerSrc.indexOf('import("@/lib/prediction/live/boot")');
    assert.ok(pre >= 0 && bootImport > pre, `pre=${pre} boot=${bootImport}`);
  });

  it("lease retry uses short interval without removing fencing", () => {
    assert.match(bootSrc, /WORKER_LEASE_RETRY_MS/);
    assert.match(bootSrc, /fencing/i);
  });
});
