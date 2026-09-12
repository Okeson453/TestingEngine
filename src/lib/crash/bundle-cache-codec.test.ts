/**
 * wr_utils worker_state cache codec — directive 17:10Z.
 *
 * The bundle body is multi-hundred-KB plain text; worker_state reads AND
 * writes of it measured ~0.5s repeatedly in prod (Neon payload transfer,
 * loop_lag=1). v2 gzip+base64 format cuts the stored payload ~4-5x.
 * v1 (plain body) must still decode — no-migration rollback path.
 */

import { describe, it, expect } from "vitest";
import { encodeBundleCache, decodeBundleCache } from "@/lib/crash/native-sign";

// Bundle-sized text: JS-like, repetitive → realistic gzip ratio.
const BUNDLE = Array.from({ length: 8000 }, (_, i) => `function wr${i}(){return ${i}%7};`).join("\n");
const URL = "https://bc.game/_app/immutable/chunks/wr-utils.x.js";

describe("wr_utils worker_state cache codec (17:10Z)", () => {
  it("v2 roundtrip: encode → decode returns the exact body/url/at", () => {
    const encoded = encodeBundleCache(BUNDLE, URL, 1757689573000);
    const decoded = decodeBundleCache(encoded);
    expect(decoded).not.toBeNull();
    expect(decoded!.body).toBe(BUNDLE);
    expect(decoded!.url).toBe(URL);
    expect(decoded!.at).toBe(1757689573000);
  });

  it("v2 payload is materially smaller than the v1 plain body", () => {
    const v1 = JSON.stringify({ body: BUNDLE, url: URL, at: 1 });
    const v2 = encodeBundleCache(BUNDLE, URL, 1);
    // gzip on repetitive JS should compress far more than 2x; assert ≥2x
    // as a conservative floor so the test survives bundle-shape changes.
    expect(v2.length).toBeLessThan(v1.length / 2);
  });

  it("v1 legacy plain format still decodes (rollback safety)", () => {
    const v1 = JSON.stringify({ body: BUNDLE, url: URL, at: 42 });
    const decoded = decodeBundleCache(v1);
    expect(decoded).not.toBeNull();
    expect(decoded!.body).toBe(BUNDLE);
    expect(decoded!.url).toBe(URL);
    expect(decoded!.at).toBe(42);
  });

  it("corrupt / truncated payloads decode to null (treated as no cache)", () => {
    expect(decodeBundleCache("not json at all")).toBeNull();
    expect(decodeBundleCache("{}")).toBeNull();
    expect(decodeBundleCache(JSON.stringify({ url: URL, at: 1 }))).toBeNull();
    // Truncated gzip payload — must not throw, must return null.
    const good = encodeBundleCache(BUNDLE, URL, 1);
    expect(decodeBundleCache(good.slice(0, Math.floor(good.length / 3)))).toBeNull();
  });
});
