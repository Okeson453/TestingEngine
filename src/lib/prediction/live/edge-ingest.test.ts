import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { verifyEdgeAuth } from "./edge-ingest.ts";

describe("verifyEdgeAuth timing-safe compare", () => {
  it("rejects missing token when EDGE_INGEST_TOKEN is set", () => {
    process.env.EDGE_INGEST_TOKEN = "secret-token-value";
    const err = verifyEdgeAuth(null);
    assert.ok(err && err.ok === false);
    assert.equal(err.status, 401);
  });

  it("accepts matching Bearer token", () => {
    process.env.EDGE_INGEST_TOKEN = "secret-token-value";
    const err = verifyEdgeAuth("Bearer secret-token-value");
    assert.equal(err, null);
  });

  it("rejects mismatched token", () => {
    process.env.EDGE_INGEST_TOKEN = "secret-token-value";
    const err = verifyEdgeAuth("Bearer other-token-value");
    assert.ok(err && err.ok === false);
    assert.equal(err.status, 401);
  });

  it("rejects different-length token without throwing", () => {
    process.env.EDGE_INGEST_TOKEN = "secret-token-value";
    const err = verifyEdgeAuth("Bearer short");
    assert.ok(err && err.ok === false);
    assert.equal(err.status, 401);
  });
});
