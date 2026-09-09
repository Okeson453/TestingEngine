import assert from "node:assert/strict";
import { describe, it, beforeEach, afterEach } from "node:test";
import { verifyEdgeAuth } from "./edge-ingest.ts";

describe("verifyEdgeAuth", () => {
  const prevToken = process.env.EDGE_INGEST_TOKEN;
  const prevInsecure = process.env.EDGE_INGEST_ALLOW_INSECURE;
  const prevNode = process.env.NODE_ENV;

  beforeEach(() => {
    process.env.EDGE_INGEST_TOKEN = "secret-token-value";
    delete process.env.EDGE_INGEST_ALLOW_INSECURE;
  });

  afterEach(() => {
    if (prevToken === undefined) delete process.env.EDGE_INGEST_TOKEN;
    else process.env.EDGE_INGEST_TOKEN = prevToken;
    if (prevInsecure === undefined) delete process.env.EDGE_INGEST_ALLOW_INSECURE;
    else process.env.EDGE_INGEST_ALLOW_INSECURE = prevInsecure;
    if (prevNode === undefined) delete process.env.NODE_ENV;
    else process.env.NODE_ENV = prevNode;
  });

  it("accepts matching Bearer token", () => {
    assert.equal(verifyEdgeAuth("Bearer secret-token-value"), null);
  });

  it("rejects wrong token", () => {
    const r = verifyEdgeAuth("Bearer other-token-value");
    assert.ok(r && r.ok === false);
    assert.equal(r.status, 401);
  });

  it("rejects length-mismatched token", () => {
    const r = verifyEdgeAuth("Bearer short");
    assert.ok(r && r.ok === false);
    assert.equal(r.status, 401);
  });

  it("rejects missing header", () => {
    const r = verifyEdgeAuth(null);
    assert.ok(r && r.ok === false);
    assert.equal(r.status, 401);
  });

  it("returns 503 when token unset", () => {
    delete process.env.EDGE_INGEST_TOKEN;
    const r = verifyEdgeAuth("Bearer anything");
    assert.ok(r && r.ok === false);
    assert.equal(r.status, 503);
  });
});
