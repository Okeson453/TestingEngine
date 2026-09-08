import { test } from "node:test";
import assert from "node:assert/strict";
import { verifyEdgeAuth } from "./edge-ingest.ts";

function withEnv<T>(vars: Record<string, string | undefined>, fn: () => T): T {
  const prev: Record<string, string | undefined> = {};
  for (const k of Object.keys(vars)) {
    prev[k] = process.env[k];
    if (vars[k] === undefined) delete process.env[k];
    else process.env[k] = vars[k] as string;
  }
  try {
    return fn();
  } finally {
    for (const k of Object.keys(prev)) {
      if (prev[k] === undefined) delete process.env[k];
      else process.env[k] = prev[k] as string;
    }
  }
}

test("verifyEdgeAuth: rejects missing token when not opted in", () => {
  withEnv({ EDGE_INGEST_TOKEN: undefined, EDGE_INGEST_ALLOW_INSECURE: undefined }, () => {
    const err = verifyEdgeAuth("Bearer anything");
    assert.ok(err && err.ok === false);
    assert.equal(err.status, 503);
  });
});

test("verifyEdgeAuth: accepts matching bearer token", () => {
  withEnv({ EDGE_INGEST_TOKEN: "edge-secret-token" }, () => {
    assert.equal(verifyEdgeAuth("Bearer edge-secret-token"), null);
    assert.equal(verifyEdgeAuth("edge-secret-token"), null);
  });
});

test("verifyEdgeAuth: rejects wrong token", () => {
  withEnv({ EDGE_INGEST_TOKEN: "edge-secret-token" }, () => {
    const err = verifyEdgeAuth("Bearer wrong-token");
    assert.ok(err && err.ok === false);
    assert.equal(err.status, 401);
  });
});
