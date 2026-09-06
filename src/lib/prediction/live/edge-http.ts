/**
 * Minimal HTTP server for browser-edge ingest (runs inside the worker process).
 *
 * Env:
 *   EDGE_INGEST_PORT   — if set (e.g. 8091), listen; otherwise no-op
 *   EDGE_INGEST_HOST   — default 0.0.0.0
 *   EDGE_INGEST_TOKEN  — Bearer token required on mutating routes
 */
import { createServer, type IncomingMessage, type ServerResponse, type Server } from "node:http";
import { getLogger } from "@/lib/observability/logger";
import {
  ingestEdgeBg,
  ingestEdgeCrash,
  isEdgeFresh,
  verifyEdgeAuth,
} from "@/lib/prediction/live/edge-ingest";

const logger = getLogger("edge-http");

let server: Server | null = null;

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on("data", (c) => {
      chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c));
      if (chunks.reduce((n, b) => n + b.length, 0) > 64_000) {
        reject(new Error("body too large"));
        req.destroy();
      }
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  });
  res.end(payload);
}

async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? "/", "http://localhost");
  const path = url.pathname.replace(/\/$/, "") || "/";
  const method = (req.method ?? "GET").toUpperCase();

  // CORS for browser agents (token still required)
  res.setHeader("access-control-allow-origin", "*");
  res.setHeader("access-control-allow-headers", "authorization, content-type");
  res.setHeader("access-control-allow-methods", "GET, POST, OPTIONS");
  if (method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  if (method === "GET" && (path === "/edge/health" || path === "/health")) {
    const fresh = await isEdgeFresh();
    sendJson(res, 200, {
      ok: true,
      service: "edge-ingest",
      edgeFresh: fresh.fresh,
      edgeAgeMs: fresh.ageMs,
      lastEdgeGameId: fresh.lastGameId,
    });
    return;
  }

  if (method === "POST" && path === "/edge/crash") {
    let body: unknown = {};
    try {
      const raw = await readBody(req);
      body = raw ? JSON.parse(raw) : {};
    } catch {
      sendJson(res, 400, { ok: false, error: "invalid JSON" });
      return;
    }
    const result = await ingestEdgeCrash(body, req.headers.authorization);
    sendJson(res, result.ok ? 200 : result.status, result);
    return;
  }

  if (method === "POST" && path === "/edge/bg") {
    let body: unknown = {};
    try {
      const raw = await readBody(req);
      body = raw ? JSON.parse(raw) : {};
    } catch {
      sendJson(res, 400, { ok: false, error: "invalid JSON" });
      return;
    }
    const result = await ingestEdgeBg(body, req.headers.authorization);
    sendJson(res, result.ok ? 200 : result.status, result);
    return;
  }

  if (method === "GET" && path === "/edge/auth-check") {
    const err = verifyEdgeAuth(req.headers.authorization);
    if (err) {
      sendJson(res, err.status, err);
      return;
    }
    sendJson(res, 200, { ok: true });
    return;
  }

  sendJson(res, 404, { ok: false, error: "not found" });
}

export async function startEdgeHttpServer(): Promise<Server | null> {
  const port = Number(process.env.EDGE_INGEST_PORT ?? "");
  if (!Number.isFinite(port) || port <= 0) {
    logger.info(
      { component: "edge-http" },
      "EDGE_INGEST_PORT not set — browser-edge HTTP ingest disabled",
    );
    return null;
  }
  if (server) return server;

  const host = process.env.EDGE_INGEST_HOST ?? "0.0.0.0";
  server = createServer((req, res) => {
    void handle(req, res).catch((e) => {
      logger.error({ error: String(e) }, "edge-http handler error");
      try {
        sendJson(res, 500, { ok: false, error: "internal" });
      } catch {
        /* */
      }
    });
  });

  await new Promise<void>((resolve, reject) => {
    server!.once("error", reject);
    server!.listen(port, host, () => resolve());
  });

  logger.info(
    { component: "edge-http", host, port },
    `browser-edge ingest listening on http://${host}:${port}`,
  );
  return server;
}

export async function stopEdgeHttpServer(): Promise<void> {
  if (!server) return;
  const s = server;
  server = null;
  await new Promise<void>((resolve) => {
    s.close(() => resolve());
  });
}
