/**
 * Socket connectivity diagnostics for Railway → BC.Game path.
 * Spec: Diagnosis P0-3 — do not blindly reconnect when Cloudflare blocks.
 */
import { getLogger } from "@/lib/observability/logger";
import { bcGameSocket, type ConnectionState } from "./socket-client";

const logger = getLogger("socket-diagnostics");

const SOCKET_HOST = process.env.BCGAME_SOCKET_URL ?? "wss://socketv4.bc.game";

export interface SocketDiagnosticReport {
  measuredAt: string;
  socketState: ConnectionState;
  dnsOk: boolean | null;
  tlsOk: boolean | null;
  httpProbeOk: boolean | null;
  httpStatus: number | null;
  recommendation: string;
  details: string[];
}

/**
 * Best-effort probe of the network path to BC.Game Socket.IO.
 * Safe to call from worker; never throws.
 */
export async function runSocketDiagnostics(): Promise<SocketDiagnosticReport> {
  const details: string[] = [];
  const measuredAt = new Date().toISOString();
  const socketState = bcGameSocket.getState();
  let dnsOk: boolean | null = null;
  let tlsOk: boolean | null = null;
  let httpProbeOk: boolean | null = null;
  let httpStatus: number | null = null;

  try {
    const host = SOCKET_HOST.replace(/^wss?:\/\//, "").split("/")[0]!;
    // DNS
    try {
      const dns = await import("node:dns/promises");
      const addrs = await dns.lookup(host, { all: true });
      dnsOk = addrs.length > 0;
      details.push(`dns: ${addrs.map((a) => a.address).join(", ")}`);
    } catch (e) {
      dnsOk = false;
      details.push(`dns_failed: ${String(e)}`);
    }

    // HTTP probe of engine.io polling endpoint (even if we use WS-only)
    try {
      const httpUrl = `https://${host}/socket.io/?EIO=4&transport=polling`;
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), 8_000);
      const res = await fetch(httpUrl, {
        method: "GET",
        signal: ctrl.signal,
        headers: {
          Origin: "https://bc.game",
          "User-Agent":
            process.env.BCGAME_SOCKET_UA ??
            "Mozilla/5.0 (compatible; TestingEngine/1.0)",
        },
      });
      clearTimeout(t);
      httpStatus = res.status;
      httpProbeOk = res.status >= 200 && res.status < 500;
      tlsOk = true;
      const body = (await res.text()).slice(0, 200);
      details.push(`http_probe status=${res.status} body=${body}`);
      if (res.status === 403 || res.status === 503) {
        details.push("likely_cloudflare_or_waf_block");
      }
    } catch (e) {
      const msg = String(e);
      if (msg.includes("certificate") || msg.includes("TLS") || msg.includes("SSL")) {
        tlsOk = false;
      } else if (tlsOk == null) {
        tlsOk = true; // got past TLS enough to fail elsewhere, or network
      }
      httpProbeOk = false;
      details.push(`http_probe_failed: ${msg}`);
    }
  } catch (e) {
    details.push(`probe_error: ${String(e)}`);
  }

  let recommendation: string;
  if (socketState.status === "connected") {
    recommendation = "socket_connected — no action";
  } else if (httpStatus === 403 || details.some((d) => d.includes("cloudflare"))) {
    recommendation =
      "Cloudflare/WAF blocking Railway egress. Application reconnect cannot fix this. " +
      "Options: residential proxy, alternate socketDomain, or rely on poll recovery only.";
  } else if (dnsOk === false) {
    recommendation = "DNS resolution failed for socket host — check network/DNS on Railway.";
  } else if (httpProbeOk === false) {
    recommendation =
      "HTTP/TLS path to socket host failed. Check egress firewall and BC.Game endpoint health.";
  } else if (socketState.status === "waf_blocked") {
    recommendation =
      "Client marked waf_blocked. Wait for backoff or set BCGAME_SOCKET_WAF_BACKOFF_MS. " +
      "Poll worker remains the recovery path.";
  } else {
    recommendation =
      "Socket not connected; poll recovery is active. Investigate ED/BG absence via getSocketHealth.";
  }

  const report: SocketDiagnosticReport = {
    measuredAt,
    socketState,
    dnsOk,
    tlsOk,
    httpProbeOk,
    httpStatus,
    recommendation,
    details,
  };

  logger.info(
    {
      component: "socket-diagnostics",
      status: socketState.status,
      httpStatus,
      dnsOk,
      recommendation,
    },
    "socket diagnostic probe complete",
  );
  return report;
}
