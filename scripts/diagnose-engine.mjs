#!/usr/bin/env node
/**
 * Prediction engine subsystem diagnosis.
 *
 * Usage (same loader as worker):
 *   node --experimental-strip-types --import ./scripts/paths-loader.mjs scripts/diagnose-engine.mjs
 *
 * Requires DATABASE_URL. Prints a structured report; exit 1 if critical failures.
 */
import { pathToFileURL } from "node:url";

const issues = [];
const ok = (label) => console.log(`  ✅ ${label}`);
const bad = (label, detail) => {
  console.log(`  ❌ ${label}${detail ? `: ${detail}` : ""}`);
  issues.push(label);
};
const warn = (label, detail) => console.log(`  ⚠️  ${label}${detail ? `: ${detail}` : ""}`);

async function main() {
  console.log("\n=== TestingEngine diagnosis ===\n");

  // 1. Database + pool
  console.log("1. Database / pool");
  try {
    const { getSql, getPoolStats, endPgPool } = await import("../src/lib/db.ts");
    const sql = await getSql();
    const t0 = performance.now();
    await sql`SELECT 1 AS ok`;
    const rtt = Math.round(performance.now() - t0);
    ok(`SELECT 1 ok (${rtt}ms RTT)`);
    if (rtt > 800) warn("High DB RTT", `${rtt}ms — Neon cold or far region`);
    const stats = getPoolStats();
    if (stats) {
      console.log(
        `     pool total=${stats.totalCount} idle=${stats.idleCount} waiting=${stats.waitingCount} max=${stats.max}`,
      );
      if (stats.waitingCount > 0) bad("Pool saturated", `${stats.waitingCount} waiting`);
    }
    // worker_state snapshot
    try {
      const rows = await sql`
        SELECT key, value, updated_at FROM worker_state
        WHERE key IN (
          'socket_status','socket_waf_blocked','socket_last_error',
          'effective_skip_below_ms','worker_heartbeat','last_error'
        )
        ORDER BY key
      `;
      for (const r of rows) {
        console.log(`     worker_state.${r.key} = ${String(r.value).slice(0, 80)}`);
        if (r.key === "socket_waf_blocked" && String(r.value) === "1") {
          bad("Socket WAF blocked (worker_state)");
        }
        if (r.key === "effective_skip_below_ms") {
          const n = Number(r.value);
          if (Number.isFinite(n) && n > 200) {
            warn("effective_skip_below_ms elevated", String(n));
          }
        }
      }
    } catch (e) {
      warn("worker_state read", String(e));
    }
  } catch (e) {
    bad("Database", String(e?.message ?? e));
  }

  // 2. Health
  console.log("\n2. Health");
  try {
    const { getHealthStatus } = await import("../src/lib/health.ts");
    const h = await getHealthStatus();
    console.log(`     status=${h.status}`);
    if (h.status !== "ok" && h.status !== "healthy") bad("Health status", h.status);
    else ok(`Health ${h.status}`);
  } catch (e) {
    warn("Health module", String(e?.message ?? e));
  }

  // 3. Readiness
  console.log("\n3. Readiness");
  try {
    const { getReadinessReport } = await import("../src/lib/observability/readiness.ts");
    const r = await getReadinessReport();
    console.log(`     ready=${r.ready} live=${r.live}`);
    if (r.checks?.database && !r.checks.database.ok) {
      bad("Readiness database", r.checks.database.error);
    } else ok("Readiness database");
    if (r.checks?.workerLock) {
      const wl = r.checks.workerLock;
      console.log(`     lock ok=${wl.ok} owner=${wl.ownerId ?? "none"} ageMs=${wl.ageMs ?? "?"}`);
      if (!wl.ok && wl.ownerId) warn("Worker lock held by another instance", wl.ownerId);
    }
    if (r.checks?.outbox) {
      console.log(`     outbox pending=${r.checks.outbox.pending ?? "?"}`);
      if ((r.checks.outbox.pending ?? 0) > 20) warn("Outbox backlog", String(r.checks.outbox.pending));
    }
  } catch (e) {
    warn("Readiness", String(e?.message ?? e));
  }

  // 4. Socket
  console.log("\n4. Socket.IO");
  try {
    const { getSocketHealth, getSocketDiagnostics } = await import(
      "../src/lib/prediction/live/server.ts"
    );
    const sh = await getSocketHealth();
    console.log(
      `     status=${sh.status} lastError=${sh.lastError ?? "none"} lastEd=${sh.lastEdAt ?? "never"}`,
    );
    if (sh.status === "connected") ok("Socket connected");
    else if (sh.status === "waf_blocked") bad("Socket WAF blocked", "use edge agent / P+T");
    else warn("Socket not connected", sh.status);

    const diag = await getSocketDiagnostics();
    console.log(`     recommendation=${diag.recommendation ?? "?"}`);
    console.log(
      `     dns=${diag.dnsOk} tls=${diag.tlsOk} httpProbe=${diag.httpProbeOk} status=${diag.httpStatus}`,
    );
  } catch (e) {
    warn("Socket health", String(e?.message ?? e));
  }

  // 5. Temporal + latency
  console.log("\n5. Temporal / latency");
  try {
    const {
      getInvariantStatus,
      getAheadOfTimeStats,
      getStuckPredictions,
      getLatencyDashboard,
      getRecentLiveEvents,
    } = await import("../src/lib/prediction/live/server.ts");

    const inv = await getInvariantStatus();
    console.log(`     invariant violations=${inv.violations}/${inv.measurable} total=${inv.total}`);
    if (inv.violations > 0) bad("Temporal violations", String(inv.violations));
    else ok("Temporal invariant");

    const aot = await getAheadOfTimeStats(24);
    console.log(
      `     AOT 24h valid=${aot.valid} late=${aot.late} rate=${((aot.rate ?? 0) * 100).toFixed(1)}%`,
    );
    if (aot.late > 10) warn("Many late predictions (24h)", String(aot.late));

    const stuck = await getStuckPredictions({ minutes: 15 });
    console.log(`     stuck (15m)=${stuck.length}`);
    if (stuck.length > 5) warn("Stuck predictions", String(stuck.length));

    try {
      const lat = await getLatencyDashboard();
      if (lat?.outbox) {
        console.log(
          `     outbox pending=${lat.outbox.pending} delivered=${lat.outbox.delivered}`,
        );
      }
    } catch {
      /* optional */
    }

    const events = await getRecentLiveEvents({ limit: 10 });
    console.log(`     recent live_event_log rows=${events.length}`);
    if (events.length === 0) warn("No recent live events", "socket or poll may be idle");
  } catch (e) {
    warn("Temporal/latency", String(e?.message ?? e));
  }

  // 6. Incremental state / features (in-process only — warm if DB seed ran in this process)
  console.log("\n6. In-process model state (this process only)");
  try {
    const { globalIncrementalState } = await import(
      "../src/lib/prediction/state/incremental-state-engine.ts"
    );
    const warm = globalIncrementalState.isWarm(20);
    const snap = globalIncrementalState.snapshot?.() ?? {};
    console.log(`     isWarm(20)=${warm} count=${snap.count ?? "?"}`);
    if (!warm) warn("Incremental state cold in this process", "worker process may still be warm");
  } catch (e) {
    warn("Incremental state", String(e?.message ?? e));
  }

  // Summary
  console.log("\n=== Summary ===");
  if (issues.length === 0) {
    console.log("No critical issues flagged by this script.");
    console.log("If dashboard still shows Offline, check Railway worker logs for max_client_conn / WAF.");
  } else {
    console.log("Critical:");
    for (const i of issues) console.log(`  - ${i}`);
  }
  console.log("");

  try {
    const { endPgPool } = await import("../src/lib/db.ts");
    await endPgPool();
  } catch {
    /* soft */
  }

  process.exit(issues.length > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error("Diagnosis crashed:", e);
  process.exit(2);
});
