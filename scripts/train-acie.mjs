#!/usr/bin/env node
/**
 * Offline ACIE training — replay crash history through observeRound and
 * persist the learned online state (+ Platt) to acie_online_state.
 *
 * Usage:
 *   DATABASE_URL=... node --experimental-strip-types --import ./scripts/paths-loader.mjs scripts/train-acie.mjs
 *
 * Options:
 *   --limit=N       Max rounds to load (default 5000, newest first then chronological replay)
 *   --min-mult=X    Ignore invalid multipliers below X (default 1.0)
 *   --dry-run       Train in memory only; do not write DB
 *   --from-id=ID    Only rounds with game_id >= ID
 *
 * Live worker already trains online on every ED. This script bootstraps or
 * refreshes the snapshot from durable crash_rounds history.
 */
import { pathToFileURL } from "node:url";

function arg(name, fallback) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  if (!hit) return fallback;
  return hit.slice(name.length + 3);
}

const LIMIT = Math.max(100, Number(arg("limit", 5000)));
const MIN_MULT = Number(arg("min-mult", 1.0));
const DRY = process.argv.includes("--dry-run");
const FROM_ID = arg("from-id", null);

async function main() {
  if (!process.env.DATABASE_URL) {
    console.error("DATABASE_URL is required");
    process.exit(1);
  }

  console.log("\n=== ACIE offline train ===\n");
  console.log(`limit=${LIMIT} dry-run=${DRY} min-mult=${MIN_MULT}`);

  const { getSql, endPgPool } = await import("../src/lib/db.ts");
  const { ACIEEngine } = await import("../src/lib/prediction/acie/engine.ts");
  const { saveAcieStateToDb } = await import(
    "../src/lib/prediction/acie/state-persistence.ts"
  );

  const sql = await getSql();
  const t0 = performance.now();

  let rows;
  if (FROM_ID) {
    rows = await sql`
      SELECT game_id, multiplier, crashed_at
      FROM crash_rounds
      WHERE multiplier >= ${MIN_MULT}
        AND game_id >= ${FROM_ID}
      ORDER BY game_id ASC
      LIMIT ${LIMIT}
    `;
  } else {
    // Newest LIMIT rows, then replay chronological.
    rows = await sql`
      SELECT game_id, multiplier, crashed_at FROM (
        SELECT game_id, multiplier, crashed_at
        FROM crash_rounds
        WHERE multiplier >= ${MIN_MULT}
        ORDER BY game_id DESC
        LIMIT ${LIMIT}
      ) t
      ORDER BY game_id ASC
    `;
  }

  console.log(`loaded ${rows.length} crash_rounds (${Math.round(performance.now() - t0)}ms)`);
  if (rows.length < 20) {
    console.error("Need at least 20 rounds to train meaningfully");
    await endPgPool?.().catch(() => undefined);
    process.exit(1);
  }

  const acie = new ACIEEngine();
  let observed = 0;
  let wins130 = 0;
  const trainT0 = performance.now();

  for (const r of rows) {
    const m = Number(r.multiplier);
    if (!Number.isFinite(m) || m < MIN_MULT) continue;
    acie.observeRound({
      roundId: String(r.game_id),
      crashPoint: m,
      timestamp: r.crashed_at
        ? new Date(r.crashed_at).toISOString()
        : new Date().toISOString(),
    });
    observed += 1;
    if (m >= 1.3) wins130 += 1;
  }

  const trainMs = Math.round(performance.now() - trainT0);
  const online = acie.getOnlineState();
  const snap = acie.exportSnapshot();
  const baseRate = observed > 0 ? wins130 / observed : 0;

  console.log("\n--- Training result ---");
  console.log(`  observations:     ${observed}`);
  console.log(`  train_ms:         ${trainMs}`);
  console.log(`  base_rate_≥1.30:  ${(baseRate * 100).toFixed(2)}%`);
  console.log(`  ewma_hit_rate:    ${online.ewmaHitRate != null ? (online.ewmaHitRate * 100).toFixed(2) + "%" : "n/a"}`);
  console.log(`  ewma_brier:       ${online.ewmaBrier != null ? online.ewmaBrier.toFixed(4) : "n/a"}`);
  console.log(`  crashPoints kept: ${snap.crashPoints?.length ?? 0}`);
  console.log(`  consecutiveLosses:${snap.consecutiveLosses}`);
  if (snap.platt) {
    console.log(
      `  platt:            fitted=${snap.platt.fitted} A=${snap.platt.A?.toFixed?.(4)} B=${snap.platt.B?.toFixed?.(4)} n=${snap.platt.sampleCount}`,
    );
  }
  if (online.ensembleWeights) {
    const w = online.ensembleWeights;
    const top = Object.entries(w)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([k, v]) => `${k}:${Number(v).toFixed(3)}`)
      .join(", ");
    console.log(`  top weights:      ${top}`);
  }

  // Quick self-check: evaluate next after training
  try {
    const ev = acie.evaluateNext({ dailyEntriesUsed: 0, dailyEntriesLimit: 1500 });
    console.log(
      `  next evaluate:    p=${(ev.psi.estimatedProbability * 100).toFixed(2)}% regime=${ev.regime} action=${ev.strategy?.action}`,
    );
  } catch (e) {
    console.log(`  next evaluate:    failed ${e?.message ?? e}`);
  }

  if (DRY) {
    console.log("\n(dry-run) snapshot NOT saved");
  } else {
    const ok = await saveAcieStateToDb(acie);
    console.log(ok ? "\n✅ snapshot saved to acie_online_state" : "\n❌ save failed");
    if (!ok) process.exitCode = 1;
  }

  try {
    await endPgPool?.();
  } catch {
    /* ignore */
  }
  console.log("");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
