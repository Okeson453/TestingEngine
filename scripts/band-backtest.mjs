#!/usr/bin/env bun
/**
 * Probability-band backtest report — directive 2026-09-12.
 *
 * Reads the durable decision audit (prediction_decisions, migration 0044)
 * joined against realized outcomes (crash_rounds.multiplier) and prints the
 * empirical performance of EVERY probability band:
 *
 *   65-69.99 | 70-74.99 | 75-76.91 | 76.92-79.99 | 80-84.99 | 85+
 *
 * Per band: sample count, empirical win rate, loss rate, EV at 1.30x,
 * realized edge (win rate - break-even 0.7692), calibration error
 * (mean predicted - mean realized), Brier score, max drawdown (units,
 * chronological), longest losing streak (rounds), Wilson 95% CI.
 * Split: ALL / in-sample (first half by decided_at) / OUT-OF-SAMPLE
 * (second half) / by regime.
 *
 * Usage: DATABASE_URL=... bun scripts/band-backtest.mjs
 * BET_ELIGIBLE signals live in pending_predictions (matched rows carry
 * decision='BET_ELIGIBLE' if recorded) — this script reads
 * prediction_decisions (WATCH + NO_BET tiers); pass --with-signals to also
 * join pending_predictions as the BET_ELIGIBLE tier.
 */
import { Client } from "pg";

const BREAK_EVEN = 1 / 1.3; // 0.769230... mathematical break-even at 1.30x
const PAYOUT = 1.3;

const BANDS = [
  ["65-69.99", 0.65, 0.70],
  ["70-74.99", 0.70, 0.75],
  ["75-76.91", 0.75, BREAK_EVEN],
  ["76.92-79.99", BREAK_EVEN, 0.80],
  ["80-84.99", 0.80, 0.85],
  ["85+", 0.85, 1.01],
];

function wilson95(wins, n) {
  if (n === 0) return "n/a";
  const p = wins / n;
  const z = 1.96;
  const denom = 1 + (z * z) / n;
  const center = p + (z * z) / (2 * n);
  const spread = z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n));
  return `[${((center - spread) / denom).toFixed(3)}, ${((center + spread) / denom).toFixed(3)}]`;
}

function stats(rows) {
  const n = rows.length;
  if (n === 0) return null;
  let wins = 0, brier = 0, predSum = 0;
  for (const r of rows) {
    if (r.win) wins += 1;
    brier += (r.probability - (r.win ? 1 : 0)) ** 2;
    predSum += r.probability;
  }
  const winRate = wins / n;
  // Chronological max drawdown (1 unit per loss, +0.30 per win) + streaks
  let cum = 0, peak = 0, maxDD = 0, streak = 0, maxStreak = 0;
  for (const r of rows) {
    if (r.win) { cum += PAYOUT - 1; streak = 0; } else { cum -= 1; streak += 1; maxStreak = Math.max(maxStreak, streak); }
    peak = Math.max(peak, cum);
    maxDD = Math.min(maxDD, cum - peak);
  }
  return {
    n,
    winRate,
    lossRate: 1 - winRate,
    ev: winRate * (PAYOUT - 1) - (1 - winRate) * 1,
    realizedEdge: winRate - BREAK_EVEN,
    calibError: predSum / n - winRate,
    brier: brier / n,
    maxDD,
    maxStreak,
    ci: wilson95(wins, n),
  };
}

function printBandTable(title, rows) {
  console.log(`\n== ${title} (n=${rows.length}) ==`);
  console.log(
    "band         n     win%    loss%   EV/1u   edge    calibErr  Brier   maxDD  maxLossStreak  95% CI",
  );
  for (const [label, lo, hi] of BANDS) {
    const band = rows.filter((r) => r.probability >= lo && r.probability < hi);
    const s = stats(band);
    if (!s) { console.log(`${label.padEnd(12)} 0`); continue; }
    console.log(
      `${label.padEnd(12)} ${String(s.n).padStart(5)} ${(s.winRate * 100).toFixed(2).padStart(6)} ` +
      `${(s.lossRate * 100).toFixed(2).padStart(6)} ${s.ev.toFixed(4).padStart(7)} ` +
      `${(s.realizedEdge * 100).toFixed(2).padStart(6)}pp ${(s.calibError * 100).toFixed(2).padStart(6)}pp ` +
      `${s.brier.toFixed(4).padStart(7)} ${String(s.maxDD).padStart(5)} ${String(s.maxStreak).padStart(8)}   ${s.ci}`,
    );
  }
}

async function main() {
  const client = new Client({ connectionString: process.env.DATABASE_URL });
  await client.connect();
  const res = await client.query(`
    select d.game_id, d.probability, d.confidence, d.decision, d.regime,
           d.decided_at, d.used_calibrated,
           (c.multiplier >= d.target_multiplier) as win
    from prediction_decisions d
    join crash_rounds c on c.game_id = d.game_id
    order by d.decided_at asc
  `);
  await client.end();
  const rows = res.rows.map((r) => ({
    probability: Number(r.probability),
    win: r.win === true,
    regime: r.regime,
    decidedAt: r.decided_at,
    calibrated: r.used_calibrated === true,
  }));
  if (rows.length === 0) {
    console.log("no decision-audit rows joined to outcomes yet — run the worker first");
    return;
  }
  const total65 = rows.filter((r) => r.probability >= 0.65).length;
  console.log(`prediction_decisions joined to outcomes: ${rows.length} rows; 65%+ coverage: ${total65} (${((total65 / rows.length) * 100).toFixed(1)}%)`);
  console.log(`break-even at 1.30x = ${BREAK_EVEN.toFixed(4)} (win rate below this = negative EV)`);

  printBandTable("ALL ROUNDS", rows);
  const mid = Math.floor(rows.length / 2);
  printBandTable("IN-SAMPLE (first half, chronological)", rows.slice(0, mid));
  printBandTable("OUT-OF-SAMPLE (second half, chronological)", rows.slice(mid));

  const regimes = [...new Set(rows.map((r) => r.regime).filter(Boolean))];
  for (const regime of regimes) {
    printBandTable(`REGIME=${regime}`, rows.filter((r) => r.regime === regime));
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
