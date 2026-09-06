/**
 * Standalone demo: connect to BC.Game Crash event stream and print round lifecycle.
 *
 * Run: npx tsx src/lib/crash/transport/demo.ts
 * (or: bun run src/lib/crash/transport/demo.ts)
 *
 * This connects using the reverse-engineered transport, decodes protobuf
 * events, and prints normalized round data to stdout. No DB, no UI, no
 * external dependencies beyond the WASM sign module.
 */
import { BcGameCrashTransport, type CrashEvent } from "./bcgame-crash-transport";

const log = (msg: string, data?: unknown) => {
  const ts = new Date().toISOString().slice(11, 23);
  if (data) {
    console.log(`[${ts}] ${msg}`, JSON.stringify(data));
  } else {
    console.log(`[${ts}] ${msg}`);
  }
};

const transport = new BcGameCrashTransport({ log });

const eventEmoji: Record<string, string> = {
  pr: "🟡", bg: "🟢", pg: "📈", e: "🏃", ed: "💥", st: "🏁",
};

// Subscribe to all round lifecycle events
const events: Array<"pr" | "bg" | "pg" | "e" | "ed" | "st"> = ["pr", "bg", "pg", "e", "ed", "st"];
for (const ev of events) {
  transport.on(ev, (event: CrashEvent) => {
    const e = event.event;
    switch (e) {
      case "pr":
        log(`${eventEmoji[e]} PREPARE  round=${event.roundId} startTime=${new Date(event.startTime).toISOString()}`);
        break;
      case "bg":
        log(`${eventEmoji[e]} BEGIN    round=${event.roundId}`);
        break;
      case "pg":
        // Progress is high-frequency; only log every 10th
        if (event.multiplier > 0) {
          log(`${eventEmoji[e]} PROGRESS round=${event.roundId} multiplier=${event.multiplier.toFixed(2)}x elapsed=${event.elapsed}ms`);
        }
        break;
      case "e":
        log(`${eventEmoji[e]} ESCAPE   user=${event.userId} betId=${event.betId} odds=${event.odds}`);
        break;
      case "ed":
        log(`${eventEmoji[e]} CRASH    round=${event.roundId} multiplier=${event.multiplier.toFixed(2)}x hash=${event.hash.slice(0, 16)}...`);
        break;
      case "st":
        log(`${eventEmoji[e]} SETTLE   round=${event.roundId} multiplier=${event.multiplier.toFixed(2)}x escapes=${event.escapes.length} hash=${event.hash.slice(0, 16)}...`);
        break;
    }
  });
}

log("Starting BC.Game Crash transport demo...");
log("Press Ctrl+C to stop.");

// Graceful shutdown
process.on("SIGINT", () => {
  log("Shutting down...");
  transport.disconnect();
  process.exit(0);
});

// Start connection
transport.connect().catch((err) => {
  log("Fatal error", { error: String(err) });
  process.exit(1);
});
