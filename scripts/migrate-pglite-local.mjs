/**
 * One-shot: apply pending migrations/ to the local PGLite database
 * (PG_DATA_PATH or data/crashwave). Mirrors scripts/worker.mjs ensureMigrations.
 * Test-infra only — never used in production paths.
 */
import { readdir, readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
process.env.PG_DATA_PATH ||= join(ROOT, "data", "crashwave");

const { PGlite } = await import("@electric-sql/pglite");
const db = new PGlite(process.env.PG_DATA_PATH);
await db.waitReady;

await db.query(
  "CREATE TABLE IF NOT EXISTS _migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
);
const applied = (await db.query("SELECT name FROM _migrations")).rows.map((r) => r.name);
const entries = (await readdir(join(ROOT, "migrations"))).filter((f) => f.endsWith(".sql")).sort();
let count = 0;
for (const name of entries) {
  if (applied.includes(name)) continue;
  const text = await readFile(join(ROOT, "migrations", name), "utf8");
  await db.exec(text);
  await db.query("INSERT INTO _migrations (name) VALUES ($1)", [name]);
  console.log("applied", name);
  count += 1;
}
console.log(`done: ${count} migrations applied`);
process.exit(0);
