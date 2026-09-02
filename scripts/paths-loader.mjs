#!/usr/bin/env node
/**
 * Tiny ESM loader so the standalone worker (`scripts/worker.mjs`) can import the
 * TypeScript source tree under `@/...` aliases and extensionless relative
 * specifiers, the same resolution Vite applies during `npm run dev`.
 *
 * Only used when running `npm run worker` directly against a remote DB
 * (DATABASE_URL / Neon). In local PGLite/preview mode the worker runs
 * in-process inside the dev server — there is no separate process.
 *
 * Usage: `node --experimental-strip-types --import ./scripts/paths-loader.mjs ...`
 */
import { existsSync } from "node:fs";
import { dirname, resolve as resolvePath } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = resolvePath(dirname(fileURLToPath(import.meta.url)), "..");

function isTsFile(p: string): boolean {
  return p.endsWith(".ts");
}

/** Append `.ts` to a relative specifier when it has no recognised extension. */
function resolveRelative(specifier: string, parentUrl: string | undefined): string | null {
  const base =
    specifier.endsWith(".ts") ||
    specifier.endsWith(".js") ||
    specifier.endsWith(".json") ||
    specifier.endsWith(".mjs")
      ? specifier
      : specifier + ".ts";
  const from = parentUrl ? dirname(fileURLToPath(parentUrl)) : process.cwd();
  const target = resolvePath(from, base);
  return existsSync(target) ? target : null;
}

export async function resolve(
  specifier: string,
  context: { parentURL?: string },
  nextResolve: (spec: string, ctx: typeof context) => Promise<{ url: string }>,
): Promise<{ url: string; shortCircuit: true }> {
  // @/ alias -> <root>/src/...(.ts)
  if (specifier.startsWith("@/")) {
    const target = resolvePath(ROOT, "src", specifier.slice(2));
    const final = isTsFile(target) ? target : target + ".ts";
    if (existsSync(final)) {
      return { url: pathToFileURL(final).href, shortCircuit: true };
    }
  }

  // Extensionless relative specifiers -> try resolving them as .ts
  if (specifier.startsWith("./") || specifier.startsWith("../")) {
    const resolved = resolveRelative(specifier, context.parentURL);
    if (resolved) {
      return { url: pathToFileURL(resolved).href, shortCircuit: true };
    }
  }

  return nextResolve(specifier, context).then((r) => ({ url: r.url, shortCircuit: false }));
}
