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
 * Usage: `node --experimental-strip-types --import ./scripts/paths-loader.mjs scripts/worker.mjs`
 */
import { existsSync, statSync } from "node:fs";
import { dirname, resolve as resolvePath, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { register } from "node:module";

const ROOT = resolvePath(dirname(fileURLToPath(import.meta.url)), "..");

function isTsFile(p) {
  return p.endsWith(".ts");
}

/**
 * Resolve a filesystem path for an extensionless or .ts specifier.
 * Tries: exact, + .ts, /index.ts (directory imports).
 */
function resolveTsCandidate(basePath) {
  if (existsSync(basePath)) {
    try {
      if (statSync(basePath).isFile()) return basePath;
      // Directory → index.ts
      const idx = join(basePath, "index.ts");
      if (existsSync(idx)) return idx;
    } catch {
      /* ignore */
    }
  }
  if (!isTsFile(basePath)) {
    const withTs = basePath + ".ts";
    if (existsSync(withTs)) return withTs;
    const idx = join(basePath, "index.ts");
    if (existsSync(idx)) return idx;
  }
  return null;
}

/** Append `.ts` to a relative specifier when it has no recognised extension. */
function resolveRelative(specifier, parentUrl) {
  const hasExt =
    specifier.endsWith(".ts") ||
    specifier.endsWith(".js") ||
    specifier.endsWith(".json") ||
    specifier.endsWith(".mjs");
  const base = hasExt ? specifier : specifier;
  const from = parentUrl ? dirname(fileURLToPath(parentUrl)) : process.cwd();
  const target = resolvePath(from, base);
  return resolveTsCandidate(target);
}

register(import.meta.url);

export async function resolve(specifier, context, nextResolve) {
  // @/ alias -> <root>/src/...(.ts | /index.ts)
  if (specifier.startsWith("@/")) {
    const target = resolvePath(ROOT, "src", specifier.slice(2));
    const final = resolveTsCandidate(target);
    if (final) {
      return { url: pathToFileURL(final).href, shortCircuit: true };
    }
  }

  // Extensionless relative specifiers -> try resolving them as .ts / index.ts
  if (specifier.startsWith("./") || specifier.startsWith("../")) {
    const resolved = resolveRelative(specifier, context.parentURL);
    if (resolved) {
      return { url: pathToFileURL(resolved).href, shortCircuit: true };
    }
  }

  // Relative with explicit .ts that may still need index resolution is handled above
  return nextResolve(specifier, context);
}
