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
import { existsSync } from "node:fs";
import { dirname, resolve as resolvePath } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { register } from "node:module";

const ROOT = resolvePath(dirname(fileURLToPath(import.meta.url)), "..");

function isTsFile(p) {
  return p.endsWith(".ts");
}

/** Append `.ts` to a relative specifier when it has no recognised extension. */
function resolveRelative(specifier, parentUrl) {
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

register(import.meta.url);

export async function resolve(specifier, context, nextResolve) {
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

  return nextResolve(specifier, context);
}
