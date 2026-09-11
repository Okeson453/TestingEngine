/**
 * Process-level failure handlers for the standalone worker (Phase 13).
 *
 * Extracted from worker.mjs so the registration logic is unit-testable.
 * CRITICAL: these must be installed at the VERY TOP of the worker entry —
 * before ensureMigrations / bootWithRetry / any dynamic import — because the
 * crash that motivated this (a raw TypeError from a setTimeout inside the
 * dynamically-imported wr_utils bundle) fires DURING boot, before the old
 * post-boot registration ever ran. Unregistered, it produced a raw Node
 * crash trace and a 3-cycle ~25s restart loop.
 *
 * Injected deps (exit / endPool) exist purely for tests.
 *
 * @param {{ fatalEnv?: NodeJS.ProcessEnv, exit?: (code: number) => void, endPool?: () => Promise<void> | void, console?: Console }} opts
 * @returns {() => void} disposer — removes the listeners (tests).
 */
export function registerProcessFailureHandlers(opts = {}) {
  const fatalEnv = opts.fatalEnv ?? process.env;
  const exit = opts.exit ?? ((code) => process.exit(code));
  const endPool = opts.endPool ?? null;
  const out = opts.console ?? console;

  const serialize = (err) => {
    const errorObj = err instanceof Error ? err : { message: String(err), name: "Unknown" };
    const chain = [];
    // Full cause chain (report #2): wrap(asyncFn) patterns bury the root
    // cause in error.cause — losing it makes prod rejections undebuggable.
    let cause = errorObj.cause;
    let depth = 0;
    while (cause && depth < 5) {
      chain.push(
        cause instanceof Error
          ? { name: cause.name, message: cause.message }
          : { name: "Unknown", message: String(cause) },
      );
      cause = cause.cause;
      depth += 1;
    }
    return {
      name: errorObj.name,
      message: errorObj.message,
      stack: String(errorObj.stack ?? "").slice(0, 4000),
      ...(chain.length > 0 ? { cause: chain } : {}),
    };
  };

  const onUncaughtException = (err) => {
    out.error(
      JSON.stringify({
        level: "fatal",
        time: new Date().toISOString(),
        component: "worker-entry",
        msg: "uncaughtException",
        error: serialize(err),
      }),
    );
    // Only hard-exit on explicit fatal / OOM-style errors. NOTE: FATAL is
    // case-SENSITIVE on purpose — with /i, any stack containing a path with
    // the substring "fatal" (e.g. worker-fatal.test.mjs) would hard-exit.
    const stack = String(err?.stack ?? err);
    const fatal =
      fatalEnv.WORKER_FATAL_ON_UNCAUGHT === "1" ||
      /out of memory|Cannot find module|FATAL/.test(stack);
    if (fatal) {
      try {
        void endPool?.();
      } catch {
        /* ignore */
      }
      exit(1);
    }
  };

  const onUnhandledRejection = (reason) => {
    // Sandbox-contained rejections (sep 11 boot log: unhandledRejection in
    // the same tick as "wr_utils loaded"): rejections originating INSIDE the
    // node:vm wr_utils bundle (identifier "wr_utils.vm.mjs") cannot touch
    // host state — the context is an explicit allowlist with no fs/net/
    // process. The verified bundle has a dead fetch branch; its stub
    // rejection escapes the bundle unhandled. These are benign by
    // construction and must never be fatal-worthy, even if
    // WORKER_FATAL_ON_UNCAUGHT=1 is set — log-and-continue at info level.
    const sandboxStack = String(reason instanceof Error ? reason.stack ?? reason.message : reason);
    if (sandboxStack.includes("wr_utils.vm.mjs")) {
      out.log(
        JSON.stringify({
          level: "info",
          time: new Date().toISOString(),
          component: "worker-entry",
          msg: "benign unhandledRejection from wr_utils sandbox bundle (contained, no host access)",
          error: serialize(reason),
        }),
      );
      return;
    }
    out.error(
      JSON.stringify({
        level: "fatal",
        time: new Date().toISOString(),
        component: "worker-entry",
        msg: "unhandledRejection",
        error: serialize(reason),
      }),
    );
    if (fatalEnv.WORKER_FATAL_ON_UNCAUGHT === "1") {
      try {
        void endPool?.();
      } catch {
        /* ignore */
      }
      exit(1);
    }
  };

  process.on("uncaughtException", onUncaughtException);
  process.on("unhandledRejection", onUnhandledRejection);

  return function off() {
    process.off("uncaughtException", onUncaughtException);
    process.off("unhandledRejection", onUnhandledRejection);
  };
}
