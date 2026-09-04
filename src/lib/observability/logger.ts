/**
 * Structured production logger.
 *
 * Emits JSON lines to stdout/stderr so Railway / Vercel capture them.
 * Levels: debug < info < warn < error. Controlled by LOG_LEVEL (default: info).
 * The `child(meta)` factory produces a derived logger with bound context
 * (e.g. `{ component: "live-predictor" }`) — every line emitted by the child
 * merges that context into the JSON envelope.
 */

export interface Logger {
  debug(...args: unknown[]): void;
  info(...args: unknown[]): void;
  warn(...args: unknown[]): void;
  error(...args: unknown[]): void;
  child(meta: Record<string, unknown>): Logger;
}

type Level = "debug" | "info" | "warn" | "error";

const LEVEL_RANK: Record<Level, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

function parseLevel(raw: string | undefined): Level {
  const v = (raw ?? "info").toLowerCase();
  if (v === "debug" || v === "info" || v === "warn" || v === "error") return v;
  return "info";
}

const minLevel = parseLevel(
  typeof process !== "undefined" ? process.env.LOG_LEVEL : undefined,
);

function serializeArg(arg: unknown): unknown {
  if (arg instanceof Error) {
    return {
      name: arg.name,
      message: arg.message,
      stack: arg.stack,
    };
  }
  if (typeof arg === "object" && arg !== null) return arg;
  return arg;
}

function emit(
  level: Level,
  name: string | undefined,
  base: Record<string, unknown>,
  args: unknown[],
): void {
  if (LEVEL_RANK[level] < LEVEL_RANK[minLevel]) return;

  let meta: Record<string, unknown> = { ...base };
  let message = "";
  const rest: unknown[] = [];

  for (let i = 0; i < args.length; i += 1) {
    const a = args[i];
    if (i === 0 && a && typeof a === "object" && !(a instanceof Error) && !Array.isArray(a)) {
      meta = { ...meta, ...(a as Record<string, unknown>) };
    } else if (typeof a === "string" && !message) {
      message = a;
    } else {
      rest.push(serializeArg(a));
    }
  }

  const line = JSON.stringify({
    level,
    time: new Date().toISOString(),
    name: name ?? undefined,
    msg: message || undefined,
    ...meta,
    ...(rest.length ? { data: rest } : {}),
  });

  if (level === "error" || level === "warn") {
    // eslint-disable-next-line no-console
    console.error(line);
  } else {
    // eslint-disable-next-line no-console
    console.log(line);
  }
}

function createLogger(
  name?: string,
  base: Record<string, unknown> = {},
): Logger {
  return {
    debug: (...args) => emit("debug", name, base, args),
    info: (...args) => emit("info", name, base, args),
    warn: (...args) => emit("warn", name, base, args),
    error: (...args) => emit("error", name, base, args),
    child: (meta) => createLogger(name, { ...base, ...meta }),
  };
}

export function getLogger(name?: string): Logger {
  return createLogger(name);
}
