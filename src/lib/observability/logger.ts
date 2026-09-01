export interface Logger {
  debug(...args: unknown[]): void;
  info(...args: unknown[]): void;
  warn(...args: unknown[]): void;
  error(...args: unknown[]): void;
  child(_meta: Record<string, unknown>): Logger;
}
const noop = () => undefined;
const logger: Logger = { debug: noop, info: noop, warn: noop, error: noop, child: () => logger };
export function getLogger(_name?: string): Logger {
  return logger;
}
