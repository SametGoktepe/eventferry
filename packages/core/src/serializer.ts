import type { Logger, OutboxRecord, Serializer } from "./types.js";

/**
 * Default serializer: JSON-encodes the record payload to UTF-8 bytes.
 */
export class JsonSerializer implements Serializer {
  readonly contentType = "application/json";

  serialize(record: OutboxRecord): Buffer {
    return Buffer.from(JSON.stringify(record.payload), "utf8");
  }
}

/**
 * Minimal console-backed logger. Swap in pino/winston by implementing Logger.
 */
export class ConsoleLogger implements Logger {
  constructor(private readonly prefix = "[outbox]") {}

  private fmt(
    write: (line: string, meta?: Record<string, unknown>) => void,
    level: string,
    msg: string,
    meta?: Record<string, unknown>,
  ): void {
    const line = `${this.prefix} ${level} ${msg}`;
    if (meta) {
      write(line, meta);
    } else {
      write(line);
    }
  }

  debug(msg: string, meta?: Record<string, unknown>): void {
    // eslint-disable-next-line no-console
    this.fmt(console.debug, "DEBUG", msg, meta);
  }
  info(msg: string, meta?: Record<string, unknown>): void {
    // eslint-disable-next-line no-console
    this.fmt(console.info, "INFO", msg, meta);
  }
  warn(msg: string, meta?: Record<string, unknown>): void {
    // eslint-disable-next-line no-console
    this.fmt(console.warn, "WARN", msg, meta);
  }
  error(msg: string, meta?: Record<string, unknown>): void {
    // eslint-disable-next-line no-console
    this.fmt(console.error, "ERROR", msg, meta);
  }
}

/** Logger that discards everything. Useful for tests. */
export class NoopLogger implements Logger {
  debug(): void {}
  info(): void {}
  warn(): void {}
  error(): void {}
}
