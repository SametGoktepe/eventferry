import type { Logger, Waker } from "@eventferry/core";
import { NoopLogger } from "@eventferry/core";
import { assertIdent } from "./ident.js";

/**
 * Structural subset of `pg.Client` the waker needs. A `pg.Client` satisfies this
 * directly. A pooled client must NOT be used — a LISTENing connection is held for
 * the relay's lifetime and would never return to the pool.
 */
export interface NotificationConnection {
  connect(): Promise<void>;
  query(sql: string): Promise<unknown>;
  on(
    event: "notification",
    listener: (msg: { channel: string; payload?: string }) => void,
  ): unknown;
  on(event: "error", listener: (err: Error) => void): unknown;
  on(event: "end", listener: () => void): unknown;
  end(): Promise<void>;
}

export interface PostgresNotifyWakerOptions {
  /**
   * Creates a fresh, unconnected notification connection (e.g.
   * `() => new pg.Client(config)`). Called on every (re)connect.
   */
  connect: () => NotificationConnection;
  /** LISTEN channel; must match `createNotifyTriggerSql`. Default "outbox". */
  channel?: string;
  /** Base reconnect backoff in ms after a dropped connection. Default 1000. */
  reconnectDelayMs?: number;
  /** Optional logger. Defaults to a no-op. */
  logger?: Logger;
}

/**
 * A {@link Waker} backed by Postgres LISTEN/NOTIFY. Holds a dedicated connection
 * that LISTENs on a channel; each notification wakes the relay. On a dropped
 * connection it reconnects with a fixed backoff — meanwhile the relay's polling
 * covers the gap, so no notification gap can lose an event.
 */
export class PostgresNotifyWaker implements Waker {
  private readonly connectFactory: () => NotificationConnection;
  private readonly channel: string;
  private readonly reconnectDelayMs: number;
  private readonly log: Logger;

  private conn: NotificationConnection | null = null;
  private onWake: (() => void) | null = null;
  private stopped = false;

  constructor(opts: PostgresNotifyWakerOptions) {
    this.connectFactory = opts.connect;
    this.channel = assertIdent(opts.channel ?? "outbox");
    this.reconnectDelayMs = opts.reconnectDelayMs ?? 1000;
    this.log = opts.logger ?? new NoopLogger();
  }

  async start(onWake: () => void): Promise<void> {
    this.onWake = onWake;
    this.stopped = false;
    await this.connectAndListen();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    const conn = this.conn;
    this.conn = null;
    if (!conn) return;
    try {
      await conn.query(`UNLISTEN ${this.channel}`);
    } catch {
      // best effort — the connection may already be gone
    }
    try {
      await conn.end();
    } catch {
      // best effort
    }
  }

  private async connectAndListen(): Promise<void> {
    if (this.stopped) return;
    const conn = this.connectFactory();
    this.conn = conn;
    conn.on("notification", () => this.onWake?.());
    conn.on("error", (err) => this.scheduleReconnect(err));
    conn.on("end", () => this.scheduleReconnect(new Error("connection ended")));
    await conn.connect();
    await conn.query(`LISTEN ${this.channel}`);
  }

  private scheduleReconnect(err: Error): void {
    if (this.stopped) return;
    this.conn = null;
    this.log.warn("notify waker connection lost; reconnecting", {
      error: err.message,
    });
    setTimeout(() => {
      if (this.stopped) return;
      this.connectAndListen().catch((e) => {
        const error = e instanceof Error ? e : new Error(String(e));
        this.log.error("notify waker reconnect failed", {
          error: error.message,
        });
        this.scheduleReconnect(error);
      });
    }, this.reconnectDelayMs);
  }
}
