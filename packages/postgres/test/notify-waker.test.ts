import { describe, expect, it } from "vitest";
import {
  PostgresNotifyWaker,
  type NotificationConnection,
} from "../src/notify-waker.js";
import { createNotifyTriggerSql } from "../src/migrations.js";

const flush = (ms = 30) => new Promise((r) => setTimeout(r, ms));

type Listener = (arg?: unknown) => void;

class FakeConn implements NotificationConnection {
  connected = false;
  ended = false;
  queries: string[] = [];
  private listeners: Record<string, Listener[]> = {};

  async connect(): Promise<void> {
    this.connected = true;
  }
  async query(sql: string): Promise<unknown> {
    this.queries.push(sql);
    return { rows: [] };
  }
  on(event: string, listener: Listener): this {
    (this.listeners[event] ??= []).push(listener);
    return this;
  }
  async end(): Promise<void> {
    this.ended = true;
  }
  emit(event: string, arg?: unknown): void {
    for (const l of this.listeners[event] ?? []) l(arg);
  }
}

function factory() {
  const conns: FakeConn[] = [];
  const connect = () => {
    const c = new FakeConn();
    conns.push(c);
    return c;
  };
  return { connect, conns };
}

describe("PostgresNotifyWaker", () => {
  it("connects, LISTENs the channel, and forwards notifications to onWake", async () => {
    const { connect, conns } = factory();
    const waker = new PostgresNotifyWaker({ connect, channel: "outbox" });

    let woken = 0;
    await waker.start(() => {
      woken++;
    });

    expect(conns).toHaveLength(1);
    expect(conns[0]?.connected).toBe(true);
    expect(conns[0]?.queries.some((q) => q.includes("LISTEN outbox"))).toBe(true);

    conns[0]?.emit("notification", { channel: "outbox" });
    conns[0]?.emit("notification", { channel: "outbox" });
    expect(woken).toBe(2);

    await waker.stop();
  });

  it("reconnects on a dropped connection and keeps waking", async () => {
    const { connect, conns } = factory();
    const waker = new PostgresNotifyWaker({
      connect,
      channel: "outbox",
      reconnectDelayMs: 1,
    });

    let woken = 0;
    await waker.start(() => {
      woken++;
    });

    conns[0]?.emit("error", new Error("connection reset"));
    await flush();

    expect(conns).toHaveLength(2); // factory called again
    expect(conns[1]?.connected).toBe(true);
    expect(conns[1]?.queries.some((q) => q.includes("LISTEN outbox"))).toBe(true);

    // The fresh connection's notifications still reach onWake.
    conns[1]?.emit("notification", {});
    expect(woken).toBe(1);

    await waker.stop();
  });

  it("UNLISTENs and ends the connection on stop, and stops reconnecting", async () => {
    const { connect, conns } = factory();
    const waker = new PostgresNotifyWaker({
      connect,
      channel: "outbox",
      reconnectDelayMs: 1,
    });
    await waker.start(() => {});

    await waker.stop();
    expect(conns[0]?.queries.some((q) => q.includes("UNLISTEN outbox"))).toBe(true);
    expect(conns[0]?.ended).toBe(true);

    // A drop after stop must NOT trigger a reconnect.
    conns[0]?.emit("end");
    await flush();
    expect(conns).toHaveLength(1);
  });

  it("rejects an unsafe channel name", () => {
    const { connect } = factory();
    expect(
      () => new PostgresNotifyWaker({ connect, channel: "ch; DROP TABLE x" }),
    ).toThrow();
  });
});

describe("createNotifyTriggerSql", () => {
  it("emits an AFTER INSERT statement trigger that pg_notifies the channel", () => {
    const sql = createNotifyTriggerSql("outbox", "outbox");
    expect(sql).toContain("pg_notify('outbox', '')");
    expect(sql).toContain("AFTER INSERT ON outbox");
    expect(sql).toContain("FOR EACH STATEMENT");
    expect(sql).toContain("outbox_notify");
    expect(sql).toContain("outbox_notify_trg");
  });

  it("embeds custom table and channel names", () => {
    const sql = createNotifyTriggerSql("orders_outbox", "orders_chan");
    expect(sql).toContain("ON orders_outbox");
    expect(sql).toContain("pg_notify('orders_chan', '')");
  });

  it("rejects unsafe identifiers", () => {
    expect(() => createNotifyTriggerSql("a; DROP")).toThrow();
    expect(() => createNotifyTriggerSql("outbox", "c'x")).toThrow();
  });
});
