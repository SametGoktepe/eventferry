import { describe, expect, it } from "vitest";
import type {
  OutboxStore,
  PublishableMessage,
  PublishResult,
  Publisher,
} from "@eventferry/core";
import {
  MysqlBinlogRelay,
  type BinlogPosition,
  type BinlogStream,
  type BinlogStreamHandlers,
  type DecodedInsert,
  type MysqlBinlogRelayOptions,
} from "../src/binlog-relay.js";
import type { OutboxRow } from "../src/row.js";

const flush = (ms = 20) => new Promise((r) => setTimeout(r, ms));

function row(over: Partial<OutboxRow> = {}): OutboxRow {
  return {
    id: "1",
    message_id: "m1",
    aggregate_type: "order",
    aggregate_id: "a1",
    topic: "orders.created",
    key: null,
    payload: { ok: true },
    headers: {},
    trace_id: null,
    status: 0,
    attempts: 0,
    next_retry_at: null,
    created_at: new Date("2026-01-01"),
    processed_at: null,
    ...over,
  };
}

function pos(p: number, filename = "mysql-bin.000001"): BinlogPosition {
  return { filename, position: p };
}

class FakeStore implements OutboxStore {
  done: string[] = [];
  failed: { id: string; status: string }[] = [];
  async claimBatch(): Promise<never[]> {
    return []; // the internal retry loop finds nothing in these tests
  }
  async markDone(ids: string[]): Promise<void> {
    this.done.push(...ids);
  }
  async markFailed(
    id: string,
    _retryAt: Date | null,
    status: "failed" | "dead",
  ): Promise<void> {
    this.failed.push({ id, status });
  }
}

class FakePublisher implements Publisher {
  connects = 0;
  disconnects = 0;
  published: PublishableMessage[] = [];
  constructor(private readonly failIds = new Set<string>()) {}
  async connect(): Promise<void> {
    this.connects++;
  }
  async disconnect(): Promise<void> {
    this.disconnects++;
  }
  async publish(messages: PublishableMessage[]): Promise<PublishResult[]> {
    return messages.map((m) => {
      if (this.failIds.has(m.recordId)) {
        return { recordId: m.recordId, ok: false, error: new Error("boom") };
      }
      this.published.push(m);
      return { recordId: m.recordId, ok: true };
    });
  }
}

class FakeStream implements BinlogStream {
  handlers: BinlogStreamHandlers | null = null;
  acked: BinlogPosition[] = [];
  started = 0;
  stopped = 0;
  async start(handlers: BinlogStreamHandlers): Promise<void> {
    this.handlers = handlers;
    this.started++;
  }
  async acknowledge(position: BinlogPosition): Promise<void> {
    this.acked.push(position);
  }
  async stop(): Promise<void> {
    this.stopped++;
  }

  /** Drive a transaction: inserts followed by a commit. */
  async commit(rows: OutboxRow[], position: BinlogPosition): Promise<void> {
    if (!this.handlers) throw new Error("stream not started");
    for (const r of rows) {
      this.handlers.onInsert({ position, row: r });
    }
    await this.handlers.onCommit(position);
  }
}

class TestRelay extends MysqlBinlogRelay {
  public readonly fake: FakeStream;
  constructor(opts: MysqlBinlogRelayOptions, fake: FakeStream) {
    super(opts);
    this.fake = fake;
  }
  protected override createBinlogStream(): BinlogStream {
    return this.fake;
  }
}

function buildRelay(opts: {
  failIds?: string[];
  markPublished?: boolean;
} = {}) {
  const store = new FakeStore();
  const publisher = new FakePublisher(new Set(opts.failIds ?? []));
  const fake = new FakeStream();
  const relay = new TestRelay(
    {
      store,
      publisher,
      binlog: {
        host: "localhost",
        user: "u",
        password: "p",
        database: "shop",
      },
      markPublished: opts.markPublished,
      failedPollIntervalMs: 100_000,
    },
    fake,
  );
  return { relay, store, publisher, fake };
}

describe("MysqlBinlogRelay lifecycle", () => {
  it("connects the publisher, starts the stream, and stops cleanly", async () => {
    const { relay, publisher, fake } = buildRelay();
    await relay.start();
    expect(publisher.connects).toBe(1);
    expect(fake.started).toBe(1);
    await relay.stop();
    expect(fake.stopped).toBe(1);
    expect(publisher.disconnects).toBe(1);
  });

  it("is idempotent on repeated start/stop", async () => {
    const { relay, publisher, fake } = buildRelay();
    await relay.start();
    await relay.start(); // no-op
    expect(publisher.connects).toBe(1);
    expect(fake.started).toBe(1);
    await relay.stop();
    await relay.stop(); // no-op
    expect(fake.stopped).toBe(1);
  });
});

describe("MysqlBinlogRelay publishing", () => {
  it("publishes inserts on commit and acknowledges the position", async () => {
    const { relay, store, publisher, fake } = buildRelay();
    await relay.start();
    await fake.commit([row({ id: "10", message_id: "m10" })], pos(100));
    await flush();
    expect(publisher.published).toHaveLength(1);
    expect(publisher.published[0]?.recordId).toBe("10");
    expect(store.done).toEqual(["10"]);
    expect(fake.acked).toEqual([pos(100)]);
    await relay.stop();
  });

  it("publishes multiple rows from one commit in order", async () => {
    const { relay, publisher, fake } = buildRelay();
    await relay.start();
    await fake.commit(
      [
        row({ id: "1", message_id: "m1" }),
        row({ id: "2", message_id: "m2" }),
        row({ id: "3", message_id: "m3" }),
      ],
      pos(200),
    );
    await flush();
    expect(publisher.published.map((m) => m.recordId)).toEqual(["1", "2", "3"]);
    await relay.stop();
  });

  it("demotes failed publishes to failed(3) and does NOT acknowledge", async () => {
    const { relay, store, publisher, fake } = buildRelay({ failIds: ["99"] });
    await relay.start();
    await fake.commit(
      [
        row({ id: "98", message_id: "m98" }),
        row({ id: "99", message_id: "m99" }),
      ],
      pos(300),
    );
    await flush();
    expect(publisher.published.map((m) => m.recordId)).toEqual(["98"]);
    expect(store.failed).toEqual([{ id: "99", status: "failed" }]);
    expect(store.done).toEqual(["98"]); // the successful one is marked done
    // The position IS acknowledged — failure demotion is a successful
    // side-effect; we don't want to re-stream the same commit endlessly.
    expect(fake.acked).toEqual([pos(300)]);
    await relay.stop();
  });

  it("does not mark rows done when markPublished=false", async () => {
    const { relay, store, fake } = buildRelay({ markPublished: false });
    await relay.start();
    await fake.commit([row({ id: "55", message_id: "m55" })], pos(400));
    await flush();
    expect(store.done).toEqual([]);
    await relay.stop();
  });

  it("processes back-to-back commits in their original order", async () => {
    const { relay, publisher, fake } = buildRelay();
    await relay.start();
    await fake.commit([row({ id: "1", message_id: "m1" })], pos(100));
    await fake.commit([row({ id: "2", message_id: "m2" })], pos(200));
    await fake.commit([row({ id: "3", message_id: "m3" })], pos(300));
    await flush();
    expect(publisher.published.map((m) => m.recordId)).toEqual(["1", "2", "3"]);
    expect(fake.acked).toEqual([pos(100), pos(200), pos(300)]);
    await relay.stop();
  });
});
