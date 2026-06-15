import { describe, expect, it } from "vitest";
import type {
  OutboxStore,
  PublishableMessage,
  PublishResult,
  Publisher,
} from "@eventferry/core";
import {
  PostgresStreamingRelay,
  type ReplicationStream,
  type ReplicationStreamHandlers,
} from "../src/streaming-relay.js";
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
    created_at: new Date("2025-01-01"),
    processed_at: null,
    ...over,
  };
}

class FakeStore implements OutboxStore {
  done: string[] = [];
  failed: { id: string; status: string }[] = [];
  async claimBatch(): Promise<never[]> {
    return []; // internal retry loop finds no failed rows in these tests
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

class FakeStream implements ReplicationStream {
  handlers: ReplicationStreamHandlers | null = null;
  acked: string[] = [];
  started = 0;
  stopped = 0;
  async start(handlers: ReplicationStreamHandlers): Promise<void> {
    this.handlers = handlers;
    this.started++;
  }
  async acknowledge(lsn: string): Promise<void> {
    this.acked.push(lsn);
  }
  async stop(): Promise<void> {
    this.stopped++;
  }
  insert(r: OutboxRow, lsn = "0/1"): void {
    this.handlers?.onInsert({ lsn, row: r });
  }
  commit(lsn = "0/2"): void | Promise<void> {
    return this.handlers?.onCommit(lsn);
  }
  fail(err: Error): void {
    this.handlers?.onError(err);
  }
}

class TestStreamingRelay extends PostgresStreamingRelay {
  readonly fakeStream = new FakeStream();
  protected override createReplicationStream(): ReplicationStream {
    return this.fakeStream;
  }
}

function makeRelay(opts: {
  store?: FakeStore;
  publisher?: FakePublisher;
  markPublished?: boolean;
  onError?: (e: Error) => void;
}) {
  const store = opts.store ?? new FakeStore();
  const publisher = opts.publisher ?? new FakePublisher();
  const relay = new TestStreamingRelay({
    store,
    publisher,
    replication: {
      connectionString: "postgres://x",
      slot: "outbox_slot",
      publication: "outbox_pub",
    },
    markPublished: opts.markPublished,
    failedPollIntervalMs: 60_000,
    hooks: opts.onError ? { onError: opts.onError } : undefined,
  });
  return { relay, store, publisher };
}

describe("PostgresStreamingRelay", () => {
  it("publishes a committed batch, marks done, and acknowledges the LSN", async () => {
    const { relay, store, publisher } = makeRelay({});
    await relay.start();
    expect(publisher.connects).toBe(1);

    relay.fakeStream.insert(row({ id: "1" }));
    relay.fakeStream.insert(row({ id: "2", aggregate_id: "a2" }));
    await relay.fakeStream.commit("0/AB");
    await flush();

    expect(publisher.published.map((m) => m.recordId).sort()).toEqual(["1", "2"]);
    expect(store.done.sort()).toEqual(["1", "2"]);
    expect(relay.fakeStream.acked).toContain("0/AB");

    await relay.stop();
    expect(publisher.disconnects).toBe(1);
    expect(relay.fakeStream.stopped).toBe(1);
  });

  it("demotes a failed publish to status=failed and still advances the LSN", async () => {
    const { relay, store, publisher } = makeRelay({
      publisher: new FakePublisher(new Set(["2"])),
    });
    await relay.start();

    relay.fakeStream.insert(row({ id: "1" }));
    relay.fakeStream.insert(row({ id: "2" }));
    await relay.fakeStream.commit("0/CD");
    await flush();

    expect(store.done).toEqual(["1"]); // only the ok row
    expect(store.failed).toEqual([{ id: "2", status: "failed" }]); // handed to retry loop
    expect(relay.fakeStream.acked).toContain("0/CD"); // stream never stalls

    await relay.stop();
  });

  it("skips markDone when markPublished is false", async () => {
    const { relay, store } = makeRelay({ markPublished: false });
    await relay.start();

    relay.fakeStream.insert(row({ id: "1" }));
    await relay.fakeStream.commit("0/EF");
    await flush();

    expect(store.done).toEqual([]);
    expect(relay.fakeStream.acked).toContain("0/EF");

    await relay.stop();
  });

  it("surfaces stream errors via hooks.onError", async () => {
    const errors: Error[] = [];
    const { relay } = makeRelay({ onError: (e) => errors.push(e) });
    await relay.start();

    relay.fakeStream.fail(new Error("replication dropped"));
    await flush();

    expect(errors.map((e) => e.message)).toContain("replication dropped");
    await relay.stop();
  });

  it("builds messages with the partition key and correlation headers", async () => {
    const { relay, publisher } = makeRelay({});
    await relay.start();

    relay.fakeStream.insert(
      row({ id: "9", aggregate_id: "agg-9", trace_id: "tr-9", key: null }),
    );
    await relay.fakeStream.commit();
    await flush();

    const msg = publisher.published[0];
    expect(msg?.key).toBe("agg-9");
    expect(msg?.headers["aggregate-id"]).toBe("agg-9");
    expect(msg?.headers["trace-id"]).toBe("tr-9");
    expect(msg?.headers["content-type"]).toBe("application/json");

    await relay.stop();
  });
});
