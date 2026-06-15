import { describe, expect, it } from "vitest";
import { Relay } from "../src/relay.js";
import { NoopLogger } from "../src/serializer.js";
import type {
  OutboxRecord,
  OutboxStore,
  PublishResult,
  PublishableMessage,
  Publisher,
  Waker,
} from "../src/types.js";

function makeRecord(over: Partial<OutboxRecord> = {}): OutboxRecord {
  return {
    id: "1",
    messageId: "m1",
    topic: "t",
    aggregateType: "order",
    aggregateId: "a1",
    key: null,
    payload: { hello: "world" },
    headers: {},
    traceId: null,
    status: "pending",
    attempts: 0,
    nextRetryAt: null,
    createdAt: new Date(),
    processedAt: null,
    ...over,
  };
}

class FakeStore implements OutboxStore {
  done: string[] = [];
  failed: { id: string; retryAt: Date | null; status: string }[] = [];
  private queue: OutboxRecord[];

  constructor(records: OutboxRecord[]) {
    this.queue = [...records];
  }

  /** Add records after construction (to test wake-driven claiming). */
  add(...records: OutboxRecord[]): void {
    this.queue.push(...records);
  }
  async claimBatch(n: number): Promise<OutboxRecord[]> {
    return this.queue.splice(0, n);
  }
  async markDone(ids: string[]): Promise<void> {
    this.done.push(...ids);
  }
  async markFailed(
    id: string,
    retryAt: Date | null,
    status: "failed" | "dead",
  ): Promise<void> {
    this.failed.push({ id, retryAt, status });
  }
}

class FakePublisher implements Publisher {
  published: PublishableMessage[] = [];
  dlq: PublishableMessage[] = [];
  connects = 0;
  disconnects = 0;
  constructor(private readonly failIds: Set<string> = new Set()) {}

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
  async publishToDlq(message: PublishableMessage): Promise<void> {
    this.dlq.push(message);
  }
}

describe("Relay.tick", () => {
  it("publishes a claimed batch and marks done", async () => {
    const store = new FakeStore([makeRecord({ id: "1" }), makeRecord({ id: "2" })]);
    const pub = new FakePublisher();
    const relay = new Relay({ store, publisher: pub, logger: new NoopLogger() });

    const processed = await relay.tick();

    expect(processed).toBe(2);
    expect(pub.published).toHaveLength(2);
    expect(store.done.sort()).toEqual(["1", "2"]);
  });

  it("uses aggregateId as default partition key", async () => {
    const store = new FakeStore([makeRecord({ id: "1", aggregateId: "agg-9" })]);
    const pub = new FakePublisher();
    const relay = new Relay({ store, publisher: pub, logger: new NoopLogger() });

    await relay.tick();

    expect(pub.published[0]?.key).toBe("agg-9");
  });

  it("schedules retry on failure while attempts remain", async () => {
    const store = new FakeStore([makeRecord({ id: "1", attempts: 0 })]);
    const pub = new FakePublisher(new Set(["1"]));
    const relay = new Relay({
      store,
      publisher: pub,
      logger: new NoopLogger(),
      retry: { maxAttempts: 3, baseMs: 10, maxMs: 100, strategy: "fixed" },
    });

    await relay.tick();

    expect(store.done).toHaveLength(0);
    expect(store.failed).toHaveLength(1);
    expect(store.failed[0]?.status).toBe("failed");
    expect(store.failed[0]?.retryAt).toBeInstanceOf(Date);
  });

  it("routes to DLQ and marks dead when attempts exhausted", async () => {
    const store = new FakeStore([makeRecord({ id: "1", attempts: 4 })]);
    const pub = new FakePublisher(new Set(["1"]));
    const relay = new Relay({
      store,
      publisher: pub,
      logger: new NoopLogger(),
      retry: { maxAttempts: 5, baseMs: 10, maxMs: 100, strategy: "fixed" },
      dlq: { topic: "t.dlq" },
    });

    await relay.tick();

    expect(store.failed[0]?.status).toBe("dead");
    expect(store.failed[0]?.retryAt).toBeNull();
    expect(pub.dlq).toHaveLength(1);
    expect(pub.dlq[0]?.topic).toBe("t.dlq");
    // The original destination is preserved as a header for triage.
    expect(pub.dlq[0]?.headers["original-topic"]).toBe("t");
  });

  it("handles partial batch failure", async () => {
    const store = new FakeStore([
      makeRecord({ id: "1" }),
      makeRecord({ id: "2" }),
      makeRecord({ id: "3" }),
    ]);
    const pub = new FakePublisher(new Set(["2"]));
    const relay = new Relay({ store, publisher: pub, logger: new NoopLogger() });

    await relay.tick();

    expect(store.done.sort()).toEqual(["1", "3"]);
    expect(store.failed).toHaveLength(1);
    expect(store.failed[0]?.id).toBe("2");
  });

  it("returns 0 when nothing to claim", async () => {
    const store = new FakeStore([]);
    const pub = new FakePublisher();
    const relay = new Relay({ store, publisher: pub, logger: new NoopLogger() });
    expect(await relay.tick()).toBe(0);
  });
});

describe("Relay lifecycle", () => {
  const flush = () => new Promise((r) => setTimeout(r, 30));

  it("connects, drains the backlog, then disconnects on stop", async () => {
    const store = new FakeStore([makeRecord({ id: "1" }), makeRecord({ id: "2" })]);
    const pub = new FakePublisher();
    const relay = new Relay({
      store,
      publisher: pub,
      logger: new NoopLogger(),
      pollIntervalMs: 5,
    });

    await relay.start();
    expect(pub.connects).toBe(1);
    await flush();
    await relay.stop();

    expect(store.done.sort()).toEqual(["1", "2"]);
    expect(pub.published).toHaveLength(2);
    expect(pub.disconnects).toBe(1);
  });

  it("start is idempotent and stop on an unstarted relay is a no-op", async () => {
    const store = new FakeStore([]);
    const pub = new FakePublisher();
    const relay = new Relay({
      store,
      publisher: pub,
      logger: new NoopLogger(),
      pollIntervalMs: 5,
    });

    await relay.stop(); // not running yet
    expect(pub.disconnects).toBe(0);

    await relay.start();
    await relay.start(); // second call ignored
    expect(pub.connects).toBe(1);

    await relay.stop();
    expect(pub.disconnects).toBe(1);
  });

  it("survives a loop error, reports it via onError, and keeps draining", async () => {
    let calls = 0;
    const good = makeRecord({ id: "1" });
    const errors: Error[] = [];
    const store: OutboxStore = {
      async claimBatch() {
        calls++;
        if (calls === 1) throw new Error("transient db blip");
        if (calls === 2) return [good];
        return [];
      },
      async markDone() {},
      async markFailed() {},
    };
    const pub = new FakePublisher();
    const relay = new Relay({
      store,
      publisher: pub,
      logger: new NoopLogger(),
      pollIntervalMs: 5,
      hooks: { onError: (e) => errors.push(e) },
    });

    await relay.start();
    await flush();
    await relay.stop();

    expect(errors.map((e) => e.message)).toContain("transient db blip");
    expect(pub.published.map((m) => m.recordId)).toContain("1");
  });
});

class FakeWaker implements Waker {
  onWake?: () => void;
  starts = 0;
  stops = 0;
  async start(onWake: () => void): Promise<void> {
    this.onWake = onWake;
    this.starts++;
  }
  async stop(): Promise<void> {
    this.stops++;
  }
  fire(): void {
    this.onWake?.();
  }
}

describe("Relay waker", () => {
  const flush = () => new Promise((r) => setTimeout(r, 30));

  it("claims promptly on a wake signal instead of waiting for the poll", async () => {
    const store = new FakeStore([]); // starts empty -> relay goes idle
    const pub = new FakePublisher();
    const waker = new FakeWaker();
    const relay = new Relay({
      store,
      publisher: pub,
      logger: new NoopLogger(),
      pollIntervalMs: 60_000, // would never fire within the test on its own
      waker,
    });

    await relay.start();
    expect(waker.starts).toBe(1);
    await flush(); // let it drain the initial empty claim and park on the wait

    // New work arrives; a wake should pull it through well before pollIntervalMs.
    store.add(makeRecord({ id: "w1" }));
    waker.fire();
    await flush();

    expect(store.done).toEqual(["w1"]);
    expect(pub.published.map((m) => m.recordId)).toEqual(["w1"]);

    await relay.stop();
    expect(waker.stops).toBe(1);
  });

  it("stops promptly even with a huge poll interval (wait is interruptible)", async () => {
    const store = new FakeStore([]);
    const pub = new FakePublisher();
    const waker = new FakeWaker();
    const relay = new Relay({
      store,
      publisher: pub,
      logger: new NoopLogger(),
      pollIntervalMs: 60_000,
      waker,
    });

    await relay.start();
    await flush();
    // If stop() did not interrupt the 60s wait, this would hang the test.
    await relay.stop();
    expect(waker.stops).toBe(1);
  });

  it("does not lose a wake that arrives during processing", async () => {
    const store = new FakeStore([makeRecord({ id: "a" })]);
    const pub = new FakePublisher();
    const waker = new FakeWaker();
    const relay = new Relay({
      store,
      publisher: pub,
      logger: new NoopLogger(),
      pollIntervalMs: 60_000,
      waker,
    });

    await relay.start();
    // Queue more work and fire immediately; the signal must not be dropped even
    // if it races the in-flight cycle.
    store.add(makeRecord({ id: "b" }));
    waker.fire();
    await flush();

    expect(store.done.sort()).toEqual(["a", "b"]);
    await relay.stop();
  });
});
