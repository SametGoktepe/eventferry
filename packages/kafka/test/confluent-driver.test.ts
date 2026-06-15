import { describe, expect, it } from "vitest";
import { ConfluentDriver } from "../src/confluent-driver.js";
import type { PublishableMessage } from "@eventferry/core";

function msg(over: Partial<PublishableMessage> = {}): PublishableMessage {
  return {
    topic: "orders",
    key: "agg-1",
    value: Buffer.from("{}"),
    headers: {},
    recordId: "1",
    messageId: "m1",
    ...over,
  };
}

/** confluent's KafkaJS-compatible API sends one call per topic. */
class FakeProducer {
  connected = false;
  sends: any[] = [];
  txnSends: any[] = [];
  committed = 0;
  aborted = 0;
  constructor(private readonly failOnSend = false) {}

  async connect() {
    this.connected = true;
  }
  async disconnect() {
    this.connected = false;
  }
  async send(args: any) {
    if (this.failOnSend) throw new Error("send failed");
    this.sends.push(args);
  }
  async transaction() {
    const self = this;
    return {
      async send(args: any) {
        if (self.failOnSend) throw new Error("txn send failed");
        self.txnSends.push(args);
      },
      async commit() {
        self.committed++;
      },
      async abort() {
        self.aborted++;
      },
    };
  }
}

class TestConfluentDriver extends ConfluentDriver {
  readonly fake: FakeProducer;
  constructor(
    opts: ConstructorParameters<typeof ConfluentDriver>[0],
    fake = new FakeProducer(),
  ) {
    super(opts);
    this.fake = fake;
  }
  protected override async createProducer() {
    return this.fake as any;
  }
}

describe("ConfluentDriver", () => {
  it("throws if sendBatch is called before connect", async () => {
    const d = new TestConfluentDriver({ brokers: ["x"] });
    await expect(d.sendBatch([msg()])).rejects.toThrow(/not connected/);
  });

  it("sends once per topic and defaults acks to -1", async () => {
    const d = new TestConfluentDriver({ brokers: ["x"] });
    await d.connect();
    const results = await d.sendBatch([
      msg({ recordId: "1", topic: "a" }),
      msg({ recordId: "2", topic: "b" }),
    ]);
    expect(results.every((r) => r.ok)).toBe(true);
    expect(d.fake.sends).toHaveLength(2);
    expect(d.fake.sends.every((s) => s.acks === -1)).toBe(true);
  });

  it("wires through acks and compression (the v0.1 gap)", async () => {
    const d = new TestConfluentDriver({
      brokers: ["x"],
      acks: 1,
      compression: "snappy",
    });
    await d.connect();
    await d.sendBatch([msg()]);
    expect(d.fake.sends[0].acks).toBe(1);
    expect(d.fake.sends[0].compression).toBe("snappy");
  });

  it('omits compression when set to "none"', async () => {
    const d = new TestConfluentDriver({ brokers: ["x"], compression: "none" });
    await d.connect();
    await d.sendBatch([msg()]);
    expect(d.fake.sends[0].compression).toBeUndefined();
  });

  it("reports failure when a send throws", async () => {
    const d = new TestConfluentDriver({ brokers: ["x"] }, new FakeProducer(true));
    await d.connect();
    const results = await d.sendBatch([msg()]);
    expect(results[0]?.ok).toBe(false);
  });

  describe("transactional", () => {
    it("requires a transactionalId", () => {
      expect(
        () => new TestConfluentDriver({ brokers: ["x"], transactional: true }),
      ).toThrow(/transactionalId/);
    });

    it("commits on success and aborts on failure", async () => {
      const ok = new TestConfluentDriver({
        brokers: ["x"],
        transactional: true,
        transactionalId: "tx-1",
      });
      await ok.connect();
      await ok.sendBatch([msg()]);
      expect(ok.fake.committed).toBe(1);

      const bad = new TestConfluentDriver(
        { brokers: ["x"], transactional: true, transactionalId: "tx-1" },
        new FakeProducer(true),
      );
      await bad.connect();
      const results = await bad.sendBatch([msg()]);
      expect(results[0]?.ok).toBe(false);
      expect(bad.fake.aborted).toBe(1);
    });
  });
});
