import { describe, expect, it } from "vitest";
import { KafkaJsDriver } from "../src/kafkajs-driver.js";
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

/** Records every send/transaction call so we can assert on the args. */
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
  async sendBatch(args: any) {
    if (this.failOnSend) throw new Error("send failed");
    this.sends.push(args);
  }
  async transaction() {
    const self = this;
    return {
      async sendBatch(args: any) {
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

/** Subclass that injects the fake producer via the protected test seam. */
class TestKjsDriver extends KafkaJsDriver {
  readonly fake: FakeProducer;
  constructor(opts: ConstructorParameters<typeof KafkaJsDriver>[0], fake = new FakeProducer()) {
    super(opts);
    this.fake = fake;
  }
  protected override async createProducer() {
    return this.fake as any;
  }
}

describe("KafkaJsDriver", () => {
  it("connects and disconnects the producer", async () => {
    const d = new TestKjsDriver({ brokers: ["x"] });
    await d.connect();
    expect(d.fake.connected).toBe(true);
    await d.disconnect();
    expect(d.fake.connected).toBe(false);
  });

  it("throws if sendBatch is called before connect", async () => {
    const d = new TestKjsDriver({ brokers: ["x"] });
    await expect(d.sendBatch([msg()])).rejects.toThrow(/not connected/);
  });

  it("groups by topic and defaults acks to -1", async () => {
    const d = new TestKjsDriver({ brokers: ["x"] });
    await d.connect();
    const results = await d.sendBatch([
      msg({ recordId: "1", topic: "a" }),
      msg({ recordId: "2", topic: "a" }),
      msg({ recordId: "3", topic: "b" }),
    ]);

    expect(results.every((r) => r.ok)).toBe(true);
    expect(d.fake.sends).toHaveLength(1);
    const arg = d.fake.sends[0];
    expect(arg.acks).toBe(-1);
    expect(arg.topicMessages).toHaveLength(2); // topic a + topic b
    const topicA = arg.topicMessages.find((t: any) => t.topic === "a");
    expect(topicA.messages).toHaveLength(2);
  });

  it("passes through acks and compression", async () => {
    const d = new TestKjsDriver({
      brokers: ["x"],
      acks: 1,
      compression: "gzip",
    });
    await d.connect();
    await d.sendBatch([msg()]);
    const arg = d.fake.sends[0];
    expect(arg.acks).toBe(1);
    expect(arg.topicMessages[0].compression).toBe("gzip");
  });

  it('omits compression when set to "none"', async () => {
    const d = new TestKjsDriver({ brokers: ["x"], compression: "none" });
    await d.connect();
    await d.sendBatch([msg()]);
    expect(d.fake.sends[0].topicMessages[0].compression).toBeUndefined();
  });

  it("returns failure results when the send throws", async () => {
    const d = new TestKjsDriver({ brokers: ["x"] }, new FakeProducer(true));
    await d.connect();
    const results = await d.sendBatch([msg({ recordId: "9" })]);
    expect(results[0]?.ok).toBe(false);
    expect(results[0]?.error?.message).toBe("send failed");
  });

  describe("transactional", () => {
    it("requires a transactionalId", () => {
      expect(
        () => new TestKjsDriver({ brokers: ["x"], transactional: true }),
      ).toThrow(/transactionalId/);
    });

    it("commits the transaction on success", async () => {
      const d = new TestKjsDriver({
        brokers: ["x"],
        transactional: true,
        transactionalId: "tx-1",
      });
      await d.connect();
      const results = await d.sendBatch([msg()]);
      expect(results[0]?.ok).toBe(true);
      expect(d.fake.committed).toBe(1);
      expect(d.fake.aborted).toBe(0);
    });

    it("aborts the transaction and reports failure on error", async () => {
      const d = new TestKjsDriver(
        { brokers: ["x"], transactional: true, transactionalId: "tx-1" },
        new FakeProducer(true),
      );
      await d.connect();
      const results = await d.sendBatch([msg()]);
      expect(results[0]?.ok).toBe(false);
      expect(d.fake.aborted).toBe(1);
      expect(d.fake.committed).toBe(0);
    });
  });
});
