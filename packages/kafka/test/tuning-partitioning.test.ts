import { afterEach, describe, expect, it, vi } from "vitest";
import type { PublishableMessage } from "@eventferry/core";
import {
  KafkaJsDriver,
  _resetKafkajsWarnDedup,
} from "../src/kafkajs-driver.js";

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

class FakeProducer {
  sends: unknown[] = [];
  txnSends: unknown[] = [];
  async connect(): Promise<void> {}
  async disconnect(): Promise<void> {}
  async sendBatch(args: unknown): Promise<void> {
    this.sends.push(args);
  }
  async transaction(): Promise<{
    sendBatch(args: unknown): Promise<void>;
    commit(): Promise<void>;
    abort(): Promise<void>;
  }> {
    const self = this;
    return {
      async sendBatch(args: unknown) {
        self.txnSends.push(args);
      },
      async commit() {},
      async abort() {},
    };
  }
}

class TestKjsDriver extends KafkaJsDriver {
  readonly fake = new FakeProducer();
  readonly producerArgs: unknown[] = [];
  protected override async createProducer(): Promise<FakeProducer> {
    // Capture what the publisher passes to kafkajs's `producer({...})` so
    // tests can assert on tuning + partitioner mapping without a real broker.
    this.producerArgs.push({ via: "test-seam" });
    return this.fake;
  }
}

describe("KafkaJsDriver — warn-on-unsupported tuning", () => {
  afterEach(() => {
    _resetKafkajsWarnDedup();
    vi.restoreAllMocks();
  });

  it("warns once when lingerMs is set (kafkajs has no equivalent)", () => {
    const spy = vi.spyOn(console, "warn").mockImplementation(() => {});
    new KafkaJsDriver({ brokers: ["b:9092"], lingerMs: 25 });
    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.calls[0]?.[0]).toMatch(/lingerMs/);
    expect(spy.mock.calls[0]?.[0]).toMatch(/confluent/);
  });

  it("warns separately for each unsupported option (one line each)", () => {
    const spy = vi.spyOn(console, "warn").mockImplementation(() => {});
    new KafkaJsDriver({
      brokers: ["b:9092"],
      lingerMs: 10,
      batchSize: 65_536,
      deliveryTimeoutMs: 120_000,
      maxRequestSize: 2_000_000,
    });
    expect(spy).toHaveBeenCalledTimes(4);
    const messages = spy.mock.calls.map((c) => c[0] as string).join("\n");
    expect(messages).toMatch(/lingerMs/);
    expect(messages).toMatch(/batchSize/);
    expect(messages).toMatch(/deliveryTimeoutMs/);
    expect(messages).toMatch(/maxRequestSize/);
  });

  it("dedupes warnings across multiple driver instances in the same process", () => {
    const spy = vi.spyOn(console, "warn").mockImplementation(() => {});
    new KafkaJsDriver({ brokers: ["b:9092"], lingerMs: 10 });
    new KafkaJsDriver({ brokers: ["b:9092"], lingerMs: 25 });
    new KafkaJsDriver({ brokers: ["b:9092"], lingerMs: 50 });
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("does not warn when only supported tuning fields are set", () => {
    const spy = vi.spyOn(console, "warn").mockImplementation(() => {});
    new KafkaJsDriver({
      brokers: ["b:9092"],
      maxInFlightRequests: 5,
      requestTimeoutMs: 30_000,
      transactionTimeoutMs: 90_000,
    });
    expect(spy).not.toHaveBeenCalled();
  });
});

describe("KafkaJsDriver — per-message partition override", () => {
  it("forwards partition into the kafkajs sendBatch payload", async () => {
    const driver = new TestKjsDriver({ brokers: ["b:9092"] });
    await driver.connect();
    await driver.sendBatch([
      msg({ recordId: "a", partition: 7 }),
      msg({ recordId: "b" }), // no override
    ]);
    const sent = driver.fake.sends[0] as {
      topicMessages: Array<{ topic: string; messages: unknown[] }>;
    };
    const onlyTopic = sent.topicMessages[0]!.messages as Array<{
      partition?: number;
    }>;
    expect(onlyTopic[0]?.partition).toBe(7);
    expect(onlyTopic[1]?.partition).toBeUndefined();
  });
});
