import { describe, expect, it } from "vitest";
import { KafkaPublisher } from "../src/publisher.js";
import type { KafkaDriver } from "../src/driver.js";
import type { PublishableMessage, PublishResult } from "@eventferry/core";

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

class FakeDriver implements KafkaDriver {
  connected = false;
  sent: PublishableMessage[][] = [];
  readonly transactional: boolean;

  constructor(
    transactional = false,
    private readonly failAll = false,
  ) {
    this.transactional = transactional;
  }
  async connect() {
    this.connected = true;
  }
  async disconnect() {
    this.connected = false;
  }
  async sendBatch(messages: PublishableMessage[]): Promise<PublishResult[]> {
    this.sent.push(messages);
    return messages.map((m) => ({
      recordId: m.recordId,
      ok: !this.failAll,
      error: this.failAll ? new Error("send failed") : undefined,
    }));
  }
}

describe("KafkaPublisher", () => {
  it("delegates connect/disconnect to the driver", async () => {
    const driver = new FakeDriver();
    const pub = new KafkaPublisher({ brokers: ["x"], customDriver: driver });
    await pub.connect();
    expect(driver.connected).toBe(true);
    await pub.disconnect();
    expect(driver.connected).toBe(false);
  });

  it("publishes through the driver", async () => {
    const driver = new FakeDriver();
    const pub = new KafkaPublisher({ brokers: ["x"], customDriver: driver });
    const results = await pub.publish([msg(), msg({ recordId: "2" })]);
    expect(results.every((r) => r.ok)).toBe(true);
    expect(driver.sent[0]).toHaveLength(2);
  });

  it("exposes transactional capability of driver", () => {
    const pub = new KafkaPublisher({
      brokers: ["x"],
      customDriver: new FakeDriver(true),
    });
    expect(pub.transactional).toBe(true);
  });

  it("publishToDlq adds failure headers", async () => {
    const driver = new FakeDriver();
    const pub = new KafkaPublisher({ brokers: ["x"], customDriver: driver });
    await pub.publishToDlq(msg({ topic: "orders.dlq" }), new Error("kaboom"));
    const sent = driver.sent[0]?.[0];
    expect(sent?.topic).toBe("orders.dlq");
    expect(sent?.headers["dlq-reason"]).toBe("kaboom");
    expect(sent?.headers["dlq-failed-at"]).toBeDefined();
  });

  it("publishToDlq throws if the send fails", async () => {
    const driver = new FakeDriver(false, true);
    const pub = new KafkaPublisher({ brokers: ["x"], customDriver: driver });
    await expect(
      pub.publishToDlq(msg({ topic: "orders.dlq" }), new Error("x")),
    ).rejects.toThrow();
  });
});

describe("driver selection", () => {
  it("throws when transactional kafkajs driver lacks transactionalId", () => {
    expect(
      () =>
        new KafkaPublisher({
          brokers: ["x"],
          driver: "kafkajs",
          transactional: true,
        }),
    ).toThrow(/transactionalId/);
  });

  it("throws when transactional confluent driver lacks transactionalId", () => {
    expect(
      () =>
        new KafkaPublisher({
          brokers: ["x"],
          driver: "confluent",
          transactional: true,
        }),
    ).toThrow(/transactionalId/);
  });
});
