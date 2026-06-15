import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { PublishableMessage } from "@eventferry/core";
import { KafkaPublisher } from "@eventferry/kafka";
import { brokers, collectMessages, createTopic, uniqueName } from "./helpers.js";

function msg(topic: string, over: Partial<PublishableMessage> = {}): PublishableMessage {
  return {
    topic,
    key: "agg-1",
    value: Buffer.from(JSON.stringify({ hello: "world" }), "utf8"),
    headers: { "aggregate-id": "agg-1" },
    recordId: "1",
    messageId: "m1",
    ...over,
  };
}

describe("KafkaPublisher against real Redpanda", () => {
  let publisher: KafkaPublisher;
  beforeAll(async () => {
    publisher = new KafkaPublisher({ brokers: brokers(), idempotent: true });
    await publisher.connect();
  });
  afterAll(async () => {
    await publisher.disconnect();
  });

  it("publishes a batch that a consumer reads back", async () => {
    const topic = uniqueName("pub");
    await createTopic(topic);
    const results = await publisher.publish([msg(topic)]);
    expect(results.every((r) => r.ok)).toBe(true);

    const got = await collectMessages(topic, 1);
    expect(JSON.parse(got[0]!.value.toString("utf8"))).toEqual({ hello: "world" });
    expect(got[0]!.key).toBe("agg-1");
    expect(got[0]!.headers["aggregate-id"]).toBe("agg-1");
  });

  it("routes a dead-lettered message to the DLQ topic", async () => {
    const dlq = uniqueName("dlq");
    await createTopic(dlq);
    await publisher.publishToDlq(msg(dlq), new Error("kaboom"));

    const got = await collectMessages(dlq, 1);
    expect(got[0]!.headers["dlq-reason"]).toBe("kaboom");
  });
});
