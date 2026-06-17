import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { PublishableMessage } from "@eventferry/core";
import { KafkaPublisher } from "@eventferry/kafka";
import { Kafka } from "kafkajs";
import { brokers, createTopic, uniqueName } from "./helpers.js";

function msg(
  topic: string,
  over: Partial<PublishableMessage> = {},
): PublishableMessage {
  return {
    topic,
    key: "agg-1",
    value: Buffer.from(JSON.stringify({ ok: true }), "utf8"),
    headers: {},
    recordId: "1",
    messageId: "m1",
    ...over,
  };
}

/**
 * Collect messages and surface the broker-assigned partition per record.
 * Different from helpers.collectMessages() (which discards partition info).
 */
async function collectWithPartition(
  topic: string,
  count: number,
  timeoutMs = 20_000,
): Promise<Array<{ partition: number; key: string | null }>> {
  const kafka = new Kafka({ clientId: uniqueName("part"), brokers: brokers() });
  const consumer = kafka.consumer({ groupId: uniqueName("partg") });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  const out: Array<{ partition: number; key: string | null }> = [];
  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () =>
          reject(new Error(`timed out waiting for ${count} msg(s) on ${topic}`)),
        timeoutMs,
      );
      void consumer.run({
        eachMessage: async ({ message, partition }) => {
          out.push({
            partition,
            key: message.key ? message.key.toString() : null,
          });
          if (out.length >= count) {
            clearTimeout(timer);
            resolve();
          }
        },
      });
    });
  } finally {
    await consumer.disconnect();
  }
  return out;
}

describe("Power-user escape hatches against real Redpanda", () => {
  describe("customPartitioner (Phase B4)", () => {
    let publisher: KafkaPublisher;
    beforeAll(async () => {
      // Always-route-to-partition-0 partitioner. Real partitioners hash the
      // key; we want a deterministic, observable choice for the assertion.
      const pin = () => () => 0;
      publisher = new KafkaPublisher({
        brokers: brokers(),
        driver: "kafkajs",
        customPartitioner: pin,
      });
      await publisher.connect();
    });
    afterAll(async () => {
      await publisher.disconnect();
    });

    it("routes every record to the partition the custom factory picks", async () => {
      const topic = uniqueName("pin");
      await createTopic(topic, 4); // 4 partitions; default hash would spread.

      const batch = Array.from({ length: 8 }, (_, i) =>
        msg(topic, { recordId: String(i), key: `agg-${i}` }),
      );
      const results = await publisher.publish(batch);
      expect(results.every((r) => r.ok)).toBe(true);

      const got = await collectWithPartition(topic, 8);
      expect(got).toHaveLength(8);
      // Every single record landed on partition 0 — the custom partitioner
      // won against the default key-hash routing.
      const partitions = got.map((g) => g.partition);
      expect(
        partitions,
        `expected all on partition 0, got ${JSON.stringify(partitions)}`,
      ).toEqual(new Array(8).fill(0));
    });
  });

  describe("rawKafkaJsProducerConfig (Phase B4)", () => {
    it("publishes successfully when raw kafkajs producer config is merged in", async () => {
      // The raw block sets retry config kafkajs honors internally. We can't
      // observe `retry` on the wire, but we CAN observe that the producer
      // builds and publishes cleanly — guards against a precedence regression
      // that would drop the underlying createPartitioner / idempotent values.
      const pub = new KafkaPublisher({
        brokers: brokers(),
        driver: "kafkajs",
        rawKafkaJsProducerConfig: {
          retry: { retries: 7, initialRetryTime: 100 },
          metadataMaxAge: 5_000,
        },
      });
      await pub.connect();
      const topic = uniqueName("raw");
      await createTopic(topic);
      const results = await pub.publish([msg(topic)]);
      expect(results.every((r) => r.ok)).toBe(true);
      await pub.disconnect();
    });
  });
});
