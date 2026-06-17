import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { KafkaPublisher } from "@eventferry/kafka";
import { brokers, uniqueName } from "./helpers.js";

/**
 * Exercises the new admin surface (Phase B1) against a real Redpanda broker.
 * Validates publisher.admin(), ensureTopics() — including the growPartitions
 * path that the unit tests fake — and validateTopicsOnConnect.
 */
describe("KafkaPublisher.admin against real Redpanda", () => {
  let publisher: KafkaPublisher;
  beforeAll(async () => {
    publisher = new KafkaPublisher({ brokers: brokers() });
    await publisher.connect();
  });
  afterAll(async () => {
    await publisher.disconnect();
  });

  it("admin().listTopics surfaces real cluster topics", async () => {
    const probe = uniqueName("admin-probe");
    const admin = await publisher.admin();
    try {
      await admin.createTopics([{ topic: probe, numPartitions: 1 }]);
      const all = await admin.listTopics();
      expect(all).toContain(probe);
    } finally {
      await admin.close();
    }
  });

  it("admin().describeTopics returns empty partitions for missing topics", async () => {
    const ghost = uniqueName("ghost");
    const admin = await publisher.admin();
    try {
      const [meta] = await admin.describeTopics([ghost]);
      expect(meta?.topic).toBe(ghost);
      expect(meta?.partitions).toEqual([]);
    } finally {
      await admin.close();
    }
  });

  it("ensureTopics is idempotent — second call does nothing observable", async () => {
    const t = uniqueName("idemp");
    await publisher.ensureTopics([{ topic: t, numPartitions: 3 }]);
    // Re-running with the same spec must NOT throw (TOPIC_ALREADY_EXISTS
    // is swallowed by the wrapper).
    await publisher.ensureTopics([{ topic: t, numPartitions: 3 }]);

    const admin = await publisher.admin();
    try {
      const [meta] = await admin.describeTopics([t]);
      expect(meta?.partitions.length).toBe(3);
    } finally {
      await admin.close();
    }
  });

  it("ensureTopics with growPartitions grows an existing topic", async () => {
    const t = uniqueName("grow");
    await publisher.ensureTopics([{ topic: t, numPartitions: 2 }]);
    await publisher.ensureTopics(
      [{ topic: t, numPartitions: 5 }],
      { growPartitions: true },
    );

    const admin = await publisher.admin();
    try {
      const [meta] = await admin.describeTopics([t]);
      expect(meta?.partitions.length).toBe(5);
    } finally {
      await admin.close();
    }
  });

  it("ensureTopics WITHOUT growPartitions leaves existing partition count alone", async () => {
    const t = uniqueName("nogrow");
    await publisher.ensureTopics([{ topic: t, numPartitions: 2 }]);
    await publisher.ensureTopics([{ topic: t, numPartitions: 5 }]); // no grow flag

    const admin = await publisher.admin();
    try {
      const [meta] = await admin.describeTopics([t]);
      expect(meta?.partitions.length).toBe(2);
    } finally {
      await admin.close();
    }
  });

  it("validateTopicsOnConnect: connect succeeds when topics exist", async () => {
    const t = uniqueName("validate-ok");
    await publisher.ensureTopics([{ topic: t, numPartitions: 1 }]);

    const pub = new KafkaPublisher({
      brokers: brokers(),
      validateTopicsOnConnect: [t],
    });
    await expect(pub.connect()).resolves.toBeUndefined();
    await pub.disconnect();
  });

  it("validateTopicsOnConnect: connect throws naming every missing topic", async () => {
    const a = uniqueName("missing-a");
    const b = uniqueName("missing-b");

    const pub = new KafkaPublisher({
      brokers: brokers(),
      validateTopicsOnConnect: [a, b],
    });
    await expect(pub.connect()).rejects.toThrow(
      new RegExp(`${a}.*${b}|${b}.*${a}`),
    );
    await pub.disconnect();
  });
});
