import { describe, expect, it, vi } from "vitest";
import type { PublishableMessage, PublishResult } from "@eventferry/core";
import { KafkaPublisher } from "../src/publisher.js";
import type { KafkaDriver } from "../src/driver.js";
import type {
  KafkaDriverAdmin,
  PartitionGrowSpec,
  TopicCreateSpec,
  TopicMetadata,
} from "../src/admin.js";

class FakeAdmin implements KafkaDriverAdmin {
  connectCalls = 0;
  closeCalls = 0;
  createTopicsCalls: TopicCreateSpec[][] = [];
  createPartitionsCalls: PartitionGrowSpec[][] = [];
  // Fake the cluster's existing state. Each call returns these topics from
  // listTopics; describeTopics also reads from here.
  cluster: Map<string, TopicMetadata> = new Map();

  constructor(initial?: TopicMetadata[]) {
    for (const t of initial ?? []) this.cluster.set(t.topic, t);
  }

  async connect() {
    this.connectCalls++;
  }
  async close() {
    this.closeCalls++;
  }
  async listTopics() {
    return [...this.cluster.keys()];
  }
  async describeTopics(topics: string[]) {
    return topics.map(
      (topic) => this.cluster.get(topic) ?? { topic, partitions: [] },
    );
  }
  async createTopics(specs: TopicCreateSpec[]) {
    this.createTopicsCalls.push(specs);
    for (const s of specs) {
      if (this.cluster.has(s.topic)) continue;
      const numPartitions = s.numPartitions ?? 1;
      this.cluster.set(s.topic, {
        topic: s.topic,
        partitions: Array.from({ length: numPartitions }, (_, i) => ({
          partitionId: i,
          leader: 0,
          replicas: [0],
          isr: [0],
        })),
      });
    }
  }
  async createPartitions(specs: PartitionGrowSpec[]) {
    this.createPartitionsCalls.push(specs);
    for (const s of specs) {
      const existing = this.cluster.get(s.topic);
      if (!existing) continue;
      const current = existing.partitions.length;
      if (s.totalCount <= current) continue;
      this.cluster.set(s.topic, {
        topic: s.topic,
        partitions: Array.from({ length: s.totalCount }, (_, i) => ({
          partitionId: i,
          leader: 0,
          replicas: [0],
          isr: [0],
        })),
      });
    }
  }
}

class FakeDriverWithAdmin implements KafkaDriver {
  readonly transactional = false;
  // Spy: every admin() call increments this; the publisher should open a
  // fresh admin per operation and close it deterministically.
  adminCalls = 0;
  constructor(public adminFactory: () => KafkaDriverAdmin) {}
  async connect() {}
  async disconnect() {}
  async sendBatch(messages: PublishableMessage[]): Promise<PublishResult[]> {
    return messages.map((m) => ({ recordId: m.recordId, ok: true }));
  }
  async admin() {
    this.adminCalls++;
    return this.adminFactory();
  }
}

class FakeDriverNoAdmin implements KafkaDriver {
  readonly transactional = false;
  async connect() {}
  async disconnect() {}
  async sendBatch(messages: PublishableMessage[]): Promise<PublishResult[]> {
    return messages.map((m) => ({ recordId: m.recordId, ok: true }));
  }
  // No admin() implemented.
}

describe("KafkaPublisher.admin / ensureTopics", () => {
  it("publisher.admin() opens, connects and returns the driver admin", async () => {
    const fake = new FakeAdmin();
    const driver = new FakeDriverWithAdmin(() => fake);
    const pub = new KafkaPublisher({ brokers: ["x"], customDriver: driver });

    const admin = await pub.admin();
    expect(fake.connectCalls).toBe(1);
    expect(driver.adminCalls).toBe(1);
    // Caller owns the lifecycle when using `publisher.admin()` directly.
    expect(fake.closeCalls).toBe(0);

    await admin.close();
    expect(fake.closeCalls).toBe(1);
  });

  it("publisher.admin() throws when the driver does not implement admin()", async () => {
    const driver = new FakeDriverNoAdmin();
    const pub = new KafkaPublisher({ brokers: ["x"], customDriver: driver });
    await expect(pub.admin()).rejects.toThrow(/does not implement admin/);
  });

  it("ensureTopics creates only missing topics and closes the admin", async () => {
    const fake = new FakeAdmin([
      { topic: "existing", partitions: [{ partitionId: 0, leader: 0, replicas: [0], isr: [0] }] },
    ]);
    const driver = new FakeDriverWithAdmin(() => fake);
    const pub = new KafkaPublisher({ brokers: ["x"], customDriver: driver });

    await pub.ensureTopics([
      { topic: "existing", numPartitions: 3 },
      { topic: "new-one", numPartitions: 6, replicationFactor: 3 },
    ]);

    // Only "new-one" should be created — the existing topic is left alone
    // (we never reconcile partition counts unless growPartitions is on).
    expect(fake.createTopicsCalls).toHaveLength(1);
    expect(fake.createTopicsCalls[0]).toEqual([
      { topic: "new-one", numPartitions: 6, replicationFactor: 3 },
    ]);
    expect(fake.createPartitionsCalls).toHaveLength(0);
    expect(fake.closeCalls).toBe(1);
  });

  it("ensureTopics with growPartitions grows undersized topics", async () => {
    const fake = new FakeAdmin([
      {
        topic: "wide",
        partitions: [
          { partitionId: 0, leader: 0, replicas: [0], isr: [0] },
          { partitionId: 1, leader: 0, replicas: [0], isr: [0] },
        ],
      },
    ]);
    const driver = new FakeDriverWithAdmin(() => fake);
    const pub = new KafkaPublisher({ brokers: ["x"], customDriver: driver });

    await pub.ensureTopics(
      [
        { topic: "wide", numPartitions: 6 },
        { topic: "narrow", numPartitions: 1 },
      ],
      { growPartitions: true },
    );

    expect(fake.createTopicsCalls[0]).toEqual([
      { topic: "narrow", numPartitions: 1 },
    ]);
    expect(fake.createPartitionsCalls).toHaveLength(1);
    expect(fake.createPartitionsCalls[0]).toEqual([
      { topic: "wide", totalCount: 6 },
    ]);
  });

  it("ensureTopics does NOT grow when growPartitions is false (the default)", async () => {
    const fake = new FakeAdmin([
      {
        topic: "small",
        partitions: [{ partitionId: 0, leader: 0, replicas: [0], isr: [0] }],
      },
    ]);
    const driver = new FakeDriverWithAdmin(() => fake);
    const pub = new KafkaPublisher({ brokers: ["x"], customDriver: driver });

    await pub.ensureTopics([{ topic: "small", numPartitions: 8 }]);
    expect(fake.createPartitionsCalls).toHaveLength(0);
  });

  it("ensureTopics is a no-op on empty spec list (no admin spin-up)", async () => {
    const fake = new FakeAdmin();
    const driver = new FakeDriverWithAdmin(() => fake);
    const pub = new KafkaPublisher({ brokers: ["x"], customDriver: driver });

    await pub.ensureTopics([]);
    expect(driver.adminCalls).toBe(0);
    expect(fake.connectCalls).toBe(0);
  });

  it("ensureTopics closes the admin even when createTopics throws", async () => {
    const fake = new FakeAdmin();
    fake.createTopics = vi
      .fn()
      .mockRejectedValueOnce(new Error("zookeeper offline"));
    const driver = new FakeDriverWithAdmin(() => fake);
    const pub = new KafkaPublisher({ brokers: ["x"], customDriver: driver });

    await expect(
      pub.ensureTopics([{ topic: "doomed", numPartitions: 1 }]),
    ).rejects.toThrow(/zookeeper offline/);
    expect(fake.closeCalls).toBe(1);
  });

  it("validateTopicsOnConnect: connect succeeds when every topic exists", async () => {
    const fake = new FakeAdmin([
      { topic: "orders", partitions: [{ partitionId: 0, leader: 0, replicas: [0], isr: [0] }] },
      { topic: "events", partitions: [{ partitionId: 0, leader: 0, replicas: [0], isr: [0] }] },
    ]);
    const driver = new FakeDriverWithAdmin(() => fake);
    const pub = new KafkaPublisher({
      brokers: ["x"],
      customDriver: driver,
      validateTopicsOnConnect: ["orders", "events"],
    });

    await expect(pub.connect()).resolves.toBeUndefined();
    expect(fake.closeCalls).toBe(1);
  });

  it("validateTopicsOnConnect: connect throws listing every missing topic", async () => {
    const fake = new FakeAdmin([
      { topic: "orders", partitions: [{ partitionId: 0, leader: 0, replicas: [0], isr: [0] }] },
    ]);
    const driver = new FakeDriverWithAdmin(() => fake);
    const pub = new KafkaPublisher({
      brokers: ["x"],
      customDriver: driver,
      validateTopicsOnConnect: ["orders", "events", "dlq"],
    });

    await expect(pub.connect()).rejects.toThrow(/events.*dlq|dlq.*events/);
    // Even on failure the admin client must be closed (no leaks).
    expect(fake.closeCalls).toBe(1);
  });

  it("validateTopicsOnConnect: empty list skips the admin spin-up", async () => {
    const fake = new FakeAdmin();
    const driver = new FakeDriverWithAdmin(() => fake);
    const pub = new KafkaPublisher({
      brokers: ["x"],
      customDriver: driver,
      validateTopicsOnConnect: [],
    });

    await pub.connect();
    expect(driver.adminCalls).toBe(0);
  });
});
