import { describe, expect, it } from "vitest";
import type {
  PublishableMessage,
  PublishResult,
} from "@eventferry/core";
import { KafkaPublisher } from "../src/publisher.js";
import type { KafkaDriver } from "../src/driver.js";
import type { KafkaDriverAdmin } from "../src/admin.js";

class FakeAdmin implements KafkaDriverAdmin {
  connectCalls = 0;
  closeCalls = 0;
  listCalls = 0;
  constructor(
    private readonly opts: {
      topics?: string[];
      throwOnList?: Error;
      listDelayMs?: number;
    } = {},
  ) {}
  async connect() {
    this.connectCalls++;
  }
  async close() {
    this.closeCalls++;
  }
  async listTopics() {
    this.listCalls++;
    if (this.opts.listDelayMs) {
      await new Promise((r) => setTimeout(r, this.opts.listDelayMs));
    }
    if (this.opts.throwOnList) throw this.opts.throwOnList;
    return this.opts.topics ?? ["t1", "t2"];
  }
  async describeTopics() {
    return [];
  }
  async createTopics() {}
  async createPartitions() {}
}

class DriverWithAdmin implements KafkaDriver {
  readonly transactional = false;
  constructor(public readonly adminFactory: () => KafkaDriverAdmin) {}
  async connect() {}
  async disconnect() {}
  async sendBatch(messages: PublishableMessage[]): Promise<PublishResult[]> {
    return messages.map((m) => ({ recordId: m.recordId, ok: true }));
  }
  async admin() {
    return this.adminFactory();
  }
}

class DriverNoAdmin implements KafkaDriver {
  readonly transactional = false;
  async connect() {}
  async disconnect() {}
  async sendBatch(messages: PublishableMessage[]): Promise<PublishResult[]> {
    return messages.map((m) => ({ recordId: m.recordId, ok: true }));
  }
  // intentionally no admin()
}

describe("KafkaPublisher.healthCheck", () => {
  it("returns ok=true + latency + timestamp on a reachable broker", async () => {
    const admin = new FakeAdmin();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: new DriverWithAdmin(() => admin),
    });
    const status = await pub.healthCheck();
    expect(status.ok).toBe(true);
    expect(status.error).toBeUndefined();
    expect(status.latencyMs).toBeGreaterThanOrEqual(0);
    expect(status.timestamp).toBeGreaterThan(0);
    // Always closes the borrowed admin — no leak on success.
    expect(admin.closeCalls).toBe(1);
  });

  it("returns ok=false with the underlying error when listTopics throws", async () => {
    const admin = new FakeAdmin({ throwOnList: new Error("auth denied") });
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: new DriverWithAdmin(() => admin),
    });
    const status = await pub.healthCheck();
    expect(status.ok).toBe(false);
    expect(status.error?.message).toBe("auth denied");
    // Admin still gets closed on failure — no leak.
    expect(admin.closeCalls).toBe(1);
  });

  it("times out when the broker stalls beyond timeoutMs", async () => {
    const admin = new FakeAdmin({ listDelayMs: 100 });
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: new DriverWithAdmin(() => admin),
    });
    const status = await pub.healthCheck({ timeoutMs: 20 });
    expect(status.ok).toBe(false);
    expect(status.error?.message).toMatch(/timed out after 20ms/);
  });

  it("timeoutMs: 0 opts out of the timer entirely (slow probe still returns ok)", async () => {
    const admin = new FakeAdmin({ listDelayMs: 30 });
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: new DriverWithAdmin(() => admin),
    });
    const status = await pub.healthCheck({ timeoutMs: 0 });
    expect(status.ok).toBe(true);
  });

  it("returns ok=false (does NOT throw) when the driver has no admin()", async () => {
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: new DriverNoAdmin(),
    });
    const status = await pub.healthCheck();
    expect(status.ok).toBe(false);
    expect(status.error?.message).toMatch(/does not implement admin/);
  });

  it("each call opens a FRESH admin (no shared state across probes)", async () => {
    const admins: FakeAdmin[] = [];
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: new DriverWithAdmin(() => {
        const a = new FakeAdmin();
        admins.push(a);
        return a;
      }),
    });
    await pub.healthCheck();
    await pub.healthCheck();
    expect(admins).toHaveLength(2);
    // Both admins were opened, listed, and closed.
    for (const a of admins) {
      expect(a.connectCalls).toBe(1);
      expect(a.listCalls).toBe(1);
      expect(a.closeCalls).toBe(1);
    }
  });

  it("admin.close() throwing does not change the success outcome (best-effort cleanup)", async () => {
    class FlakyClose extends FakeAdmin {
      override async close() {
        await super.close();
        throw new Error("close failed");
      }
    }
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: new DriverWithAdmin(() => new FlakyClose()),
    });
    const status = await pub.healthCheck();
    expect(status.ok).toBe(true);
  });
});
