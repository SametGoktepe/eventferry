import { afterEach, describe, expect, it, vi } from "vitest";
import {
  KafkaJsDriver,
  _resetKafkajsWarnDedup,
} from "../src/kafkajs-driver.js";

// Stub kafkajs Partitioners namespace so buildProducerOptions has the
// surface it expects. The factories are noop — we never call them through.
const fakePartitioners = {
  DefaultPartitioner: () => () => 0,
  LegacyPartitioner: () => () => 0,
  JavaCompatiblePartitioner: () => () => 0,
};

// Test seam: bypass the dynamic kafkajs import and expose buildProducerOptions
// directly so the assertions are deterministic without a broker.
class InspectableDriver extends KafkaJsDriver {
  inspectProducerOptions(): Promise<Record<string, unknown>> {
    return (this as unknown as {
      buildProducerOptions: (p: unknown) => Promise<Record<string, unknown>>;
    }).buildProducerOptions(fakePartitioners);
  }
}

describe("KafkaJsDriver — customPartitioner escape hatch", () => {
  afterEach(() => _resetKafkajsWarnDedup());

  it("wins over the partitioner preset", async () => {
    const custom = () => () => 7;
    const driver = new InspectableDriver({
      brokers: ["b:9092"],
      partitioner: "java-compatible",
      customPartitioner: custom,
    });
    const opts = await driver.inspectProducerOptions();
    expect(opts.createPartitioner).toBe(custom);
  });

  it("falls through to the preset when not set", async () => {
    const driver = new InspectableDriver({
      brokers: ["b:9092"],
      partitioner: "legacy",
    });
    const opts = await driver.inspectProducerOptions();
    expect(opts.createPartitioner).toBe(fakePartitioners.LegacyPartitioner);
  });
});

describe("KafkaJsDriver — rawKafkaJsProducerConfig escape hatch", () => {
  afterEach(() => _resetKafkajsWarnDedup());

  it("merges raw keys into the producer args", async () => {
    const driver = new InspectableDriver({
      brokers: ["b:9092"],
      rawKafkaJsProducerConfig: {
        retry: { retries: 7 },
        metadataMaxAge: 5_000,
      },
    });
    const opts = await driver.inspectProducerOptions();
    expect(opts["retry"]).toEqual({ retries: 7 });
    expect(opts["metadataMaxAge"]).toBe(5_000);
  });

  it("raw keys WIN against translated ones (escape-hatch precedence)", async () => {
    const driver = new InspectableDriver({
      brokers: ["b:9092"],
      idempotent: true,
      rawKafkaJsProducerConfig: { idempotent: false },
    });
    const opts = await driver.inspectProducerOptions();
    expect(opts["idempotent"]).toBe(false);
  });
});

describe("KafkaJsDriver — kafkajs warns on confluent-only options", () => {
  afterEach(() => {
    _resetKafkajsWarnDedup();
    vi.restoreAllMocks();
  });

  it("warns when compressionLevel is set on kafkajs", () => {
    const spy = vi.spyOn(console, "warn").mockImplementation(() => {});
    new KafkaJsDriver({ brokers: ["b:9092"], compressionLevel: 9 });
    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.calls[0]?.[0]).toMatch(/compressionLevel/);
  });

  it("warns when rawProducerConfig is set on kafkajs", () => {
    const spy = vi.spyOn(console, "warn").mockImplementation(() => {});
    new KafkaJsDriver({
      brokers: ["b:9092"],
      rawProducerConfig: { "linger.ms": 50 },
    });
    expect(spy).toHaveBeenCalledTimes(1);
    expect(spy.mock.calls[0]?.[0]).toMatch(/rawProducerConfig/);
  });
});
