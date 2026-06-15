import { describe, expect, it } from "vitest";
import {
  SchemaRegistrySerializer,
  type SchemaRegistryClient,
} from "../src/serializer.js";
import type { OutboxRecord } from "@eventferry/core";

function record(over: Partial<OutboxRecord> = {}): OutboxRecord {
  return {
    id: "1",
    messageId: "m1",
    topic: "orders.created",
    aggregateType: "order",
    aggregateId: "a1",
    key: null,
    payload: { orderId: "a1", total: 5 },
    headers: {},
    traceId: null,
    status: "pending",
    attempts: 0,
    nextRetryAt: null,
    createdAt: new Date(),
    processedAt: null,
    ...over,
  };
}

class FakeRegistry implements SchemaRegistryClient {
  registered: { schema: { type: string; schema: string }; subject?: string }[] = [];
  latestLookups: string[] = [];
  encodes: { id: number; payload: unknown }[] = [];
  registerId = 10;
  latestId = 99;

  async register(
    schema: { type: string; schema: string },
    opts?: { subject: string },
  ): Promise<{ id: number }> {
    this.registered.push({ schema, subject: opts?.subject });
    return { id: this.registerId };
  }
  async getLatestSchemaId(subject: string): Promise<number> {
    this.latestLookups.push(subject);
    return this.latestId;
  }
  async encode(registryId: number, payload: unknown): Promise<Buffer> {
    this.encodes.push({ id: registryId, payload });
    return Buffer.from(`enc:${registryId}`, "utf8");
  }
}

describe("SchemaRegistrySerializer", () => {
  it("registers a provided schema and encodes with its id", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      schemas: { "orders.created": { type: "AVRO", schema: '{"type":"record"}' } },
    });

    const buf = await serializer.serialize(record());

    expect(registry.registered).toHaveLength(1);
    expect(registry.registered[0]?.schema.type).toBe("AVRO");
    expect(registry.registered[0]?.subject).toBe("orders.created-value");
    expect(registry.encodes[0]).toEqual({ id: 10, payload: { orderId: "a1", total: 5 } });
    expect(buf.toString("utf8")).toBe("enc:10");
  });

  it("caches the schema id (does not re-register on the next serialize)", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      schemas: { "orders.created": { type: "AVRO", schema: "{}" } },
    });

    await serializer.serialize(record());
    await serializer.serialize(record({ id: "2" }));

    expect(registry.registered).toHaveLength(1); // registered once
    expect(registry.encodes).toHaveLength(2); // encoded twice
  });

  it("uses the subject's latest schema when none is provided", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({ registry });

    await serializer.serialize(record({ topic: "orders.shipped" }));

    expect(registry.latestLookups).toEqual(["orders.shipped-value"]);
    expect(registry.encodes[0]?.id).toBe(99);
    expect(registry.registered).toHaveLength(0);
  });

  it("honors a custom subject strategy", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      subject: (topic) => `custom.${topic}`,
    });

    await serializer.serialize(record({ topic: "t" }));
    expect(registry.latestLookups).toEqual(["custom.t"]);
  });

  it("exposes a default content-type, overridable", () => {
    const registry = new FakeRegistry();
    expect(new SchemaRegistrySerializer({ registry }).contentType).toBe(
      "application/vnd.confluent.avro",
    );
    expect(
      new SchemaRegistrySerializer({ registry, contentType: "application/x-foo" })
        .contentType,
    ).toBe("application/x-foo");
  });

  it("throws when neither registry nor host is given", () => {
    expect(() => new SchemaRegistrySerializer({})).toThrow();
  });
});
