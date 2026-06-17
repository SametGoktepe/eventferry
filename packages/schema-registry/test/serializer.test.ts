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

describe("SchemaRegistrySerializer — autoRegister", () => {
  it("autoRegister: false ALWAYS uses getLatestSchemaId, even when a schema is provided", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      autoRegister: false,
      schemas: { "orders.created": { type: "AVRO", schema: '{"type":"record"}' } },
    });

    await serializer.serialize(record());

    expect(registry.registered).toHaveLength(0);
    expect(registry.latestLookups).toEqual(["orders.created-value"]);
    expect(registry.encodes[0]?.id).toBe(99);
  });

  it("autoRegister: true (default) registers when a schema is provided", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      schemas: { "orders.created": { type: "AVRO", schema: "{}" } },
    });
    await serializer.serialize(record());
    expect(registry.registered).toHaveLength(1);
  });
});

describe("SchemaRegistrySerializer — subject strategies", () => {
  it("RecordNameStrategy uses the recordName resolver as the subject", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      subjectStrategy: "RecordNameStrategy",
      recordName: (r) => `com.example.${r.aggregateType}.Created`,
    });

    await serializer.serialize(record({ aggregateType: "order" }));
    expect(registry.latestLookups).toEqual(["com.example.order.Created"]);
  });

  it("TopicRecordNameStrategy concatenates topic and record name", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      subjectStrategy: "TopicRecordNameStrategy",
      recordName: (r) => `com.example.${r.aggregateType}.Created`,
    });

    await serializer.serialize(record({ topic: "orders.v2", aggregateType: "order" }));
    expect(registry.latestLookups).toEqual([
      "orders.v2-com.example.order.Created",
    ]);
  });

  it("RecordNameStrategy without a recordName resolver throws on first serialize", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      subjectStrategy: "RecordNameStrategy",
    });
    await expect(serializer.serialize(record())).rejects.toThrow(
      /recordName.*resolver/,
    );
  });

  it("explicit `subject` function overrides any subjectStrategy preset", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      subjectStrategy: "RecordNameStrategy",
      recordName: () => "never-called",
      subject: (topic, isKey) => `wins.${topic}.${isKey ? "k" : "v"}`,
    });
    await serializer.serialize(record({ topic: "t" }));
    expect(registry.latestLookups).toEqual(["wins.t.v"]);
  });

  it("legacy single-arg `subject` callable still works (back-compat)", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      subject: ((topic: string) => `legacy.${topic}`) as (t: string) => string,
    });
    await serializer.serialize(record({ topic: "x" }));
    expect(registry.latestLookups).toEqual(["legacy.x"]);
  });
});

describe("SchemaRegistrySerializer — serializeKey", () => {
  it("returns null when the record has no key (kafka 'no key' record)", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      keySchemas: { "orders.created": { type: "AVRO", schema: '{"type":"string"}' } },
    });
    const result = await serializer.serializeKey(record({ key: null }));
    expect(result).toBeNull();
    expect(registry.registered).toHaveLength(0);
    expect(registry.encodes).toHaveLength(0);
  });

  it("registers a key schema under the -key subject and encodes the key", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      keySchemas: {
        "orders.created": { type: "AVRO", schema: '{"type":"string"}' },
      },
    });
    const buf = await serializer.serializeKey(
      record({ key: "agg-123" }),
    );
    expect(registry.registered).toHaveLength(1);
    expect(registry.registered[0]?.subject).toBe("orders.created-key");
    expect(registry.encodes[0]).toEqual({ id: 10, payload: "agg-123" });
    expect(buf?.toString("utf8")).toBe("enc:10");
  });

  it("value and key subject ids are cached separately (no cross-contamination)", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      schemas: { t: { type: "AVRO", schema: '{"type":"record"}' } },
      keySchemas: { t: { type: "AVRO", schema: '{"type":"string"}' } },
    });
    await serializer.serialize(record({ topic: "t", key: "k1" }));
    await serializer.serializeKey(record({ topic: "t", key: "k1" }));
    // One value register, one key register, but only TWO total — not four,
    // not duplicated on the second serialize call.
    expect(registry.registered).toHaveLength(2);
    const subjects = registry.registered.map((r) => r.subject).sort();
    expect(subjects).toEqual(["t-key", "t-value"]);
  });

  it("serializeKey honors autoRegister: false (lookup latest, ignore local schema)", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({
      registry,
      autoRegister: false,
      keySchemas: { t: { type: "AVRO", schema: "{}" } },
    });
    await serializer.serializeKey(record({ topic: "t", key: "k1" }));
    expect(registry.registered).toHaveLength(0);
    expect(registry.latestLookups).toEqual(["t-key"]);
  });

  it("serializeKey works WITHOUT a keySchemas entry — falls back to subject's latest", async () => {
    const registry = new FakeRegistry();
    const serializer = new SchemaRegistrySerializer({ registry });
    await serializer.serializeKey(record({ topic: "t", key: "k1" }));
    expect(registry.latestLookups).toEqual(["t-key"]);
  });
});
