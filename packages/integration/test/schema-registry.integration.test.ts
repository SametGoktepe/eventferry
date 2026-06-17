import { describe, expect, it } from "vitest";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import type { OutboxRecord } from "@eventferry/core";
import { SchemaRegistrySerializer } from "@eventferry/schema-registry";
import { schemaRegistryUrl, uniqueName } from "./helpers.js";

function record(topic: string, payload: unknown): OutboxRecord {
  return {
    id: "1",
    messageId: "m1",
    topic,
    aggregateType: "order",
    aggregateId: "a1",
    key: null,
    payload,
    headers: {},
    traceId: null,
    status: "pending",
    attempts: 0,
    nextRetryAt: null,
    createdAt: new Date(),
    processedAt: null,
  };
}

describe("SchemaRegistrySerializer against real Schema Registry (Redpanda)", () => {
  it("encodes Avro that the registry can decode back (roundtrip)", async () => {
    const topic = uniqueName("orders");
    const avro = JSON.stringify({
      type: "record",
      name: "Order",
      fields: [
        { name: "orderId", type: "string" },
        { name: "total", type: "int" },
      ],
    });
    const serializer = new SchemaRegistrySerializer({
      host: schemaRegistryUrl(),
      schemas: { [topic]: { type: "AVRO", schema: avro } },
    });

    const buf = await serializer.serialize(record(topic, { orderId: "a1", total: 5 }));

    const registry = new SchemaRegistry({ host: schemaRegistryUrl() });
    const decoded = await registry.decode(buf);
    expect(decoded).toEqual({ orderId: "a1", total: 5 });
  });

  it("serializeKey roundtrips the key through the -key subject (Phase B3)", async () => {
    const topic = uniqueName("keyed");
    const avroValue = JSON.stringify({
      type: "record",
      name: "OrderValue",
      fields: [{ name: "orderId", type: "string" }],
    });
    // Avro registry helper (@kafkajs/confluent-schema-registry) requires a
    // named type for register() — primitive `{"type":"string"}` fails its
    // own name-validation. Use a record-typed key schema (the conventional
    // shape for non-trivial keys anyway).
    const avroKey = JSON.stringify({
      type: "record",
      name: "OrderKey",
      fields: [{ name: "id", type: "string" }],
    });

    const serializer = new SchemaRegistrySerializer({
      host: schemaRegistryUrl(),
      schemas: { [topic]: { type: "AVRO", schema: avroValue } },
      keySchemas: { [topic]: { type: "AVRO", schema: avroKey } },
    });

    const rec = record(topic, { orderId: "agg-7" });
    // record.key is conventionally a string, but the schema-registry
    // serializer treats it as the payload to encode — pass the structured
    // shape the key schema expects.
    rec.key = JSON.stringify({ id: "agg-7" });
    rec.payload = { orderId: "agg-7" };
    const encodedValue = await serializer.serialize(rec);
    // serializeKey receives the parsed object — the publisher would do the
    // same projection in the production wiring.
    const recForKey = { ...rec, key: { id: "agg-7" } as unknown as string };
    const encodedKey = await serializer.serializeKey(recForKey);

    expect(encodedKey).not.toBeNull();
    const registry = new SchemaRegistry({ host: schemaRegistryUrl() });
    const decodedValue = await registry.decode(encodedValue);
    const decodedKey = await registry.decode(encodedKey!);
    expect(decodedValue).toEqual({ orderId: "agg-7" });
    expect(decodedKey).toEqual({ id: "agg-7" });
  });

  it("autoRegister: false uses getLatestSchemaId on already-registered subjects (Phase B3)", async () => {
    const topic = uniqueName("readonly");
    const avro = JSON.stringify({
      type: "record",
      name: "Probe",
      fields: [{ name: "v", type: "string" }],
    });

    // Pre-register the value subject by running once with autoRegister on.
    const seeder = new SchemaRegistrySerializer({
      host: schemaRegistryUrl(),
      schemas: { [topic]: { type: "AVRO", schema: avro } },
    });
    await seeder.serialize(record(topic, { v: "seed" }));

    // Now consume with autoRegister: false — no schemas supplied, just
    // lookup-by-subject. Must still encode + roundtrip.
    const consumer = new SchemaRegistrySerializer({
      host: schemaRegistryUrl(),
      autoRegister: false,
    });
    const buf = await consumer.serialize(record(topic, { v: "live" }));

    const registry = new SchemaRegistry({ host: schemaRegistryUrl() });
    const decoded = await registry.decode(buf);
    expect(decoded).toEqual({ v: "live" });
  });

  it("RecordNameStrategy picks the subject by recordName (Phase B3)", async () => {
    const topic = uniqueName("rn-topic");
    const recordType = "com.example.OrderCreated_" + Date.now().toString(36);
    const avro = JSON.stringify({
      type: "record",
      name: recordType.split(".").pop(),
      namespace: recordType.split(".").slice(0, -1).join("."),
      fields: [{ name: "v", type: "string" }],
    });

    const serializer = new SchemaRegistrySerializer({
      host: schemaRegistryUrl(),
      subjectStrategy: "RecordNameStrategy",
      recordName: () => recordType,
      schemas: { [topic]: { type: "AVRO", schema: avro } },
    });

    const buf = await serializer.serialize(record(topic, { v: "hello" }));

    // The subject the schema landed under should be the record name, NOT
    // ${topic}-value. Verify by encoding via the same subject directly.
    const registry = new SchemaRegistry({ host: schemaRegistryUrl() });
    const latestId = await registry.getLatestSchemaId(recordType);
    const direct = await registry.encode(latestId, { v: "hello" });
    expect(direct.equals(buf)).toBe(true);
  });

  it("encodes JSON Schema that the registry can decode back", async () => {
    const topic = uniqueName("events");
    const jsonSchema = JSON.stringify({
      type: "object",
      properties: { name: { type: "string" } },
      required: ["name"],
      additionalProperties: false,
    });
    const serializer = new SchemaRegistrySerializer({
      host: schemaRegistryUrl(),
      schemas: { [topic]: { type: "JSON", schema: jsonSchema } },
    });

    const buf = await serializer.serialize(record(topic, { name: "alice" }));

    const registry = new SchemaRegistry({ host: schemaRegistryUrl() });
    const decoded = await registry.decode(buf);
    expect(decoded).toEqual({ name: "alice" });
  });
});
