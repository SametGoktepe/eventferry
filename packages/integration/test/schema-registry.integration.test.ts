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
