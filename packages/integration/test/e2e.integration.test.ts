import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Pool } from "pg";
import { Relay } from "@eventferry/core";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { PostgresStore, createMigrationSql } from "@eventferry/postgres";
import { KafkaPublisher } from "@eventferry/kafka";
import { SchemaRegistrySerializer } from "@eventferry/schema-registry";
import {
  brokers,
  collectMessages,
  createTopic,
  newPool,
  schemaRegistryUrl,
  uniqueName,
} from "./helpers.js";

describe("End-to-end: enqueue -> polling relay -> Redpanda", () => {
  let pool: Pool;
  beforeAll(() => {
    pool = newPool();
  });
  afterAll(async () => {
    await pool.end();
  });

  async function enqueue(
    store: PostgresStore,
    topic: string,
    aggregateId: string,
    payload: unknown,
  ) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await store.enqueue(client, {
        topic,
        aggregateType: "order",
        aggregateId,
        payload,
      });
      await client.query("COMMIT");
    } finally {
      client.release();
    }
  }

  it("publishes a JSON event end to end with W3C trace context", async () => {
    const table = uniqueName("outbox");
    const topic = uniqueName("orders");
    await pool.query(createMigrationSql(table));
    await createTopic(topic);

    const store = new PostgresStore({
      pool,
      table,
      tracing: { inject: (c) => (c.traceparent = "00-abc-def-01") },
    });
    const relay = new Relay({
      store,
      publisher: new KafkaPublisher({ brokers: brokers(), idempotent: true }),
      pollIntervalMs: 200,
    });

    await relay.start();
    try {
      await enqueue(store, topic, "a1", { orderId: "a1", total: 7 });
      const msgs = await collectMessages(topic, 1);
      expect(JSON.parse(msgs[0]!.value.toString("utf8"))).toEqual({
        orderId: "a1",
        total: 7,
      });
      // Trace context captured at enqueue rides through to the consumer.
      expect(msgs[0]!.headers["traceparent"]).toBe("00-abc-def-01");
    } finally {
      await relay.stop();
    }
  });

  it("publishes a Schema-Registry-encoded event end to end", async () => {
    const table = uniqueName("outbox");
    const topic = uniqueName("orders");
    await pool.query(createMigrationSql(table));
    await createTopic(topic);

    const avro = JSON.stringify({
      type: "record",
      name: "Order",
      fields: [{ name: "orderId", type: "string" }],
    });
    const store = new PostgresStore({ pool, table });
    const relay = new Relay({
      store,
      publisher: new KafkaPublisher({ brokers: brokers(), idempotent: true }),
      serializer: new SchemaRegistrySerializer({
        host: schemaRegistryUrl(),
        schemas: { [topic]: { type: "AVRO", schema: avro } },
      }),
      pollIntervalMs: 200,
    });

    await relay.start();
    try {
      await enqueue(store, topic, "a1", { orderId: "a1" });
      const msgs = await collectMessages(topic, 1);
      const registry = new SchemaRegistry({ host: schemaRegistryUrl() });
      const decoded = await registry.decode(msgs[0]!.value);
      expect(decoded).toEqual({ orderId: "a1" });
    } finally {
      await relay.stop();
    }
  });
});
