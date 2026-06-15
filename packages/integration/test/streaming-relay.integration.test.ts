import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Pool } from "pg";
import type {
  Publisher,
  PublishableMessage,
  PublishResult,
} from "@eventferry/core";
import {
  PostgresStore,
  PostgresStreamingRelay,
  createMigrationSql,
  createPublicationSql,
} from "@eventferry/postgres";
import { KafkaPublisher } from "@eventferry/kafka";
import {
  brokers,
  collectMessages,
  createTopic,
  newPool,
  pgUrl,
  uniqueName,
} from "./helpers.js";

/** Publisher that fails its first publish call, then delegates to the real one. */
class FailFirstPublisher implements Publisher {
  calls = 0;
  constructor(private readonly inner: Publisher) {}
  connect() {
    return this.inner.connect();
  }
  disconnect() {
    return this.inner.disconnect();
  }
  async publish(messages: PublishableMessage[]): Promise<PublishResult[]> {
    this.calls += 1;
    if (this.calls === 1) {
      return messages.map((m) => ({
        recordId: m.recordId,
        ok: false,
        error: new Error("transient"),
      }));
    }
    return this.inner.publish(messages);
  }
}

describe("PostgresStreamingRelay against real Postgres + Redpanda", () => {
  let pool: Pool;
  const slots: string[] = [];

  beforeAll(() => {
    pool = newPool();
  });
  afterAll(async () => {
    for (const s of slots) {
      await pool
        .query(
          "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
          [s],
        )
        .catch(() => undefined);
    }
    await pool.end();
  });

  async function setupStream(publisher?: Publisher) {
    const table = uniqueName("outbox");
    const topic = uniqueName("orders");
    const publication = uniqueName("pub");
    const slot = uniqueName("slot");
    slots.push(slot);
    await pool.query(createMigrationSql(table));
    await pool.query(createPublicationSql(table, publication));
    await createTopic(topic);
    const store = new PostgresStore({ pool, table, claimFailedOnly: true });
    const relay = new PostgresStreamingRelay({
      store,
      publisher: publisher ?? new KafkaPublisher({ brokers: brokers(), idempotent: true }),
      replication: { connectionString: pgUrl(), slot, publication, table },
      failedPollIntervalMs: 500,
    });
    return { topic, store, relay };
  }

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

  it("streams a committed insert from the WAL to the broker", async () => {
    const { topic, store, relay } = await setupStream();
    await relay.start();
    try {
      await enqueue(store, topic, "agg-1", { orderId: "agg-1", total: 42 });
      const msgs = await collectMessages(topic, 1);
      expect(JSON.parse(msgs[0]!.value.toString("utf8"))).toEqual({
        orderId: "agg-1",
        total: 42,
      });
      expect(msgs[0]!.headers["aggregate-id"]).toBe("agg-1");
      expect(msgs[0]!.key).toBe("agg-1");
    } finally {
      await relay.stop();
    }
  });

  it("recovers a failed publish via the internal retry loop", async () => {
    const publisher = new FailFirstPublisher(
      new KafkaPublisher({ brokers: brokers(), idempotent: true }),
    );
    const { topic, store, relay } = await setupStream(publisher);
    await relay.start();
    try {
      await enqueue(store, topic, "agg-2", { orderId: "agg-2" });
      // First stream publish fails -> row demoted to failed -> retry loop republishes.
      const msgs = await collectMessages(topic, 1);
      expect(JSON.parse(msgs[0]!.value.toString("utf8"))).toEqual({
        orderId: "agg-2",
      });
      expect(publisher.calls).toBeGreaterThanOrEqual(2);
    } finally {
      await relay.stop();
    }
  });
});
