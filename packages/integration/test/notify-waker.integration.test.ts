import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Client, type Pool } from "pg";
import { Relay } from "@eventferry/core";
import {
  PostgresStore,
  PostgresNotifyWaker,
  createMigrationSql,
  createNotifyTriggerSql,
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

describe("PostgresNotifyWaker against real Postgres (LISTEN/NOTIFY)", () => {
  let pool: Pool;
  beforeAll(() => {
    pool = newPool();
  });
  afterAll(async () => {
    await pool.end();
  });

  it("wakes the relay on insert instead of waiting out the poll interval", async () => {
    const table = uniqueName("outbox");
    const topic = uniqueName("orders");
    const channel = uniqueName("ch");
    await pool.query(createMigrationSql(table));
    await pool.query(createNotifyTriggerSql(table, channel));
    await createTopic(topic);

    const store = new PostgresStore({ pool, table });
    const relay = new Relay({
      store,
      publisher: new KafkaPublisher({ brokers: brokers(), idempotent: true }),
      // Deliberately huge: if polling fired this, the test would time out first.
      pollIntervalMs: 60_000,
      waker: new PostgresNotifyWaker({
        connect: () => new Client({ connectionString: pgUrl() }),
        channel,
      }),
    });

    await relay.start();
    try {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await store.enqueue(client, {
          topic,
          aggregateType: "order",
          aggregateId: "a1",
          payload: { woken: true },
        });
        await client.query("COMMIT");
      } finally {
        client.release();
      }

      // Must arrive well within the 60s poll interval -> proves the waker fired.
      const msgs = await collectMessages(topic, 1, 15_000);
      expect(JSON.parse(msgs[0]!.value.toString("utf8"))).toEqual({ woken: true });
    } finally {
      await relay.stop();
    }
  });
});
