import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Pool } from "mysql2/promise";
import {
  MysqlBinlogRelay,
  MysqlStore,
  createMigrationSql,
} from "@eventferry/mysql";
import { KafkaPublisher } from "@eventferry/kafka";
import {
  brokers,
  collectMessages,
  createTopic,
  mysqlInfo,
  newMysqlPool,
  uniqueName,
} from "./helpers.js";

describe("MysqlBinlogRelay against real MySQL 8 + Redpanda", () => {
  let pool: Pool;
  beforeAll(() => {
    pool = newMysqlPool();
  });
  afterAll(async () => {
    await pool.end();
  });

  async function setupStream() {
    const table = uniqueName("outbox");
    const topic = uniqueName("orders");
    await pool.query(createMigrationSql(table));
    await createTopic(topic);

    const info = mysqlInfo();
    const store = new MysqlStore({ pool, table, claimFailedOnly: true });
    const relay = new MysqlBinlogRelay({
      store,
      publisher: new KafkaPublisher({ brokers: brokers(), idempotent: true }),
      binlog: {
        host: info.host,
        port: info.port,
        user: info.user,
        password: info.password,
        database: info.database,
        table,
        // Unique serverId per test so concurrent or sequential readers don't
        // collide on MySQL's replica-id check.
        serverId: 900_000 + Math.floor(Math.random() * 90_000),
      },
      failedPollIntervalMs: 500,
    });
    return { topic, table, store, relay };
  }

  async function enqueue(
    table: string,
    aggregateId: string,
    payload: unknown,
  ): Promise<void> {
    const store = new MysqlStore({ pool, table });
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      await store.enqueue(conn, {
        topic: "ignored-here",
        aggregateType: "order",
        aggregateId,
        payload,
      });
      await conn.commit();
    } finally {
      conn.release();
    }
  }

  it("streams a committed insert from the binlog to the broker", async () => {
    const { topic, table, relay } = await setupStream();
    await relay.start();
    // Give zongji a moment to subscribe before we commit the row, otherwise the
    // INSERT lands in the binlog before the reader is positioned.
    await new Promise((r) => setTimeout(r, 500));
    try {
      // Re-enqueue with the actual topic so the published message goes to `topic`.
      const store = new MysqlStore({ pool, table });
      const conn = await pool.getConnection();
      try {
        await conn.beginTransaction();
        await store.enqueue(conn, {
          topic,
          aggregateType: "order",
          aggregateId: "agg-1",
          payload: { orderId: "agg-1", total: 42 },
        });
        await conn.commit();
      } finally {
        conn.release();
      }

      const msgs = await collectMessages(topic, 1);
      expect(JSON.parse(msgs[0]!.value.toString("utf8"))).toEqual({
        orderId: "agg-1",
        total: 42,
      });
      expect(msgs[0]!.key).toBe("agg-1");
    } finally {
      await relay.stop();
    }
  });

  it("delivers multiple inserts in commit order", async () => {
    const { topic, table, relay } = await setupStream();
    await relay.start();
    await new Promise((r) => setTimeout(r, 500));
    try {
      const store = new MysqlStore({ pool, table });
      for (const [i, agg] of ["a1", "a2", "a3"].entries()) {
        const conn = await pool.getConnection();
        try {
          await conn.beginTransaction();
          await store.enqueue(conn, {
            topic,
            aggregateType: "order",
            aggregateId: agg,
            payload: { n: i + 1 },
          });
          await conn.commit();
        } finally {
          conn.release();
        }
      }

      const msgs = await collectMessages(topic, 3);
      const payloads = msgs.map(
        (m) => JSON.parse(m.value.toString("utf8")) as { n: number },
      );
      expect(payloads.map((p) => p.n)).toEqual([1, 2, 3]);
    } finally {
      await relay.stop();
    }
  });

  // Note: failed-publish recovery is exercised by the postgres streaming-relay
  // suite; the recovery code lives in core and is shared, so it doesn't need a
  // MySQL-specific test. Add one if the binlog path ever diverges.
});
