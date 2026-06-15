import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Pool } from "pg";
import { PostgresStore, createMigrationSql } from "@eventferry/postgres";
import { newPool, uniqueName } from "./helpers.js";

describe("purgeDone against real Postgres", () => {
  let pool: Pool;
  beforeAll(() => {
    pool = newPool();
  });
  afterAll(async () => {
    await pool.end();
  });

  async function enqueue(store: PostgresStore, aggregateId: string) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await store.enqueue(client, {
        topic: "t",
        aggregateType: "order",
        aggregateId,
        payload: { id: aggregateId },
      });
      await client.query("COMMIT");
    } finally {
      client.release();
    }
  }

  it("deletes only old done rows, leaving recent/pending/failed intact", async () => {
    const table = uniqueName("outbox");
    await pool.query(createMigrationSql(table));
    const store = new PostgresStore({ pool, table });

    // A, B, C -> done (processed_at = now)
    await enqueue(store, "A");
    await enqueue(store, "B");
    await enqueue(store, "C");
    const claimed = await store.claimBatch(10);
    await store.markDone(claimed.map((r) => r.id));

    // Age A and B past the retention window.
    await pool.query(
      `UPDATE ${table} SET processed_at = now() - interval '2 days' WHERE aggregate_id IN ('A','B')`,
    );
    // P stays pending; F is failed.
    await enqueue(store, "P");
    await enqueue(store, "F");
    await pool.query(`UPDATE ${table} SET status = 3 WHERE aggregate_id = 'F'`);

    const deleted = await store.purgeDone({ olderThanMs: 24 * 60 * 60 * 1000 });
    expect(deleted).toBe(2); // A and B

    const remaining = await pool.query(
      `SELECT aggregate_id FROM ${table} ORDER BY aggregate_id`,
    );
    expect(remaining.rows.map((r) => r.aggregate_id)).toEqual(["C", "F", "P"]);
  });
});
