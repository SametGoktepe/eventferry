import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Pool } from "pg";
import { PostgresStore, createMigrationSql } from "@eventferry/postgres";
import { newPool, uniqueName } from "./helpers.js";

describe("PostgresStore against real Postgres", () => {
  let pool: Pool;
  beforeAll(() => {
    pool = newPool();
  });
  afterAll(async () => {
    await pool.end();
  });

  async function freshTable(): Promise<string> {
    const table = uniqueName("outbox");
    await pool.query(createMigrationSql(table));
    return table;
  }

  async function enqueue(
    table: string,
    aggregateId: string,
    payload: unknown,
  ): Promise<void> {
    const store = new PostgresStore({ pool, table });
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await store.enqueue(client, {
        topic: "orders.created",
        aggregateType: "order",
        aggregateId,
        payload,
      });
      await client.query("COMMIT");
    } finally {
      client.release();
    }
  }

  it("enqueues inside a transaction and claims the row", async () => {
    const table = await freshTable();
    await enqueue(table, "a1", { x: 1 });

    const store = new PostgresStore({ pool, table });
    const claimed = await store.claimBatch(10);

    expect(claimed).toHaveLength(1);
    expect(claimed[0]?.aggregateId).toBe("a1");
    expect(claimed[0]?.payload).toEqual({ x: 1 });
    expect(claimed[0]?.status).toBe("processing");
  });

  it("a rolled-back transaction enqueues nothing", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await store.enqueue(client, {
        topic: "t",
        aggregateType: "order",
        aggregateId: "a1",
        payload: {},
      });
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }
    expect(await store.claimBatch(10)).toHaveLength(0);
  });

  it("enforces strict head-of-aggregate ordering", async () => {
    const table = await freshTable();
    await enqueue(table, "agg", { n: 1 });
    await enqueue(table, "agg", { n: 2 });

    const store = new PostgresStore({ pool, table });
    const first = await store.claimBatch(10);
    expect(first).toHaveLength(1); // only the head, not both
    expect(first[0]?.payload).toEqual({ n: 1 });

    expect(await store.claimBatch(10)).toHaveLength(0); // successor blocked

    await store.markDone([first[0]!.id]);
    const second = await store.claimBatch(10);
    expect(second).toHaveLength(1);
    expect(second[0]?.payload).toEqual({ n: 2 });
  });

  it("reclaims a row stuck in processing past the claim timeout (reaper)", async () => {
    const table = await freshTable();
    await enqueue(table, "a1", { x: 1 });
    const store = new PostgresStore({ pool, table });

    const claimed = await store.claimBatch(10);
    expect(claimed).toHaveLength(1);
    // Simulate the owning relay having died an hour ago.
    await pool.query(
      `UPDATE ${table} SET claimed_at = now() - interval '1 hour' WHERE id = $1`,
      [claimed[0]!.id],
    );

    const reclaimed = await store.claimBatch(10); // default 60s timeout
    expect(reclaimed.map((r) => r.id)).toEqual([claimed[0]!.id]);
  });

  it("claimFailedOnly claims failed rows but never pending ones", async () => {
    const table = await freshTable();
    await enqueue(table, "pending-agg", { p: true });
    await enqueue(table, "failed-agg", { f: true });
    // Demote the second row to failed(3), due now.
    await pool.query(
      `UPDATE ${table} SET status = 3, next_retry_at = NULL WHERE aggregate_id = 'failed-agg'`,
    );

    const failedOnly = new PostgresStore({ pool, table, claimFailedOnly: true });
    const claimed = await failedOnly.claimBatch(10);

    expect(claimed).toHaveLength(1);
    expect(claimed[0]?.payload).toEqual({ f: true });
  });
});
