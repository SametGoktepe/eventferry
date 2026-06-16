import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Pool } from "mysql2/promise";
import { MysqlStore, createMigrationSql } from "@eventferry/mysql";
import { newMysqlPool, uniqueName } from "./helpers.js";

describe("MysqlStore against real MySQL 8", () => {
  let pool: Pool;
  beforeAll(() => {
    pool = newMysqlPool();
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
    const store = new MysqlStore({ pool, table });
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      await store.enqueue(conn, {
        topic: "orders.created",
        aggregateType: "order",
        aggregateId,
        payload,
      });
      await conn.commit();
    } finally {
      conn.release();
    }
  }

  it("enqueues inside a transaction and claims the row", async () => {
    const table = await freshTable();
    await enqueue(table, "a1", { x: 1 });

    const store = new MysqlStore({ pool, table });
    const claimed = await store.claimBatch(10);

    expect(claimed).toHaveLength(1);
    expect(claimed[0]?.aggregateId).toBe("a1");
    expect(claimed[0]?.payload).toEqual({ x: 1 });
    expect(claimed[0]?.status).toBe("processing");
  });

  it("a rolled-back transaction enqueues nothing", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      await store.enqueue(conn, {
        topic: "t",
        aggregateType: "order",
        aggregateId: "a1",
        payload: {},
      });
      await conn.rollback();
    } finally {
      conn.release();
    }
    expect(await store.claimBatch(10)).toHaveLength(0);
  });

  it("enforces strict head-of-aggregate ordering", async () => {
    const table = await freshTable();
    await enqueue(table, "agg", { n: 1 });
    await enqueue(table, "agg", { n: 2 });

    const store = new MysqlStore({ pool, table });
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
    const store = new MysqlStore({ pool, table });

    const claimed = await store.claimBatch(10);
    expect(claimed).toHaveLength(1);
    // Simulate the owning relay having died an hour ago.
    await pool.query(
      `UPDATE \`${table}\` SET claimed_at = DATE_SUB(NOW(3), INTERVAL 1 HOUR) WHERE id = ?`,
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
      `UPDATE \`${table}\` SET status = 3, next_retry_at = NULL WHERE aggregate_id = 'failed-agg'`,
    );

    const failedOnly = new MysqlStore({ pool, table, claimFailedOnly: true });
    const claimed = await failedOnly.claimBatch(10);

    expect(claimed).toHaveLength(1);
    expect(claimed[0]?.payload).toEqual({ f: true });
  });

  it("markDone flips status to done(2) and stamps processed_at", async () => {
    const table = await freshTable();
    await enqueue(table, "a1", { x: 1 });
    const store = new MysqlStore({ pool, table });

    const claimed = await store.claimBatch(10);
    await store.markDone([claimed[0]!.id]);

    const [rows] = await pool.query(
      `SELECT status, processed_at FROM \`${table}\` WHERE id = ?`,
      [claimed[0]!.id],
    );
    const row = (rows as Array<{ status: number; processed_at: Date | null }>)[0]!;
    expect(row.status).toBe(2);
    expect(row.processed_at).not.toBeNull();
  });

  it("markFailed increments attempts and stores next_retry_at", async () => {
    const table = await freshTable();
    await enqueue(table, "a1", { x: 1 });
    const store = new MysqlStore({ pool, table });

    const claimed = await store.claimBatch(10);
    const retryAt = new Date(Date.now() + 60_000);
    await store.markFailed(claimed[0]!.id, retryAt, "failed");

    const [rows] = await pool.query(
      `SELECT status, attempts, next_retry_at FROM \`${table}\` WHERE id = ?`,
      [claimed[0]!.id],
    );
    const row = (rows as Array<{
      status: number;
      attempts: number;
      next_retry_at: Date | null;
    }>)[0]!;
    expect(row.status).toBe(3);
    expect(row.attempts).toBe(1);
    expect(row.next_retry_at).not.toBeNull();
  });

  it("purgeDone deletes only done rows older than the cutoff", async () => {
    const table = await freshTable();
    await enqueue(table, "a1", { x: 1 });
    await enqueue(table, "a2", { x: 2 });
    const store = new MysqlStore({ pool, table });

    // Mark one done with an old processed_at so it qualifies for purge.
    const claimed = await store.claimBatch(10);
    await store.markDone([claimed[0]!.id]);
    await pool.query(
      `UPDATE \`${table}\` SET processed_at = DATE_SUB(NOW(3), INTERVAL 1 HOUR) WHERE id = ?`,
      [claimed[0]!.id],
    );

    const purged = await store.purgeDone({ olderThanMs: 60_000, batchSize: 100 });
    expect(purged).toBe(1);

    // The other row (still pending) is untouched.
    const [rows] = await pool.query(`SELECT COUNT(*) AS n FROM \`${table}\``);
    expect((rows as Array<{ n: number | string }>)[0]?.n).toBe(1);
  });
});
