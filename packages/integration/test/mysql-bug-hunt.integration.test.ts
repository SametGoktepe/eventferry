import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Pool, RowDataPacket } from "mysql2/promise";
import { MysqlStore, createMigrationSql } from "@eventferry/mysql";
import { newMysqlPool, uniqueName } from "./helpers.js";

/**
 * Adversarial coverage for MysqlStore. Mirrors the postgres-bug-hunt
 * suite: concurrent claims, idempotent terminal markers, requeue
 * semantics, empty-batch hygiene, oversized + unicode payloads, dead
 * status fencing. Assertions land on observable database state.
 */
describe("MysqlStore — adversarial / bug hunt", () => {
  let pool: Pool;
  beforeAll(() => {
    pool = newMysqlPool();
  });
  afterAll(async () => {
    await pool.end();
  });

  async function freshTable(): Promise<string> {
    const table = uniqueName("outbox_hunt");
    await pool.query(createMigrationSql(table));
    return table;
  }

  async function enqueue(
    store: MysqlStore,
    aggregateId: string,
    payload: unknown,
    over: Partial<{
      topic: string;
      aggregateType: string;
      key: string;
      headers: Record<string, string>;
    }> = {},
  ): Promise<string> {
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      const id = await store.enqueue(conn, {
        topic: over.topic ?? "orders.created",
        aggregateType: over.aggregateType ?? "order",
        aggregateId,
        payload,
        key: over.key,
        headers: over.headers,
      });
      await conn.commit();
      return id;
    } finally {
      conn.release();
    }
  }

  // ── Concurrency ────────────────────────────────────────────────────────

  it("concurrent claims (different aggregates) do not double-claim any row", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    const ids: string[] = [];
    for (let i = 0; i < 30; i++) {
      ids.push(await enqueue(store, `agg-${i}`, { n: i }));
    }
    const batches = await Promise.all(
      Array.from({ length: 6 }, () => store.claimBatch(10)),
    );
    const grabbed = batches.flat().map((r) => r.id);
    expect(grabbed).toHaveLength(30);
    expect(new Set(grabbed).size).toBe(30); // no duplicates
    const [rows] = await pool.query<({ status: number } & RowDataPacket)[]>(
      `SELECT status FROM \`${table}\``,
    );
    expect(rows.every((r) => r.status === 1)).toBe(true);
  });

  it("strict head-of-aggregate: 50 concurrent claims on a single aggregate yield exactly ONE row", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    for (let i = 0; i < 5; i++) {
      await enqueue(store, "single-agg", { n: i });
    }
    const batches = await Promise.all(
      Array.from({ length: 50 }, () => store.claimBatch(10)),
    );
    const grabbed = batches.flat();
    expect(grabbed).toHaveLength(1);
    expect(grabbed[0]?.payload).toEqual({ n: 0 });
  });

  // ── Idempotency ────────────────────────────────────────────────────────

  it("markDone twice on the same id is idempotent", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    await enqueue(store, "a1", { x: 1 });
    const [r] = await store.claimBatch(10);
    await store.markDone([r!.id]);
    await expect(store.markDone([r!.id])).resolves.toBeUndefined();
    const [rows] = await pool.query<
      ({ status: number; processed_at: Date | null } & RowDataPacket)[]
    >(
      `SELECT status, processed_at FROM \`${table}\` WHERE id = ?`,
      [r!.id],
    );
    expect(rows[0]?.status).toBe(2); // done
    expect(rows[0]?.processed_at).not.toBeNull();
  });

  it("markDone on a non-existent id is a no-op", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    await expect(store.markDone(["99999999"])).resolves.toBeUndefined();
  });

  it("markDone on an empty id list is a no-op", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    await expect(store.markDone([])).resolves.toBeUndefined();
  });

  // ── Requeue (backpressure path) ────────────────────────────────────────

  it("requeue resets the row to failed WITHOUT incrementing attempts", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    await enqueue(store, "a1", { x: 1 });
    const [r] = await store.claimBatch(10);
    expect(r!.attempts).toBe(0);
    if (typeof store.requeue !== "function") {
      throw new Error("MysqlStore.requeue not implemented — regression");
    }
    const retryAt = new Date(Date.now() + 1000);
    await store.requeue(r!.id, retryAt);
    const [rows] = await pool.query<
      ({
        status: number;
        attempts: number;
        claimed_at: Date | null;
      } & RowDataPacket)[]
    >(
      `SELECT status, attempts, claimed_at FROM \`${table}\` WHERE id = ?`,
      [r!.id],
    );
    expect(rows[0]?.status).toBe(3); // failed
    expect(rows[0]?.attempts).toBe(0); // not incremented — that's the point
    expect(rows[0]?.claimed_at).toBeNull();
  });

  // ── claimBatch hygiene ─────────────────────────────────────────────────

  it("claimBatch(0) returns [] without crashing", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    await enqueue(store, "a1", { x: 1 });
    expect(await store.claimBatch(0)).toEqual([]);
  });

  it("claimBatch on an empty table returns []", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    expect(await store.claimBatch(10)).toEqual([]);
  });

  // ── Large / weird payloads ─────────────────────────────────────────────

  it("survives a 1 MB JSON payload round-trip", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    const big = { items: Array.from({ length: 50_000 }, (_, i) => ({ i, k: "v" })) };
    await enqueue(store, "big", big);
    const [r] = await store.claimBatch(10);
    expect(r?.payload).toEqual(big);
  });

  it("unicode in payload + headers + aggregateId survives encode/decode", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    await enqueue(
      store,
      "müşteri-😀-π",
      { msg: "merhaba — 你好 — שלום — 🎉" },
      { headers: { "x-tenant": "tüpır", "x-emoji": "🦋" } },
    );
    const [r] = await store.claimBatch(10);
    expect(r?.aggregateId).toBe("müşteri-😀-π");
    expect(r?.payload).toEqual({ msg: "merhaba — 你好 — שלום — 🎉" });
    expect(r?.headers["x-tenant"]).toBe("tüpır");
    expect(r?.headers["x-emoji"]).toBe("🦋");
  });

  it("nulls in payload roundtrip cleanly", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    await enqueue(store, "a1", { a: null, b: undefined, c: [null, 1] });
    const [r] = await store.claimBatch(10);
    expect(r?.payload).toEqual({ a: null, c: [null, 1] });
  });

  // ── markFailed contract ────────────────────────────────────────────────

  it("markFailed increments attempts monotonically across 5 sequential calls", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    await enqueue(store, "a1", { x: 1 });
    const [r] = await store.claimBatch(10);
    const next = new Date(Date.now() + 60_000);
    for (let i = 0; i < 5; i++) {
      await store.markFailed(r!.id, next, "failed");
    }
    const [rows] = await pool.query<({ attempts: number } & RowDataPacket)[]>(
      `SELECT attempts FROM \`${table}\` WHERE id = ?`,
      [r!.id],
    );
    expect(rows[0]?.attempts).toBe(5);
  });

  it("markFailed with status 'dead' stops the row from being reclaimed by the reaper", async () => {
    const table = await freshTable();
    const store = new MysqlStore({ pool, table });
    await enqueue(store, "a1", { x: 1 });
    const [r] = await store.claimBatch(10);
    await store.markFailed(r!.id, null, "dead");
    // Server-side TZ-safe trick: nudge claimed_at far back via UPDATE.
    await pool.query(
      `UPDATE \`${table}\` SET claimed_at = NOW(3) - INTERVAL 1 HOUR WHERE id = ?`,
      [r!.id],
    );
    const reclaimed = await store.claimBatch(10);
    expect(reclaimed).toHaveLength(0);
  });
});
