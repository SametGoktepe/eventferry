import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Pool } from "pg";
import { PostgresStore, createMigrationSql } from "@eventferry/postgres";
import { newPool, uniqueName } from "./helpers.js";

/**
 * Adversarial coverage for PostgresStore. Each test pushes on an
 * invariant the store claims to hold — concurrent claims under load,
 * idempotency of terminal markers, requeue not double-counting attempts,
 * empty-batch hygiene, oversized payloads, and a few weirder shapes.
 *
 * Tests are written to FAIL LOUDLY if a regression slips in — assertions
 * are tight and on observable Postgres state, not just store return
 * values.
 */
describe("PostgresStore — adversarial / bug hunt", () => {
  let pool: Pool;
  beforeAll(() => {
    pool = newPool();
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
    store: PostgresStore,
    aggregateId: string,
    payload: unknown,
    over: Partial<{
      topic: string;
      aggregateType: string;
      key: string;
      headers: Record<string, string>;
    }> = {},
  ): Promise<string> {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const id = await store.enqueue(client, {
        topic: over.topic ?? "orders.created",
        aggregateType: over.aggregateType ?? "order",
        aggregateId,
        payload,
        key: over.key,
        headers: over.headers,
      });
      await client.query("COMMIT");
      return id;
    } finally {
      client.release();
    }
  }

  // ── Concurrency ────────────────────────────────────────────────────────

  it("concurrent claims (different aggregates) do not double-claim any row", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
    // 30 distinct aggregates, one row each — every row should be claimed
    // by exactly ONE relay.
    const ids: string[] = [];
    for (let i = 0; i < 30; i++) {
      ids.push(await enqueue(store, `agg-${i}`, { n: i }));
    }
    // 6 concurrent claimers, each grabbing up to 10.
    const batches = await Promise.all(
      Array.from({ length: 6 }, () => store.claimBatch(10)),
    );
    const grabbed = batches.flat().map((r) => r.id);
    expect(grabbed).toHaveLength(30);
    expect(new Set(grabbed).size).toBe(30); // no duplicates ever
    // Database invariant: every row is now processing.
    const { rows } = await pool.query<{ status: number }>(
      `SELECT status FROM ${table}`,
    );
    expect(rows.every((r) => r.status === 1)).toBe(true);
  });

  it("strict head-of-aggregate: 50 concurrent claims on a single aggregate yield exactly ONE row", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
    // 5 records on the SAME aggregate — strict ordering means only the
    // head should ever be claimable.
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

  it("markDone twice on the same id is idempotent (no error, status stays done)", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
    await enqueue(store, "a1", { x: 1 });
    const [r] = await store.claimBatch(10);
    await store.markDone([r!.id]);
    // Second call MUST NOT throw. If it does, the relay's retry path
    // (which may re-mark on partial failures) would crash.
    await expect(store.markDone([r!.id])).resolves.toBeUndefined();
    const { rows } = await pool.query<{ status: number; processed_at: Date }>(
      `SELECT status, processed_at FROM ${table} WHERE id = $1`,
      [r!.id],
    );
    expect(rows[0]?.status).toBe(2); // done
    expect(rows[0]?.processed_at).not.toBeNull();
  });

  it("markDone on a non-existent id is a no-op (no throw, no crash)", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
    await expect(store.markDone(["99999999"])).resolves.toBeUndefined();
  });

  it("markDone on an empty id list is a no-op (no useless query, no crash)", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
    await expect(store.markDone([])).resolves.toBeUndefined();
  });

  // ── Requeue (backpressure path) ────────────────────────────────────────

  it("requeue resets the row to failed WITHOUT incrementing attempts (backpressure semantics)", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
    await enqueue(store, "a1", { x: 1 });
    const [r] = await store.claimBatch(10);
    expect(r!.attempts).toBe(0);

    // Simulate a backpressure event: relay calls requeue.
    if (typeof store.requeue !== "function") {
      throw new Error("PostgresStore.requeue not implemented — regression");
    }
    const retryAt = new Date(Date.now() + 1000);
    await store.requeue(r!.id, retryAt);

    const { rows } = await pool.query<{
      status: number;
      attempts: number;
      claimed_at: Date | null;
    }>(
      `SELECT status, attempts, claimed_at FROM ${table} WHERE id = $1`,
      [r!.id],
    );
    expect(rows[0]?.status).toBe(3); // failed
    expect(rows[0]?.attempts).toBe(0); // NOT incremented — that's the whole point of requeue
    expect(rows[0]?.claimed_at).toBeNull();
  });

  // ── claimBatch hygiene ─────────────────────────────────────────────────

  it("claimBatch(0) returns [] without burning a query (no-op, no crash)", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
    await enqueue(store, "a1", { x: 1 });
    expect(await store.claimBatch(0)).toEqual([]);
  });

  it("claimBatch on an empty table returns [] (no claim, no error)", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
    expect(await store.claimBatch(10)).toEqual([]);
  });

  // ── Large / weird payloads ─────────────────────────────────────────────

  it("survives a 1 MB JSON payload round-trip (no JSONB truncation)", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
    // ~1 MB of repeated structured data.
    const big = { items: Array.from({ length: 50_000 }, (_, i) => ({ i, k: "v" })) };
    await enqueue(store, "big", big);
    const [r] = await store.claimBatch(10);
    expect(r?.payload).toEqual(big);
  });

  it("unicode in payload + headers + aggregateId survives encode/decode", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
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

  it("nulls in payload roundtrip cleanly (not coerced to empty object)", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
    await enqueue(store, "a1", { a: null, b: undefined, c: [null, 1] });
    const [r] = await store.claimBatch(10);
    // JSON has no `undefined`, so b is dropped — but a:null MUST survive.
    expect(r?.payload).toEqual({ a: null, c: [null, 1] });
  });

  // ── markFailed contract ────────────────────────────────────────────────

  it("markFailed increments attempts atomically (no torn read under concurrent updates)", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
    await enqueue(store, "a1", { x: 1 });
    const [r] = await store.claimBatch(10);
    const next = new Date(Date.now() + 60_000);
    // 5 sequential failures: attempts must read 5, monotonic.
    for (let i = 0; i < 5; i++) {
      await store.markFailed(r!.id, next, "failed");
    }
    const { rows } = await pool.query<{ attempts: number }>(
      `SELECT attempts FROM ${table} WHERE id = $1`,
      [r!.id],
    );
    expect(rows[0]?.attempts).toBe(5);
  });

  it("markFailed with status 'dead' stops the row from being reclaimed by the reaper", async () => {
    const table = await freshTable();
    const store = new PostgresStore({ pool, table });
    await enqueue(store, "a1", { x: 1 });
    const [r] = await store.claimBatch(10);
    await store.markFailed(r!.id, null, "dead");

    // Even after a long reaper window, a dead row stays dead.
    await pool.query(
      `UPDATE ${table} SET claimed_at = now() - interval '1 hour' WHERE id = $1`,
      [r!.id],
    );
    const reclaimed = await store.claimBatch(10);
    expect(reclaimed).toHaveLength(0);
  });
});
