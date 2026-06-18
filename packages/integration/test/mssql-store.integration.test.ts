import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as mssql from "mssql";
import { MssqlStore, createMigrationSql } from "@eventferry/mssql";
import { newMssqlPool, uniqueName } from "./helpers.js";

// MssqlStore contract against a real SQL Server 2022 testcontainer.
// Mirrors the MySQL / Postgres integration tests so the same invariants
// (head-of-aggregate ordering, SKIP-LOCKED-equivalent concurrent claim,
// reaper window, retry/dead split, BIGINT-as-string id contract) are
// proven on the SQL Server backend with its READPAST + UPDLOCK lock-hint
// recipe.
describe("MssqlStore against real SQL Server 2022", () => {
  let pool: mssql.ConnectionPool;

  beforeAll(async () => {
    pool = await newMssqlPool();
  });

  afterAll(async () => {
    await pool.close();
  });

  // Fresh table per test so describe-block state does not bleed. The store
  // never owns DDL; the migration SQL is multi-statement so it MUST go
  // through Request.batch(), not query() (sp_executesql can't carry
  // session-scoped state across the IF/BEGIN/END blocks).
  async function freshTable(): Promise<string> {
    const table = uniqueName("outbox");
    await pool.request().batch(createMigrationSql(table));
    return table;
  }

  async function enqueue(
    table: string,
    aggregateId: string,
    payload: unknown,
    headers?: Record<string, string>,
  ): Promise<void> {
    const store = new MssqlStore({ pool, table });
    const tx = new mssql.Transaction(pool);
    await tx.begin();
    try {
      await store.enqueue(tx, {
        topic: "orders.created",
        aggregateType: "order",
        aggregateId,
        payload,
        ...(headers ? { headers } : {}),
      });
      await tx.commit();
    } catch (err) {
      // Defensive: surface the real cause if the begin succeeded but the
      // enqueue/commit failed. The driver's ENOTBEGUN would only surface
      // if begin() itself never resolved.
      try {
        await tx.rollback();
      } catch {
        // Already rolled back or never begun — ignore.
      }
      throw err;
    }
  }

  it("enqueues inside a transaction and claims the row", async () => {
    const table = await freshTable();
    await enqueue(table, "a1", { x: 1 });

    const store = new MssqlStore({ pool, table });
    const claimed = await store.claimBatch(10);

    expect(claimed).toHaveLength(1);
    expect(claimed[0]?.aggregateId).toBe("a1");
    expect(claimed[0]?.payload).toEqual({ x: 1 });
    expect(claimed[0]?.status).toBe("processing");
  });

  it("a rolled-back transaction enqueues nothing", async () => {
    const table = await freshTable();
    const store = new MssqlStore({ pool, table });
    const tx = new mssql.Transaction(pool);
    await tx.begin();
    try {
      await store.enqueue(tx, {
        topic: "t",
        aggregateType: "order",
        aggregateId: "a1",
        payload: {},
      });
      await tx.rollback();
    } catch (err) {
      // If enqueue itself threw, the tx may already be aborted. Still try
      // to clean up — rollback on an already-aborted tx is a no-op-ish
      // ENOTBEGUN, swallow it.
      try {
        await tx.rollback();
      } catch {
        /* noop */
      }
      throw err;
    }
    expect(await store.claimBatch(10)).toHaveLength(0);
  });

  it("claimBatch + markDone happy path across multiple aggregates", async () => {
    const table = await freshTable();
    await enqueue(table, "agg-a", { n: 1 });
    await enqueue(table, "agg-b", { n: 2 });
    await enqueue(table, "agg-c", { n: 3 });

    const store = new MssqlStore({ pool, table });
    const claimed = await store.claimBatch(10);

    // Three distinct aggregates → all three are head-of-aggregate and
    // claimable in a single batch.
    expect(claimed).toHaveLength(3);
    const ids = claimed.map((r) => r.id);
    await store.markDone(ids);

    // Verify via direct SELECT — bypassing the store's read path to
    // confirm the row actually transitioned to status=2 (done) on disk.
    const result = await pool
      .request()
      .input("ids", mssql.NVarChar(mssql.MAX), JSON.stringify(ids))
      .query(
        `SELECT id, status FROM [dbo].[${table}]
         WHERE id IN (SELECT j.id FROM OPENJSON(@ids) WITH (id BIGINT '$') AS j)
         ORDER BY id`,
      );
    const rows = result.recordset as Array<{ id: string; status: number }>;
    expect(rows).toHaveLength(3);
    for (const row of rows) {
      expect(row.status).toBe(2);
    }
  });

  it("enforces strict head-of-aggregate ordering", async () => {
    const table = await freshTable();
    await enqueue(table, "agg", { n: 1 });
    await enqueue(table, "agg", { n: 2 });
    await enqueue(table, "agg", { n: 3 });

    const store = new MssqlStore({ pool, table });
    const first = await store.claimBatch(10);
    expect(first).toHaveLength(1); // only the head, NEVER all three
    expect(first[0]?.payload).toEqual({ n: 1 });

    // Successor is blocked by the NOT EXISTS head-of-aggregate guard
    // until the head is acked.
    expect(await store.claimBatch(10)).toHaveLength(0);

    await store.markDone([first[0]!.id]);
    const second = await store.claimBatch(10);
    expect(second).toHaveLength(1);
    expect(second[0]?.payload).toEqual({ n: 2 });

    await store.markDone([second[0]!.id]);
    const third = await store.claimBatch(10);
    expect(third).toHaveLength(1);
    expect(third[0]?.payload).toEqual({ n: 3 });
  });

  it("concurrent claimers never double-claim (READPAST + UPDLOCK SKIP semantics)", async () => {
    const table = await freshTable();
    // 20 distinct aggregates — every row is independently head-of-aggregate
    // so the batch ceiling, not the ordering guard, is what bounds each call.
    for (let i = 0; i < 20; i += 1) {
      await enqueue(table, `agg-${i}`, { n: i });
    }

    const store = new MssqlStore({ pool, table });
    const claimers = await Promise.all([
      store.claimBatch(10),
      store.claimBatch(10),
      store.claimBatch(10),
      store.claimBatch(10),
    ]);

    const allIds = claimers.flat().map((r) => r.id);
    // Total claimed across all four callers must equal 20 (no missed rows).
    expect(allIds).toHaveLength(20);
    // No id may appear twice — the READPAST + UPDLOCK + ROWLOCK hint set
    // is what guarantees this. If a row ever shows up twice here, the
    // claim CTE has regressed and concurrent relays will double-publish.
    expect(new Set(allIds).size).toBe(20);
  });

  it("reclaims a row stuck in processing past the claim timeout (reaper)", async () => {
    const table = await freshTable();
    await enqueue(table, "a1", { x: 1 });
    const store = new MssqlStore({ pool, table });

    const claimed = await store.claimBatch(10);
    expect(claimed).toHaveLength(1);

    // Simulate the owning relay having died two minutes ago. DATEADD with
    // MILLISECOND keeps parity with the store's own reaper expression and
    // dodges the legacy DATETIME 3.33 ms rounding by using DATETIME2(3).
    await pool
      .request()
      .input("id", mssql.BigInt, claimed[0]!.id)
      .query(
        `UPDATE [dbo].[${table}]
         SET    claimed_at = DATEADD(MILLISECOND, -120000, SYSUTCDATETIME())
         WHERE  id = @id`,
      );

    // Default 60s timeout — the row is now 2 minutes "old" so the reaper
    // branch of the claim CTE picks it up.
    const reclaimed = await store.claimBatch(10);
    expect(reclaimed.map((r) => r.id)).toEqual([claimed[0]!.id]);
  });

  it("markFailed with a future retryAt re-surfaces the row after the window opens", async () => {
    const table = await freshTable();
    await enqueue(table, "a1", { x: 1 });
    const store = new MssqlStore({ pool, table });

    const first = await store.claimBatch(10);
    expect(first).toHaveLength(1);
    expect(first[0]?.attempts).toBe(0);

    // 50ms retry window — short enough for the test sleep but non-zero
    // (markFailed throws TypeError on (null, 'failed') to prevent the
    // instant re-drive hot loop).
    await store.markFailed(first[0]!.id, new Date(Date.now() + 50), "failed");

    // Sleep past the retry window. 100ms gives the SYSUTCDATETIME() clock
    // comfortable headroom over the 50ms next_retry_at — flaky-test guard.
    await new Promise((resolve) => setTimeout(resolve, 100));

    const second = await store.claimBatch(10);
    expect(second).toHaveLength(1);
    expect(second[0]?.id).toBe(first[0]!.id);
    // markFailed bumped attempts to 1 (per the contract); the claim itself
    // does NOT increment attempts for a failed→processing transition,
    // only for the reaper's processing→processing path.
    expect(second[0]?.attempts).toBe(1);
  });

  it("markFailed with status=dead is terminal — reaper does not reclaim it", async () => {
    const table = await freshTable();
    await enqueue(table, "a1", { x: 1 });
    const store = new MssqlStore({ pool, table });

    const claimed = await store.claimBatch(10);
    expect(claimed).toHaveLength(1);

    await store.markFailed(claimed[0]!.id, null, "dead");

    // Verify the row is truly status=4 (dead) on disk.
    const result = await pool
      .request()
      .input("id", mssql.BigInt, claimed[0]!.id)
      .query(`SELECT status FROM [dbo].[${table}] WHERE id = @id`);
    const row = (result.recordset as Array<{ status: number }>)[0]!;
    expect(row.status).toBe(4);

    // Wind claimed_at back so the reaper window would otherwise reclaim
    // any processing row — but dead(4) is terminal and must be ignored.
    await pool
      .request()
      .input("id", mssql.BigInt, claimed[0]!.id)
      .query(
        `UPDATE [dbo].[${table}]
         SET    claimed_at = DATEADD(MILLISECOND, -120000, SYSUTCDATETIME())
         WHERE  id = @id`,
      );

    const reclaimed = await store.claimBatch(10);
    expect(reclaimed).toHaveLength(0);
  });

  it("markFailed(null, 'failed') throws TypeError mentioning the hot loop", async () => {
    const table = await freshTable();
    await enqueue(table, "a1", { x: 1 });
    const store = new MssqlStore({ pool, table });

    const claimed = await store.claimBatch(10);
    expect(claimed).toHaveLength(1);

    // The store guards this contract: a null next_retry_at with status
    // 'failed' would let the very next claim cycle re-pick the row,
    // burning CPU in a tight loop until something else changed.
    await expect(
      store.markFailed(claimed[0]!.id, null, "failed"),
    ).rejects.toThrow(/hot loop/);
  });

  it("requeue clears claimed_at but does NOT bump attempts (backpressure path)", async () => {
    const table = await freshTable();
    await enqueue(table, "a1", { x: 1 });
    const store = new MssqlStore({ pool, table });

    const first = await store.claimBatch(10);
    expect(first).toHaveLength(1);
    expect(first[0]?.attempts).toBe(0);

    // Future retryAt so the next claim is gated by next_retry_at, not by
    // an instant re-claim race. requeue is the "slow down" signal — it
    // must NOT consume retry budget.
    await store.requeue(first[0]!.id, new Date(Date.now() + 50));

    await new Promise((resolve) => setTimeout(resolve, 100));

    const second = await store.claimBatch(10);
    expect(second).toHaveLength(1);
    expect(second[0]?.id).toBe(first[0]!.id);
    // Critical: still 0. If this ever becomes >0, requeue has regressed
    // into markFailed and backpressure events will silently exhaust the
    // maxAttempts budget.
    expect(second[0]?.attempts).toBe(0);
  });

  it("purgeDone deletes only done(2) rows and leaves pending ones untouched", async () => {
    const table = await freshTable();
    // 3 rows we will mark done, 2 we leave pending.
    await enqueue(table, "agg-d1", { d: 1 });
    await enqueue(table, "agg-d2", { d: 2 });
    await enqueue(table, "agg-d3", { d: 3 });
    await enqueue(table, "agg-p1", { p: 1 });
    await enqueue(table, "agg-p2", { p: 2 });

    const store = new MssqlStore({ pool, table });

    // Claim and ack the first three — those become status=2.
    const claimed = await store.claimBatch(3);
    expect(claimed).toHaveLength(3);
    await store.markDone(claimed.map((r) => r.id));

    // olderThanMs:0 → cutoff is "now", every done row qualifies.
    const purged = await store.purgeDone({ olderThanMs: 0, batchSize: 10 });
    expect(purged).toBe(3);

    const result = await pool
      .request()
      .query(`SELECT COUNT(*) AS n FROM [dbo].[${table}]`);
    const remaining = (result.recordset as Array<{ n: number }>)[0]!.n;
    expect(remaining).toBe(2);
  });

  it("purgeDone never touches dead(4) rows", async () => {
    const table = await freshTable();
    await enqueue(table, "agg-dead", { d: 1 });
    const store = new MssqlStore({ pool, table });

    const claimed = await store.claimBatch(10);
    expect(claimed).toHaveLength(1);
    await store.markFailed(claimed[0]!.id, null, "dead");

    const purged = await store.purgeDone({ olderThanMs: 0, batchSize: 10 });
    // purgeDone targets ONLY status=2; dead must survive — operators rely
    // on dead-letter rows persisting for forensic / replay workflows.
    expect(purged).toBe(0);

    const result = await pool
      .request()
      .input("id", mssql.BigInt, claimed[0]!.id)
      .query(`SELECT status FROM [dbo].[${table}] WHERE id = @id`);
    const row = (result.recordset as Array<{ status: number }>)[0];
    expect(row?.status).toBe(4);
  });

  it("BIGINT id is returned as a numeric string usable directly in markDone", async () => {
    const table = await freshTable();
    await enqueue(table, "a1", { x: 1 });
    const store = new MssqlStore({ pool, table });

    const claimed = await store.claimBatch(10);
    expect(claimed).toHaveLength(1);
    const id = claimed[0]!.id;

    // tedious always returns BIGINT as a string (see `lib/value-parser.js`:
    // value.toString()) — this is load-bearing for outbox ids that can
    // exceed 2^53 after enough throughput. Number(row.id) would silently
    // lose precision and corrupt markDone / markFailed lookups.
    expect(typeof id).toBe("string");
    expect(/^\d+$/.test(id)).toBe(true);

    // Round-trip: the same string id must work as-is in markDone (which
    // re-validates the /^\d+$/ shape and feeds it into OPENJSON).
    await expect(store.markDone([id])).resolves.toBeUndefined();
  });

  it("JSON payload + headers round-trip with unicode and emoji", async () => {
    const table = await freshTable();
    const payload = { msg: "merhaba — 你好 — שלום — 🎉" };
    const headers = { "x-tenant": "tupir", "x-emoji": "🦋" };
    await enqueue(table, "a1", payload, headers);

    const store = new MssqlStore({ pool, table });
    const claimed = await store.claimBatch(10);
    expect(claimed).toHaveLength(1);

    // Exact equality — tedious hands back NVARCHAR(MAX) as a JS string
    // and rowToRecord JSON.parses it. UCS-2 + UTF-16 surrogate pairs
    // (emoji are outside the BMP) must survive the round-trip with no
    // byte-order or normalization drift.
    expect(claimed[0]?.payload).toEqual(payload);
    expect(claimed[0]?.headers).toEqual(headers);
  });

  it("claimBatch against an empty table returns an empty array", async () => {
    const table = await freshTable();
    const store = new MssqlStore({ pool, table });

    const claimed = await store.claimBatch(10);
    expect(claimed).toEqual([]);
  });

  it("markDone with an empty array is a no-op and does not throw", async () => {
    const table = await freshTable();
    const store = new MssqlStore({ pool, table });

    // The store short-circuits BEFORE building the SQL — verifies that
    // we don't waste a round-trip on OPENJSON('[]'), and that the
    // @@ROWCOUNT === recordIds.length invariant doesn't fire on 0===0.
    await expect(store.markDone([])).resolves.toBeUndefined();
  });
});
