import { describe, expect, it } from "vitest";
import { PostgresStore, type Queryable } from "../src/store.js";
import {
  createMigrationSql,
  createPublicationSql,
  createRetentionIndexSql,
} from "../src/migrations.js";

class FakeDb implements Queryable {
  calls: { text: string; values?: unknown[] }[] = [];
  private responder: (text: string, values?: unknown[]) => Record<string, unknown>[];

  constructor(
    responder: (
      text: string,
      values?: unknown[],
    ) => Record<string, unknown>[] = () => [],
  ) {
    this.responder = responder;
  }

  async query(text: string, values?: unknown[]) {
    this.calls.push({ text, values });
    return { rows: this.responder(text, values) };
  }
}

describe("PostgresStore.enqueue", () => {
  it("inserts with the caller-supplied transaction", async () => {
    const tx = new FakeDb(() => [{ message_id: "abc" }]);
    const store = new PostgresStore({ pool: new FakeDb() });

    const id = await store.enqueue(tx, {
      topic: "orders.created",
      aggregateType: "order",
      aggregateId: "o-1",
      payload: { total: 99 },
    });

    expect(id).toBe("abc");
    expect(tx.calls).toHaveLength(1);
    expect(tx.calls[0]?.text).toContain("INSERT INTO outbox");
    expect(tx.calls[0]?.values?.[1]).toBe("order");
    expect(tx.calls[0]?.values?.[3]).toBe("orders.created");
  });

  it("serializes payload and headers to JSON", async () => {
    const tx = new FakeDb(() => [{ message_id: "x" }]);
    const store = new PostgresStore({ pool: new FakeDb() });
    await store.enqueue(tx, {
      topic: "t",
      aggregateType: "a",
      aggregateId: "1",
      payload: { nested: { k: 1 } },
      headers: { source: "svc" },
    });
    expect(tx.calls[0]?.values?.[5]).toBe('{"nested":{"k":1}}');
    expect(tx.calls[0]?.values?.[6]).toBe('{"source":"svc"}');
  });
});

describe("PostgresStore.claimBatch", () => {
  it("uses FOR UPDATE SKIP LOCKED and maps rows", async () => {
    const pool = new FakeDb(() => [
      {
        id: 7,
        message_id: "m7",
        aggregate_type: "order",
        aggregate_id: "o-7",
        topic: "orders",
        key: null,
        payload: { a: 1 },
        headers: { h: "1" },
        trace_id: "trace-1",
        status: 1,
        attempts: 0,
        next_retry_at: null,
        created_at: new Date("2025-01-01"),
        processed_at: null,
      },
    ]);
    const store = new PostgresStore({ pool });

    const records = await store.claimBatch(10);

    expect(pool.calls[0]?.text).toContain("FOR UPDATE SKIP LOCKED");
    // $1 = batchSize, $2 = claimTimeoutMs (default 60s)
    expect(pool.calls[0]?.values).toEqual([10, 60_000]);
    expect(records).toHaveLength(1);
    expect(records[0]?.id).toBe("7");
    expect(records[0]?.status).toBe("processing");
    expect(records[0]?.traceId).toBe("trace-1");
    expect(records[0]?.headers).toEqual({ h: "1" });
  });

  it("enforces per-aggregate ordering via a head-of-aggregate check", async () => {
    const pool = new FakeDb(() => []);
    const store = new PostgresStore({ pool });
    await store.claimBatch(5);
    const text = pool.calls[0]?.text ?? "";
    // Only claim a row when no earlier (lower id) row for the same aggregate
    // is still unfinished — this is what guarantees ordering.
    expect(text).toContain("NOT EXISTS");
    expect(text).toContain("earlier.aggregate_id = o.aggregate_id");
    expect(text).toContain("earlier.id < o.id");
    expect(text).toContain("ORDER BY o.id");
  });

  it("reclaims rows stuck in processing past the claim timeout (reaper)", async () => {
    const pool = new FakeDb(() => []);
    const store = new PostgresStore({ pool });
    await store.claimBatch(5);
    const text = pool.calls[0]?.text ?? "";
    // status=1 (processing) rows older than claimed_at + timeout are due again.
    expect(text).toContain("o.status = 1");
    expect(text).toContain("claimed_at");
    expect(text).toContain("SET status = 1, claimed_at = now()");
  });

  it("honors a custom claimTimeoutMs", async () => {
    const pool = new FakeDb(() => []);
    const store = new PostgresStore({ pool, claimTimeoutMs: 5_000 });
    await store.claimBatch(3);
    expect(pool.calls[0]?.values).toEqual([3, 5_000]);
  });

  it("claimFailedOnly never claims pending(0) rows (streaming mode)", async () => {
    const pool = new FakeDb(() => []);
    const store = new PostgresStore({ pool, claimFailedOnly: true });
    await store.claimBatch(5);
    const text = pool.calls[0]?.text ?? "";
    expect(text).not.toContain("o.status = 0");
    expect(text).toContain("o.status = 3"); // still retries failed rows
    expect(text).toContain("o.status = 1"); // and reclaims timed-out processing
  });

  it("default claim includes pending(0) rows", async () => {
    const pool = new FakeDb(() => []);
    const store = new PostgresStore({ pool });
    await store.claimBatch(5);
    expect(pool.calls[0]?.text).toContain("o.status = 0");
  });
});

describe("PostgresStore.markDone / markFailed", () => {
  it("markDone is a no-op for empty input", async () => {
    const pool = new FakeDb();
    const store = new PostgresStore({ pool });
    await store.markDone([]);
    expect(pool.calls).toHaveLength(0);
  });

  it("markDone updates with bigint array", async () => {
    const pool = new FakeDb();
    const store = new PostgresStore({ pool });
    await store.markDone(["1", "2"]);
    expect(pool.calls[0]?.text).toContain("bigint[]");
    expect(pool.calls[0]?.values?.[0]).toEqual(["1", "2"]);
  });

  it("markFailed increments attempts and sets status", async () => {
    const pool = new FakeDb();
    const store = new PostgresStore({ pool });
    const retryAt = new Date();
    await store.markFailed("1", retryAt, "failed");
    expect(pool.calls[0]?.text).toContain("attempts = attempts + 1");
    expect(pool.calls[0]?.values).toEqual(["1", 3, retryAt]);
  });
});

describe("table name validation", () => {
  it("rejects unsafe table names", () => {
    expect(
      () => new PostgresStore({ pool: new FakeDb(), table: "outbox; DROP TABLE x" }),
    ).toThrow();
  });

  it("createMigrationSql rejects unsafe names", () => {
    expect(() => createMigrationSql("a'b")).toThrow();
  });

  it("createMigrationSql embeds the table name", () => {
    const sql = createMigrationSql("outbox");
    expect(sql).toContain("CREATE TABLE IF NOT EXISTS outbox");
    expect(sql).toContain("idx_outbox_ready");
    expect(sql).toContain("idx_outbox_agg_order");
  });

  it("createMigrationSql provisions the reaper column and upgrade path", () => {
    const sql = createMigrationSql();
    expect(sql).toMatch(/claimed_at\s+TIMESTAMPTZ/);
    expect(sql).toContain("ADD COLUMN IF NOT EXISTS claimed_at");
  });
});

describe("createPublicationSql", () => {
  it("creates an insert-only publication for the table, idempotently", () => {
    const sql = createPublicationSql("outbox", "outbox_pub");
    expect(sql).toContain("CREATE PUBLICATION outbox_pub");
    expect(sql).toContain("FOR TABLE outbox");
    expect(sql).toContain("publish = 'insert'");
    expect(sql).toContain("pg_publication"); // existence guard for idempotency
  });

  it("embeds custom names", () => {
    const sql = createPublicationSql("orders_outbox", "orders_pub");
    expect(sql).toContain("CREATE PUBLICATION orders_pub");
    expect(sql).toContain("FOR TABLE orders_outbox");
  });

  it("rejects unsafe identifiers", () => {
    expect(() => createPublicationSql("a; DROP")).toThrow();
    expect(() => createPublicationSql("outbox", "p'x")).toThrow();
  });
});

describe("PostgresStore tracing", () => {
  const tracing = {
    inject(carrier: Record<string, string>) {
      carrier.traceparent = "00-abc-def-01";
    },
  };

  it("injects W3C trace context into the stored headers", async () => {
    const tx = new FakeDb(() => [{ message_id: "x" }]);
    const store = new PostgresStore({ pool: new FakeDb(), tracing });
    await store.enqueue(tx, {
      topic: "t",
      aggregateType: "a",
      aggregateId: "1",
      payload: {},
      headers: { source: "svc" },
    });
    const headers = JSON.parse(tx.calls[0]?.values?.[6] as string);
    expect(headers).toEqual({ source: "svc", traceparent: "00-abc-def-01" });
  });

  it("stores headers verbatim when no tracing is configured", async () => {
    const tx = new FakeDb(() => [{ message_id: "x" }]);
    const store = new PostgresStore({ pool: new FakeDb() });
    await store.enqueue(tx, {
      topic: "t",
      aggregateType: "a",
      aggregateId: "1",
      payload: {},
      headers: { source: "svc" },
    });
    expect(tx.calls[0]?.values?.[6]).toBe('{"source":"svc"}');
  });

  it("does not mutate the caller's headers object", async () => {
    const tx = new FakeDb(() => [{ message_id: "x" }]);
    const store = new PostgresStore({ pool: new FakeDb(), tracing });
    const headers = { source: "svc" };
    await store.enqueue(tx, {
      topic: "t",
      aggregateType: "a",
      aggregateId: "1",
      payload: {},
      headers,
    });
    expect(headers).toEqual({ source: "svc" });
  });
});

describe("PostgresStore.purgeDone", () => {
  const rows = (n: number) => Array.from({ length: n }, (_, i) => ({ id: i }));

  it("batched-deletes old done rows and returns the total", async () => {
    let call = 0;
    const pool = new FakeDb(() => {
      call += 1;
      return call === 1 ? rows(1000) : rows(3); // second batch < batchSize -> stop
    });
    const store = new PostgresStore({ pool, table: "outbox" });

    const deleted = await store.purgeDone({ olderThanMs: 86_400_000 });

    expect(deleted).toBe(1003);
    expect(pool.calls).toHaveLength(2);
    const text = pool.calls[0]?.text ?? "";
    expect(text).toContain("DELETE FROM outbox");
    expect(text).toContain("status = 2");
    expect(text).toContain("processed_at");
    expect(text).toContain("RETURNING id");
    expect(pool.calls[0]?.values).toEqual([86_400_000, 1000]); // default batchSize
  });

  it("honors a custom batchSize", async () => {
    const pool = new FakeDb(() => []); // empty -> single iteration
    const store = new PostgresStore({ pool });
    await store.purgeDone({ olderThanMs: 1000, batchSize: 50 });
    expect(pool.calls[0]?.values).toEqual([1000, 50]);
  });

  it("stops once maxRows is reached", async () => {
    const pool = new FakeDb(() => rows(100)); // always a full batch
    const store = new PostgresStore({ pool });
    const deleted = await store.purgeDone({
      olderThanMs: 1000,
      batchSize: 100,
      maxRows: 250,
    });
    expect(deleted).toBe(300); // 3 full batches crosses 250
    expect(pool.calls).toHaveLength(3);
  });
});

describe("createRetentionIndexSql", () => {
  it("creates a partial index over done rows keyed on processed_at", () => {
    const sql = createRetentionIndexSql("outbox");
    expect(sql).toContain("idx_outbox_done_processed");
    expect(sql).toContain("(processed_at)");
    expect(sql).toContain("WHERE status = 2");
  });

  it("rejects unsafe identifiers", () => {
    expect(() => createRetentionIndexSql("a; DROP")).toThrow();
  });
});
