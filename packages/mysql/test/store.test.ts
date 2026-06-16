import { describe, expect, it } from "vitest";
import {
  MysqlStore,
  type MysqlConnection,
  type MysqlPool,
  type MysqlQueryable,
} from "../src/store.js";
import {
  createMigrationSql,
  createRetentionIndexSql,
} from "../src/migrations.js";

type Responder = (sql: string, values?: unknown[]) => unknown;

const NULL_ROWS: unknown[] = [];

class FakeQ implements MysqlQueryable {
  calls: { sql: string; values?: unknown[] }[] = [];
  protected responder: Responder;

  constructor(responder: Responder = () => NULL_ROWS) {
    this.responder = responder;
  }

  async query(sql: string, values?: unknown[]): Promise<[unknown, unknown]> {
    this.calls.push({ sql, values });
    return [this.responder(sql, values), undefined];
  }
}

class FakeConnection extends FakeQ implements MysqlConnection {
  events: string[] = [];
  released = false;
  async beginTransaction() {
    this.events.push("begin");
  }
  async commit() {
    this.events.push("commit");
  }
  async rollback() {
    this.events.push("rollback");
  }
  release() {
    this.released = true;
  }
}

class FakePool extends FakeQ implements MysqlPool {
  readonly connection: FakeConnection;
  constructor(opts: { connectionResponder?: Responder; poolResponder?: Responder } = {}) {
    super(opts.poolResponder);
    this.connection = new FakeConnection(opts.connectionResponder);
  }
  async getConnection(): Promise<MysqlConnection> {
    return this.connection;
  }
}

describe("MysqlStore.enqueue", () => {
  it("inserts with the caller-supplied transaction (not the pool)", async () => {
    const tx = new FakeQ();
    const pool = new FakePool();
    const store = new MysqlStore({ pool });

    await store.enqueue(tx, {
      topic: "orders.created",
      aggregateType: "order",
      aggregateId: "o-1",
      payload: { total: 99 },
    });

    expect(tx.calls).toHaveLength(1);
    expect(pool.calls).toHaveLength(0); // never touched the pool directly
    expect(tx.calls[0]?.sql).toContain("INSERT INTO `outbox`");
    expect(tx.calls[0]?.values?.[1]).toBe("order"); // aggregate_type
    expect(tx.calls[0]?.values?.[3]).toBe("orders.created"); // topic
  });

  it("serializes payload and headers to JSON strings", async () => {
    const tx = new FakeQ();
    const store = new MysqlStore({ pool: new FakePool() });
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

  it("generates a uuid v4 message_id when none is supplied", async () => {
    const tx = new FakeQ();
    const store = new MysqlStore({ pool: new FakePool() });
    const id = await store.enqueue(tx, {
      topic: "t",
      aggregateType: "a",
      aggregateId: "1",
      payload: {},
    });
    expect(id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(tx.calls[0]?.values?.[0]).toBe(id);
  });

  it("uses the caller-supplied message_id verbatim when given", async () => {
    const tx = new FakeQ();
    const store = new MysqlStore({ pool: new FakePool() });
    const id = await store.enqueue(tx, {
      messageId: "11111111-1111-4111-8111-111111111111",
      topic: "t",
      aggregateType: "a",
      aggregateId: "1",
      payload: {},
    });
    expect(id).toBe("11111111-1111-4111-8111-111111111111");
    expect(tx.calls[0]?.values?.[0]).toBe(id);
  });

  it("captures trace context via the tracing hook when supplied", async () => {
    const tx = new FakeQ();
    const store = new MysqlStore({
      pool: new FakePool(),
      tracing: {
        inject(carrier) {
          carrier["traceparent"] = "00-aaaa-bbbb-01";
        },
      },
    });
    await store.enqueue(tx, {
      topic: "t",
      aggregateType: "a",
      aggregateId: "1",
      payload: {},
    });
    const headersJson = tx.calls[0]?.values?.[6] as string;
    expect(JSON.parse(headersJson)).toEqual({ traceparent: "00-aaaa-bbbb-01" });
  });

  it("does not mutate the caller's headers object", async () => {
    const tx = new FakeQ();
    const callerHeaders = { source: "svc" };
    const store = new MysqlStore({
      pool: new FakePool(),
      tracing: {
        inject(carrier) {
          carrier["traceparent"] = "00-x-y-01";
        },
      },
    });
    await store.enqueue(tx, {
      topic: "t",
      aggregateType: "a",
      aggregateId: "1",
      payload: {},
      headers: callerHeaders,
    });
    expect(callerHeaders).toEqual({ source: "svc" }); // unchanged
  });

  it("honors a custom table name (quoted with backticks)", async () => {
    const tx = new FakeQ();
    const store = new MysqlStore({ pool: new FakePool(), table: "my_outbox" });
    await store.enqueue(tx, {
      topic: "t",
      aggregateType: "a",
      aggregateId: "1",
      payload: {},
    });
    expect(tx.calls[0]?.sql).toContain("INSERT INTO `my_outbox`");
  });
});

describe("MysqlStore constructor", () => {
  it("rejects an unsafe table identifier", () => {
    expect(
      () =>
        new MysqlStore({
          pool: new FakePool(),
          table: "outbox; DROP TABLE users;--",
        }),
    ).toThrow(/Invalid identifier/);
  });
});

describe("MysqlStore.claimBatch", () => {
  const sampleRow = {
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
    created_at: new Date("2026-01-01"),
    processed_at: null,
  };

  function claimResponder(due: Array<{ id: number }>, rows: unknown[]): Responder {
    return (sql) => {
      if (/^\s*SELECT\s+o\.id/i.test(sql)) return due;
      if (/^\s*UPDATE\s/i.test(sql)) return { affectedRows: due.length };
      if (/^\s*SELECT\s+id,\s*message_id/i.test(sql)) return rows;
      return NULL_ROWS;
    };
  }

  it("uses FOR UPDATE SKIP LOCKED with the head-of-aggregate guard", async () => {
    const pool = new FakePool({
      connectionResponder: claimResponder([{ id: 7 }], [sampleRow]),
    });
    const store = new MysqlStore({ pool });
    const out = await store.claimBatch(10);

    expect(out).toHaveLength(1);
    expect(out[0]?.id).toBe("7");
    expect(out[0]?.aggregateId).toBe("o-7");

    // First call inside the connection must be the SELECT ... FOR UPDATE SKIP LOCKED
    // with the NOT EXISTS earlier-unfinished guard.
    const first = pool.connection.calls[0]?.sql ?? "";
    expect(first).toMatch(/FOR UPDATE SKIP LOCKED/);
    expect(first).toMatch(/NOT EXISTS/);
    expect(first).toMatch(/earlier\.aggregate_id = o\.aggregate_id/);
    expect(first).toMatch(/earlier\.status IN \(0, 1, 3\)/);
  });

  it("opens a transaction, commits on success, and releases the connection", async () => {
    const pool = new FakePool({
      connectionResponder: claimResponder([{ id: 7 }], [sampleRow]),
    });
    const store = new MysqlStore({ pool });
    await store.claimBatch(10);

    expect(pool.connection.events).toEqual(["begin", "commit"]);
    expect(pool.connection.released).toBe(true);
  });

  it("rolls back and releases the connection when a step throws", async () => {
    const pool = new FakePool({
      connectionResponder: (sql) => {
        if (/^\s*SELECT\s+o\.id/i.test(sql)) return [{ id: 7 }];
        if (/^\s*UPDATE\s/i.test(sql)) throw new Error("boom");
        return NULL_ROWS;
      },
    });
    const store = new MysqlStore({ pool });
    await expect(store.claimBatch(10)).rejects.toThrow(/boom/);
    expect(pool.connection.events).toEqual(["begin", "rollback"]);
    expect(pool.connection.released).toBe(true);
  });

  it("returns an empty array (and skips UPDATE) when no rows are due", async () => {
    const pool = new FakePool({
      connectionResponder: claimResponder([], []),
    });
    const store = new MysqlStore({ pool });
    const out = await store.claimBatch(10);

    expect(out).toEqual([]);
    // Only the SELECT due query; no UPDATE, no SELECT-back.
    expect(pool.connection.calls).toHaveLength(1);
    expect(pool.connection.events).toEqual(["begin", "commit"]);
  });

  it("excludes pending(0) rows when claimFailedOnly is set", async () => {
    const pool = new FakePool({
      connectionResponder: claimResponder([], []),
    });
    const store = new MysqlStore({ pool, claimFailedOnly: true });
    await store.claimBatch(10);

    const first = pool.connection.calls[0]?.sql ?? "";
    // The pendingClause `o.status = 0 OR ` must not appear when claimFailedOnly.
    expect(first).not.toMatch(/o\.status = 0 OR/);
    // The failed(3) branch must still be there.
    expect(first).toMatch(/o\.status = 3/);
  });

  it("passes batchSize as the LIMIT parameter", async () => {
    const pool = new FakePool({
      connectionResponder: claimResponder([], []),
    });
    const store = new MysqlStore({ pool });
    await store.claimBatch(42);

    const values = pool.connection.calls[0]?.values;
    expect(values?.[1]).toBe(42); // [reaperCutoff, batchSize]
  });
});

describe("MysqlStore.markDone", () => {
  it("is a no-op when given an empty list", async () => {
    const pool = new FakePool();
    const store = new MysqlStore({ pool });
    await store.markDone([]);
    expect(pool.calls).toHaveLength(0);
  });

  it("flips status to done(2) and stamps processed_at", async () => {
    const pool = new FakePool({ poolResponder: () => ({ affectedRows: 2 }) });
    const store = new MysqlStore({ pool });
    await store.markDone(["1", "2"]);
    const call = pool.calls[0];
    expect(call?.sql).toMatch(/UPDATE\s+`outbox`/);
    expect(call?.sql).toMatch(/status = 2/);
    expect(call?.sql).toMatch(/processed_at = NOW\(3\)/);
    expect(call?.values?.[0]).toEqual(["1", "2"]);
  });
});

describe("MysqlStore.markFailed", () => {
  it("increments attempts and sets next_retry_at + status", async () => {
    const pool = new FakePool({ poolResponder: () => ({ affectedRows: 1 }) });
    const store = new MysqlStore({ pool });
    const retryAt = new Date("2026-06-17T10:00:00Z");
    await store.markFailed("42", retryAt, "failed");

    const call = pool.calls[0];
    expect(call?.sql).toMatch(/attempts = attempts \+ 1/);
    expect(call?.values).toEqual([3, retryAt, "42"]); // 3 = failed
  });

  it("uses dead(4) when status='dead'", async () => {
    const pool = new FakePool({ poolResponder: () => ({ affectedRows: 1 }) });
    const store = new MysqlStore({ pool });
    await store.markFailed("42", null, "dead");
    expect(pool.calls[0]?.values?.[0]).toBe(4);
    expect(pool.calls[0]?.values?.[1]).toBe(null);
  });
});

describe("MysqlStore.purgeDone", () => {
  it("loops in batchSize chunks until a short batch comes back", async () => {
    let remaining = 2500;
    const pool = new FakePool({
      poolResponder: (_sql, values) => {
        const limit = values?.[1] as number;
        const took = Math.min(limit, remaining);
        remaining -= took;
        return { affectedRows: took };
      },
    });
    const store = new MysqlStore({ pool });
    const total = await store.purgeDone({ olderThanMs: 1000, batchSize: 1000 });
    expect(total).toBe(2500);
    // 1000 + 1000 + 500 = three iterations, with the last being the short batch.
    expect(pool.calls).toHaveLength(3);
  });

  it("respects maxRows as an upper bound", async () => {
    const pool = new FakePool({
      poolResponder: () => ({ affectedRows: 1000 }),
    });
    const store = new MysqlStore({ pool });
    const total = await store.purgeDone({
      olderThanMs: 1000,
      batchSize: 1000,
      maxRows: 1500,
    });
    // After the second batch, total=2000 which is >= maxRows(1500), so we stop.
    expect(total).toBeGreaterThanOrEqual(1500);
    expect(pool.calls.length).toBeLessThanOrEqual(2);
  });

  it("uses DELETE ... ORDER BY id LIMIT against done rows only", async () => {
    const pool = new FakePool({ poolResponder: () => ({ affectedRows: 0 }) });
    const store = new MysqlStore({ pool });
    await store.purgeDone({ olderThanMs: 1000, batchSize: 100 });
    const sql = pool.calls[0]?.sql ?? "";
    expect(sql).toMatch(/DELETE FROM `outbox`/);
    expect(sql).toMatch(/status = 2/);
    expect(sql).toMatch(/ORDER BY id/);
    expect(sql).toMatch(/LIMIT \?/);
  });
});

describe("createMigrationSql", () => {
  it("uses InnoDB, utf8mb4, DATETIME(3), and JSON columns", () => {
    const sql = createMigrationSql();
    expect(sql).toContain("CREATE TABLE IF NOT EXISTS `outbox`");
    expect(sql).toContain("ENGINE=InnoDB");
    expect(sql).toContain("DEFAULT CHARSET=utf8mb4");
    expect(sql).toContain("payload         JSON NOT NULL");
    expect(sql).toContain("created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)");
  });

  it("emits the three indexes (ready, agg_order, uq message_id)", () => {
    const sql = createMigrationSql();
    expect(sql).toContain("UNIQUE KEY uq_outbox_message_id (message_id)");
    expect(sql).toContain("KEY idx_outbox_ready (status, id)");
    expect(sql).toContain("KEY idx_outbox_agg_order (aggregate_id, id)");
  });

  it("interpolates a custom table name into all identifiers", () => {
    const sql = createMigrationSql("events_outbox");
    expect(sql).toContain("CREATE TABLE IF NOT EXISTS `events_outbox`");
    expect(sql).toContain("uq_events_outbox_message_id");
    expect(sql).toContain("idx_events_outbox_ready");
    expect(sql).toContain("idx_events_outbox_agg_order");
  });

  it("rejects an unsafe table identifier", () => {
    expect(() => createMigrationSql("foo; DROP TABLE x;")).toThrow(/Invalid identifier/);
  });
});

describe("createRetentionIndexSql", () => {
  it("creates an index over processed_at on the right table", () => {
    expect(createRetentionIndexSql()).toBe(
      "CREATE INDEX idx_outbox_done_processed ON `outbox` (processed_at);",
    );
  });
});
