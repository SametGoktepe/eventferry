import { describe, expect, it, vi } from "vitest";

// --------------------------------------------------------------------------
// Fake mssql surface
// --------------------------------------------------------------------------
//
// The store only touches:
//   - new mssql.Request(tx)
//   - pool.request()
//   - request.input(name, type, value)
//   - request.query(sql)  /  request.batch(sql)
//   - sql.NVarChar(len) / sql.NVarChar(sql.MAX)
//   - sql.Int / sql.BigInt / sql.TinyInt / sql.DateTime2(prec)
//   - sql.MAX (sentinel passed into NVarChar)
//   - mssql.Transaction is a *type* import only (and a class for `tx
//     instanceof mssql.Transaction` is never used inside the store) —
//     the fake just needs to be a shaped object with the documented
//     internal `_acquiredConnection` field used by assertTransactionBegun.
//
// Everything else (connection pooling, TDS, cancellation tokens, output
// parameters) is out of band: the store never calls into it, and the
// integration suite owns the real-driver coverage.

type FakeQueryResult = {
  recordset?: unknown[];
  recordsets?: unknown[][];
  rowsAffected?: number[];
  output?: Record<string, unknown>;
};

type FakeCall = { kind: "query" | "batch"; sql: string };

class FakeRequest {
  public readonly inputs: Map<string, { type: unknown; value: unknown }> =
    new Map();
  public readonly calls: FakeCall[] = [];
  public lastSql: string | null = null;
  /** Programmed responses (FIFO). When exhausted, returns the default. */
  private responses: FakeQueryResult[] = [];
  private defaultResponse: FakeQueryResult = {
    recordset: [],
    recordsets: [[]],
    rowsAffected: [0],
  };

  setResponses(responses: FakeQueryResult[]): void {
    this.responses = [...responses];
  }
  setDefaultResponse(r: FakeQueryResult): void {
    this.defaultResponse = r;
  }

  input(name: string, type: unknown, value: unknown): this {
    this.inputs.set(name, { type, value });
    return this;
  }

  async query(sql: string): Promise<FakeQueryResult> {
    this.calls.push({ kind: "query", sql });
    this.lastSql = sql;
    return this.nextResponse();
  }

  async batch(sql: string): Promise<FakeQueryResult> {
    this.calls.push({ kind: "batch", sql });
    this.lastSql = sql;
    return this.nextResponse();
  }

  private nextResponse(): FakeQueryResult {
    return this.responses.shift() ?? this.defaultResponse;
  }
}

class FakeTransaction {
  /** Tracks the lifecycle so tests can assert begin/commit/rollback ordering. */
  public events: string[] = [];
  /** mssql's documented internal field set by `await tx.begin()`. */
  public _acquiredConnection: unknown = null;
  public _aborted = false;

  /** Last `new mssql.Request(tx)` against this transaction; the store
   *  only ever creates ONE request per enqueue, so we keep just the most
   *  recent for assertion. */
  public lastRequest: FakeRequest | null = null;

  async begin(): Promise<void> {
    this.events.push("begin");
    this._acquiredConnection = {}; // truthy = bound to a connection
  }
  async commit(): Promise<void> {
    this.events.push("commit");
  }
  async rollback(): Promise<void> {
    this.events.push("rollback");
    this._aborted = true;
  }
}

class FakePool {
  /** Requests minted by `pool.request()`, in order. */
  public readonly requests: FakeRequest[] = [];
  /** Each call to `.request()` pulls the next programmed response, if any.
   *  This is how tests script per-call recordsets (e.g. purgeDone loop). */
  private programmedResponses: FakeQueryResult[] = [];
  private defaultResponse: FakeQueryResult = {
    recordset: [],
    recordsets: [[]],
    rowsAffected: [0],
  };

  programResponses(responses: FakeQueryResult[]): void {
    this.programmedResponses = [...responses];
  }
  setDefaultResponse(r: FakeQueryResult): void {
    this.defaultResponse = r;
  }

  request(): FakeRequest {
    const req = new FakeRequest();
    // Each pool-minted request fires exactly one query/batch, so we hand
    // it a single programmed response (its FIFO) and let it fall back to
    // the default thereafter.
    const next = this.programmedResponses.shift();
    if (next !== undefined) req.setResponses([next]);
    req.setDefaultResponse(this.defaultResponse);
    this.requests.push(req);
    return req;
  }

  async connect(): Promise<void> {
    /* no-op */
  }
  async close(): Promise<void> {
    /* no-op */
  }
}

// --------------------------------------------------------------------------
// Mock the mssql module
// --------------------------------------------------------------------------
//
// vi.mock is hoisted before the imports below.  All factories used inside
// `vi.mock` MUST be defined inline (no out-of-scope variable capture other
// than for symbols vitest understands).  We therefore make the type stubs
// self-contained — they are plain sentinels with .name fields so tests can
// match on identity (`expect(inputs.get('id').type).toBe(sql.BigInt)`).

vi.mock("mssql", () => {
  const NVarCharType = (len: number | unknown) => ({
    kind: "NVarChar",
    length: len,
  });
  const DateTime2Type = (precision: number) => ({
    kind: "DateTime2",
    precision,
  });
  const sql = {
    NVarChar: NVarCharType,
    DateTime2: DateTime2Type,
    Int: { kind: "Int" },
    BigInt: { kind: "BigInt" },
    TinyInt: { kind: "TinyInt" },
    MAX: "MAX" as const,
    // Request: when constructed with a FakeTransaction (or anything having
    // a `lastRequest` slot), it mints a fresh FakeRequest and attaches it
    // so the test can later inspect inputs + captured SQL.
    Request: class MockRequest {
      constructor(txOrPool: unknown) {
        const fake = new FakeRequest();
        // Hide the implementation: callers see a FakeRequest API, so we
        // proxy all the methods on `this`.
        const target = txOrPool as { lastRequest?: FakeRequest } | null;
        if (target !== null && typeof target === "object") {
          target.lastRequest = fake;
        }
        // Copy the FakeRequest methods onto `this` so the store's calls
        // (request.input / request.batch / request.query) reach the fake.
        (this as unknown as { input: typeof fake.input }).input =
          fake.input.bind(fake);
        (this as unknown as { query: typeof fake.query }).query =
          fake.query.bind(fake);
        (this as unknown as { batch: typeof fake.batch }).batch =
          fake.batch.bind(fake);
        // Also expose the raw fake for any test that needs it directly
        // off the Request instance.
        (this as unknown as { _fake: FakeRequest })._fake = fake;
      }
    },
  };
  return { ...sql, default: sql };
});

// Imports must come AFTER vi.mock so the mock is wired into the store's
// `import * as mssql from "mssql"`.
import { MssqlStore } from "../src/store.js";

// Convenience: build a transaction that has already been "begun".
function begunTx(): FakeTransaction {
  const tx = new FakeTransaction();
  tx._acquiredConnection = {};
  return tx;
}

describe("MssqlStore constructor", () => {
  it("applies defaults (claimTimeoutMs=60_000, schema='dbo', table='outbox', claimFailedOnly=false)", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({
      pool: pool as unknown as never,
    });
    // Drive claimBatch to observe the SQL + timeout binding.
    await store.claimBatch(5);
    const req = pool.requests[0]!;
    expect(req.inputs.get("claimTimeoutMs")?.value).toBe(60_000);
    // default schema/table are `[dbo].[outbox]`
    expect(req.lastSql).toContain("[dbo].[outbox]");
    // claimFailedOnly=false => pending(0) clause present
    expect(req.lastSql).toContain("o.status = 0");
  });

  it("rejects an unsafe table name (assertIdent)", () => {
    const pool = new FakePool();
    expect(
      () =>
        new MssqlStore({
          pool: pool as unknown as never,
          table: "outbox'; DROP TABLE x;--",
        }),
    ).toThrow(/invalid SQL identifier/);
  });

  it("rejects an unsafe schema name (assertIdent)", () => {
    const pool = new FakePool();
    expect(
      () =>
        new MssqlStore({
          pool: pool as unknown as never,
          schema: "dbo.bad",
        }),
    ).toThrow(/invalid SQL identifier/);
  });

  it("clamps claimTimeoutMs > 86_400_000 and warns via the supplied logger", async () => {
    const pool = new FakePool();
    const warn = vi.fn();
    const logger = { info: vi.fn(), warn, error: vi.fn(), debug: vi.fn() };
    const store = new MssqlStore({
      pool: pool as unknown as never,
      claimTimeoutMs: 90_000_000, // > 24h
      logger,
    });
    expect(warn).toHaveBeenCalledTimes(1);
    const [message, ctx] = warn.mock.calls[0]!;
    expect(message).toMatch(/clamping/);
    expect(ctx).toEqual({ requested: 90_000_000, clamped: 86_400_000 });

    await store.claimBatch(1);
    expect(pool.requests[0]?.inputs.get("claimTimeoutMs")?.value).toBe(
      86_400_000,
    );
  });

  it("rejects non-positive / non-finite claimTimeoutMs", () => {
    const pool = new FakePool();
    expect(
      () =>
        new MssqlStore({
          pool: pool as unknown as never,
          claimTimeoutMs: 0,
        }),
    ).toThrow(TypeError);
    expect(
      () =>
        new MssqlStore({
          pool: pool as unknown as never,
          claimTimeoutMs: Number.POSITIVE_INFINITY,
        }),
    ).toThrow(TypeError);
  });

  it("rejects a missing pool", () => {
    expect(
      () => new MssqlStore({ pool: undefined as unknown as never }),
    ).toThrow(/pool is required/);
  });
});

describe("MssqlStore.enqueue", () => {
  it("throws when caller-supplied tx has not had .begin() called", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    const tx = new FakeTransaction(); // begin() NOT called
    await expect(
      store.enqueue(tx as unknown as never, {
        topic: "t",
        aggregateType: "a",
        aggregateId: "1",
        payload: {},
      }),
    ).rejects.toThrow(/tx\.begin\(\) has not been called/);
  });

  it("throws when tx was begun then aborted", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    const tx = begunTx();
    tx._aborted = true;
    await expect(
      store.enqueue(tx as unknown as never, {
        topic: "t",
        aggregateType: "a",
        aggregateId: "1",
        payload: {},
      }),
    ).rejects.toThrow(/aborted/);
  });

  it("throws TypeError when tx is undefined/null", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    await expect(
      store.enqueue(null as unknown as never, {
        topic: "t",
        aggregateType: "a",
        aggregateId: "1",
        payload: {},
      }),
    ).rejects.toThrow(/tx \(mssql\.Transaction\) is required/);
  });

  it("happy path returns the message_id from the OUTPUT recordset", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    const tx = begunTx();

    // Pre-arm: when the store calls request.batch(...), the mock Request
    // has already been minted by `new mssql.Request(tx)`. We program the
    // response on tx.lastRequest *after* the call lands — but since the
    // call is async we instead pre-wire by patching the Request's
    // default after construction. The simplest path is to do the call
    // and then intercept via the FakeRequest mounted on tx.lastRequest:
    // that fake's response is the default (recordsets: [[]]).
    //
    // For determinism, set a microtask-safe queue using the tx's fake
    // once it exists. We do this by overriding the global Request mock
    // for this test: when constructed, set its response.
    //
    // Simpler approach: monkey-patch tx.lastRequest right before invoking
    // enqueue by reaching through the mocked Request constructor. We do
    // that here by precomputing the desired recordsets and intercepting
    // through a one-shot Object.defineProperty on tx.
    const expectedId = "11111111-1111-4111-8111-111111111111";

    // Programmatic response: monkey-patch begin so that the next call
    // to `new mssql.Request(tx)` already has a programmed response by
    // overriding the FakeRequest.prototype.batch for THIS tx only.
    Object.defineProperty(tx, "_armedResponse", {
      value: { recordsets: [[{ message_id: expectedId }]] },
      writable: true,
    });
    // Hook: after enqueue mints the Request, before batch returns,
    // we install the programmed response. The mocked Request stores
    // itself on tx.lastRequest synchronously inside its constructor,
    // so we can intercept with a microtask.
    const installed = installResponseOnNextRequest(tx, {
      recordsets: [[{ message_id: expectedId }]],
    });

    const id = await store.enqueue(tx as unknown as never, {
      topic: "orders.created",
      aggregateType: "order",
      aggregateId: "o-1",
      payload: { total: 99 },
    });
    expect(id).toBe(expectedId);
    installed.restore();

    const req = tx.lastRequest!;
    // The store uses request.batch(...) (multi-statement)
    expect(req.calls[0]?.kind).toBe("batch");
    expect(req.lastSql).toContain("INSERT INTO [dbo].[outbox]");
    // Pool was never touched directly
    expect(pool.requests).toHaveLength(0);
  });

  it("does NOT mutate the caller's headers object", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({
      pool: pool as unknown as never,
      tracing: {
        inject(carrier: Record<string, string>) {
          carrier["traceparent"] = "00-aaaa-bbbb-01";
        },
      },
    });
    const tx = begunTx();
    const callerHeaders = { "x-tenant": "t1" };
    const installed = installResponseOnNextRequest(tx, {
      recordsets: [[{ message_id: "m" }]],
    });
    await store.enqueue(tx as unknown as never, {
      topic: "t",
      aggregateType: "a",
      aggregateId: "1",
      payload: {},
      headers: callerHeaders,
    });
    installed.restore();
    // Identity + content unchanged
    expect(callerHeaders).toEqual({ "x-tenant": "t1" });
    expect(Object.keys(callerHeaders)).toEqual(["x-tenant"]);
  });

  it("invokes tracing.inject on a COPY of headers (caller object untouched)", async () => {
    const pool = new FakePool();
    let injectedCarrier: Record<string, string> | null = null;
    const callerHeaders = { "x-tenant": "t1" };
    const store = new MssqlStore({
      pool: pool as unknown as never,
      tracing: {
        inject(carrier: Record<string, string>) {
          injectedCarrier = carrier;
          carrier["traceparent"] = "00-aa-bb-01";
        },
      },
    });
    const tx = begunTx();
    const installed = installResponseOnNextRequest(tx, {
      recordsets: [[{ message_id: "m" }]],
    });
    await store.enqueue(tx as unknown as never, {
      topic: "t",
      aggregateType: "a",
      aggregateId: "1",
      payload: {},
      headers: callerHeaders,
    });
    installed.restore();

    expect(injectedCarrier).not.toBeNull();
    // Different reference from the caller's object
    expect(injectedCarrier).not.toBe(callerHeaders);
    // The injected carrier has the trace + the original entry
    expect(injectedCarrier).toEqual({
      "x-tenant": "t1",
      traceparent: "00-aa-bb-01",
    });
    // Caller's headers stayed pristine
    expect(callerHeaders).toEqual({ "x-tenant": "t1" });

    // And the serialized headers landed in the request input
    const headersInput = tx.lastRequest?.inputs.get("headers")?.value as string;
    expect(JSON.parse(headersInput)).toEqual({
      "x-tenant": "t1",
      traceparent: "00-aa-bb-01",
    });
  });

  it("SQL contains COALESCE(@messageId, ...NEWID()...) — caller-supplied messageId wins", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    const tx = begunTx();
    const installed = installResponseOnNextRequest(tx, {
      recordsets: [[{ message_id: "caller-mid" }]],
    });
    const id = await store.enqueue(tx as unknown as never, {
      messageId: "caller-mid",
      topic: "t",
      aggregateType: "a",
      aggregateId: "1",
      payload: {},
    });
    installed.restore();

    expect(id).toBe("caller-mid");
    const req = tx.lastRequest!;
    expect(req.lastSql).toContain("COALESCE(@messageId");
    expect(req.lastSql).toMatch(/NEWID\(\)/);
    expect(req.inputs.get("messageId")?.value).toBe("caller-mid");
  });

  it("passes null @messageId when caller omits it (server mints)", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    const tx = begunTx();
    const installed = installResponseOnNextRequest(tx, {
      recordsets: [[{ message_id: "server-minted" }]],
    });
    await store.enqueue(tx as unknown as never, {
      topic: "t",
      aggregateType: "a",
      aggregateId: "1",
      payload: {},
    });
    installed.restore();
    expect(tx.lastRequest?.inputs.get("messageId")?.value).toBeNull();
  });

  it("throws when OUTPUT clause comes back empty (driver corruption)", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    const tx = begunTx();
    const installed = installResponseOnNextRequest(tx, {
      recordsets: [[]],
    });
    await expect(
      store.enqueue(tx as unknown as never, {
        topic: "t",
        aggregateType: "a",
        aggregateId: "1",
        payload: {},
      }),
    ).rejects.toThrow(/did not return a message_id/);
    installed.restore();
  });
});

describe("MssqlStore.claimBatch", () => {
  it("binds @batchSize (Int) and @claimTimeoutMs (Int) and includes the lock hints", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    await store.claimBatch(10);

    const req = pool.requests[0]!;
    const sqlText = req.lastSql ?? "";

    // Inputs: types matter for SQL Server.
    const batchInput = req.inputs.get("batchSize");
    const timeoutInput = req.inputs.get("claimTimeoutMs");
    expect(batchInput?.value).toBe(10);
    expect(batchInput?.type).toEqual({ kind: "Int" });
    expect(timeoutInput?.value).toBe(60_000);
    expect(timeoutInput?.type).toEqual({ kind: "Int" });

    // Lock hint composition on the claim CTE.
    expect(sqlText).toContain("READCOMMITTEDLOCK");
    expect(sqlText).toContain("READPAST");
    expect(sqlText).toContain("UPDLOCK");
    expect(sqlText).toContain("ROWLOCK");
    // Tx wrapping.
    expect(sqlText).toContain("BEGIN TRAN");
    expect(sqlText).toContain("COMMIT");

    // Drives through .batch() (multi-statement)
    expect(req.calls[0]?.kind).toBe("batch");
  });

  it("includes pending(0) predicate when claimFailedOnly=false (default)", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    await store.claimBatch(5);
    const sqlText = pool.requests[0]?.lastSql ?? "";
    expect(sqlText).toContain("o.status = 0");
    expect(sqlText).toContain("o.status = 3");
    expect(sqlText).toContain("o.status = 1");
  });

  it("omits pending(0) predicate when claimFailedOnly=true (streaming relay mode)", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({
      pool: pool as unknown as never,
      claimFailedOnly: true,
    });
    await store.claimBatch(5);
    const sqlText = pool.requests[0]?.lastSql ?? "";
    expect(sqlText).not.toContain("o.status = 0");
    expect(sqlText).toContain("o.status = 3");
    expect(sqlText).toContain("o.status = 1");
  });

  it("empty recordset returns [] without crashing", async () => {
    const pool = new FakePool();
    pool.programResponses([{ recordsets: [[]] }]);
    const store = new MssqlStore({ pool: pool as unknown as never });
    const out = await store.claimBatch(10);
    expect(out).toEqual([]);
  });

  it("maps recordset rows through rowToRecord (BIGINT stays string, JSON parsed, status decoded)", async () => {
    const pool = new FakePool();
    const sampleRow = {
      id: "99999999999999999", // 17-digit string — would lose precision via Number
      message_id: "m7",
      aggregate_type: "order",
      aggregate_id: "o-7",
      topic: "orders",
      key: null,
      payload: JSON.stringify({ a: 1 }),
      headers: JSON.stringify({ h: "1" }),
      trace_id: "trace-1",
      status: 1, // processing
      attempts: 0,
      next_retry_at: null,
      created_at: new Date("2026-01-01T00:00:00Z"),
      processed_at: null,
    };
    pool.programResponses([{ recordsets: [[sampleRow]] }]);
    const store = new MssqlStore({ pool: pool as unknown as never });
    const out = await store.claimBatch(10);

    expect(out).toHaveLength(1);
    expect(out[0]?.id).toBe("99999999999999999"); // still a string
    expect(out[0]?.status).toBe("processing"); // code 1 → string
    expect(out[0]?.payload).toEqual({ a: 1 }); // JSON parsed
    expect(out[0]?.headers).toEqual({ h: "1" }); // JSON parsed
    expect(out[0]?.traceId).toBe("trace-1");
  });

  it("throws on non-positive batchSize", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    await expect(store.claimBatch(0)).rejects.toThrow(TypeError);
    await expect(store.claimBatch(-1)).rejects.toThrow(TypeError);
    await expect(store.claimBatch(1.5)).rejects.toThrow(TypeError);
  });

  it("throws when batchSize exceeds the 10_000 cap", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    await expect(store.claimBatch(10_001)).rejects.toThrow(/exceeds 10000/);
  });
});

describe("MssqlStore.markDone", () => {
  it("is a no-op on an empty array (no query/batch observed)", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    await store.markDone([]);
    expect(pool.requests).toHaveLength(0);
  });

  it("rejects ids that don't match /^\\d+$/ BEFORE sending SQL", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    await expect(store.markDone(["1", "abc"])).rejects.toThrow(
      /must be a numeric string/,
    );
    await expect(store.markDone(["1", "1.5"])).rejects.toThrow(/numeric string/);
    await expect(
      store.markDone(["1", "1; DROP TABLE x"]),
    ).rejects.toThrow(/numeric string/);
    // Nothing reached the pool.
    expect(pool.requests).toHaveLength(0);
  });

  it("sends ids as JSON.stringify(array of strings) via @ids", async () => {
    const pool = new FakePool();
    pool.programResponses([{ rowsAffected: [2] }]);
    const store = new MssqlStore({ pool: pool as unknown as never });
    await store.markDone(["1", "2"]);

    const req = pool.requests[0]!;
    expect(req.calls[0]?.kind).toBe("query");
    // SQL Server does NOT support STRICT inside OPENJSON WITH (...) — the
    // STRICT keyword exists only on JSON_VALUE / JSON_QUERY. Defence is
    // upstream: every id is asserted /^\d+$/ at the TS boundary, so lax
    // mode cannot silently drop bad elements.
    expect(req.lastSql).toContain("OPENJSON(@ids)");
    expect(req.lastSql).toMatch(/WITH \(id BIGINT '\$'\)/);
    expect(req.inputs.get("ids")?.value).toBe('["1","2"]');
  });

  it("throws if rowsAffected mismatches recordIds length", async () => {
    const pool = new FakePool();
    pool.programResponses([{ rowsAffected: [1] }]); // only 1 of 2
    const store = new MssqlStore({ pool: pool as unknown as never });
    await expect(store.markDone(["1", "2"])).rejects.toThrow(
      /rowsAffected.*did not equal/,
    );
  });
});

describe("MssqlStore.markFailed", () => {
  it("rejects (null, 'failed') with TypeError mentioning the hot loop", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    await expect(store.markFailed("1", null, "failed")).rejects.toThrow(
      TypeError,
    );
    await expect(store.markFailed("1", null, "failed")).rejects.toThrow(
      /hot loop/,
    );
    expect(pool.requests).toHaveLength(0);
  });

  it("status='failed' binds @status as TINYINT 3", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    const retryAt = new Date("2026-06-17T10:00:00Z");
    await store.markFailed("42", retryAt, "failed");
    const req = pool.requests[0]!;
    expect(req.inputs.get("status")?.value).toBe(3);
    expect(req.inputs.get("status")?.type).toEqual({ kind: "TinyInt" });
    expect(req.inputs.get("id")?.value).toBe("42");
    expect(req.inputs.get("id")?.type).toEqual({ kind: "BigInt" });
    expect(req.inputs.get("nextRetryAt")?.value).toBe(retryAt);
    expect(req.inputs.get("nextRetryAt")?.type).toEqual({
      kind: "DateTime2",
      precision: 3,
    });
  });

  it("status='dead' binds @status as TINYINT 4 (terminal state may carry null retryAt)", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    await store.markFailed("42", null, "dead");
    const req = pool.requests[0]!;
    expect(req.inputs.get("status")?.value).toBe(4);
    expect(req.inputs.get("status")?.type).toEqual({ kind: "TinyInt" });
    expect(req.inputs.get("nextRetryAt")?.value).toBeNull();
  });

  it("SQL contains attempts + 1 (failure burns the retry budget)", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    await store.markFailed("42", new Date(), "failed");
    expect(pool.requests[0]?.lastSql).toMatch(/attempts\s*=\s*attempts \+ 1/);
  });
});

describe("MssqlStore.requeue", () => {
  it("flips status to 3, clears claimed_at, and does NOT touch attempts", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    const retryAt = new Date("2026-06-17T10:00:00Z");
    await store.requeue("42", retryAt);

    const req = pool.requests[0]!;
    const sqlText = req.lastSql ?? "";
    expect(sqlText).toMatch(/status\s*=\s*3/);
    expect(sqlText).toMatch(/claimed_at\s*=\s*NULL/);
    // CRITICAL: no attempts bump on a backpressure requeue.
    expect(sqlText).not.toMatch(/attempts/);
    expect(req.inputs.get("id")?.value).toBe("42");
    expect(req.inputs.get("retryAt")?.value).toBe(retryAt);
  });
});

describe("MssqlStore.purgeDone", () => {
  it("loops while deleted_count == batchSize and stops on the short batch", async () => {
    const pool = new FakePool();
    pool.programResponses([
      { recordsets: [[{ deleted_count: 1000 }]] }, // full batch
      { recordsets: [[{ deleted_count: 1000 }]] }, // full batch
      { recordsets: [[{ deleted_count: 250 }]] }, // short — stop
    ]);
    const store = new MssqlStore({ pool: pool as unknown as never });
    const total = await store.purgeDone({
      olderThanMs: 86_400_000,
      batchSize: 1000,
    });
    expect(total).toBe(2250);
    expect(pool.requests).toHaveLength(3);
  });

  it("honors maxRows as an upper bound (soft cap)", async () => {
    const pool = new FakePool();
    pool.programResponses([
      { recordsets: [[{ deleted_count: 1000 }]] },
      { recordsets: [[{ deleted_count: 1000 }]] },
      { recordsets: [[{ deleted_count: 1000 }]] }, // never reached
    ]);
    const store = new MssqlStore({ pool: pool as unknown as never });
    const total = await store.purgeDone({
      olderThanMs: 1000,
      batchSize: 1000,
      maxRows: 1500,
    });
    // After two batches total=2000 >= maxRows(1500) → stop
    expect(total).toBe(2000);
    expect(pool.requests.length).toBeLessThanOrEqual(2);
  });

  it("uses DELETE ... OUTPUT deleted.id INTO @deleted with TOP(@batchSize)", async () => {
    const pool = new FakePool();
    pool.programResponses([{ recordsets: [[{ deleted_count: 0 }]] }]);
    const store = new MssqlStore({ pool: pool as unknown as never });
    await store.purgeDone({ olderThanMs: 1000, batchSize: 100 });

    const sqlText = pool.requests[0]?.lastSql ?? "";
    expect(sqlText).toContain("DELETE FROM [dbo].[outbox]");
    expect(sqlText).toContain("OUTPUT deleted.id INTO @deleted");
    expect(sqlText).toContain("TOP (@batchSize)");
    expect(sqlText).toContain("ORDER BY processed_at, id");
    // status = 2 (done)
    expect(sqlText).toMatch(/status\s*=\s*2/);
  });

  it("binds @batchSize (Int) and @cutoff (DateTime2(3)) per iteration", async () => {
    const pool = new FakePool();
    pool.programResponses([{ recordsets: [[{ deleted_count: 0 }]] }]);
    const store = new MssqlStore({ pool: pool as unknown as never });
    await store.purgeDone({ olderThanMs: 60_000, batchSize: 50 });

    const req = pool.requests[0]!;
    expect(req.inputs.get("batchSize")?.value).toBe(50);
    expect(req.inputs.get("batchSize")?.type).toEqual({ kind: "Int" });
    expect(req.inputs.get("cutoff")?.type).toEqual({
      kind: "DateTime2",
      precision: 3,
    });
    expect(req.inputs.get("cutoff")?.value).toBeInstanceOf(Date);
    // Drives through .batch() (DECLARE + DELETE + SELECT is multi-statement)
    expect(req.calls[0]?.kind).toBe("batch");
  });

  it("rejects non-finite / negative olderThanMs", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    await expect(
      store.purgeDone({ olderThanMs: -1 }),
    ).rejects.toThrow(TypeError);
    await expect(
      store.purgeDone({ olderThanMs: Number.NaN }),
    ).rejects.toThrow(TypeError);
  });

  it("rejects non-positive batchSize", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    await expect(
      store.purgeDone({ olderThanMs: 1000, batchSize: 0 }),
    ).rejects.toThrow(TypeError);
  });
});

describe("MssqlStore.init / close", () => {
  it("are no-ops (store does NOT manage the pool lifecycle)", async () => {
    const pool = new FakePool();
    const store = new MssqlStore({ pool: pool as unknown as never });
    await expect(store.init()).resolves.toBeUndefined();
    await expect(store.close()).resolves.toBeUndefined();
    expect(pool.requests).toHaveLength(0);
  });
});

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

/**
 * Arm the *next* `new mssql.Request(tx)` instance so its first batch/query
 * call resolves with `response`.  The mocked Request constructor sets
 * `tx.lastRequest` synchronously, but the store immediately calls
 * `request.batch(...)` before the test gets to program a response.  We
 * solve that race by intercepting the construction itself: wrap the
 * tx's `lastRequest` property with a setter that programs the response
 * the moment the Request lands on the tx.
 */
function installResponseOnNextRequest(
  tx: FakeTransaction,
  response: FakeQueryResult,
): { restore: () => void } {
  let internal: FakeRequest | null = null;
  const desc = Object.getOwnPropertyDescriptor(tx, "lastRequest");
  Object.defineProperty(tx, "lastRequest", {
    configurable: true,
    get() {
      return internal;
    },
    set(req: FakeRequest) {
      internal = req;
      if (req !== null) req.setResponses([response]);
    },
  });
  return {
    restore() {
      Object.defineProperty(tx, "lastRequest", {
        configurable: true,
        writable: true,
        value: internal,
        ...(desc?.enumerable !== undefined
          ? { enumerable: desc.enumerable }
          : { enumerable: true }),
      });
    },
  };
}
