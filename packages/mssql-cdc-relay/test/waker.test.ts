import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";

// --------------------------------------------------------------------------
// Fake mssql surface (mirrors packages/mssql/test/store.test.ts FakePool /
// FakeRequest pattern).  The waker only touches:
//
//   - pool.request()
//   - request.input(name, type, value)
//   - request.query(sql)
//   - request.batch(sql)
//   - request.cancel()  (during stop())
//   - mssql.NVarChar(len)
//   - mssql.Int
//   - mssql.VarBinary(len)
//
// Each test programs a FIFO of responses on the pool; every call to
// `pool.request()` mints a FakeRequest that takes the next programmed
// response (and falls back to the default).  Recordset shape tracks the
// waker's two-recordset poll: [0] = meta, [1] = start_lsn rows.
// --------------------------------------------------------------------------

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
  public cancelled = false;
  /** Programmed responses for THIS request (FIFO). */
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

  cancel(): void {
    this.cancelled = true;
  }

  private nextResponse(): FakeQueryResult {
    return this.responses.shift() ?? this.defaultResponse;
  }
}

class FakePool {
  /** Requests minted by `pool.request()`, in order. */
  public readonly requests: FakeRequest[] = [];
  public closeCount = 0;
  /** Each call to `.request()` pulls the next programmed response, if any. */
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
    this.closeCount++;
  }
}

// --------------------------------------------------------------------------
// Mock the mssql module (must come before `import { MssqlCdcWaker }`)
// --------------------------------------------------------------------------

vi.mock("mssql", () => {
  const NVarCharType = (len: number | unknown) => ({
    kind: "NVarChar",
    length: len,
  });
  const VarBinaryType = (len: number | unknown) => ({
    kind: "VarBinary",
    length: len,
  });
  const sql = {
    NVarChar: NVarCharType,
    VarBinary: VarBinaryType,
    Int: { kind: "Int" },
    BigInt: { kind: "BigInt" },
    TinyInt: { kind: "TinyInt" },
    MAX: "MAX" as const,
    // The waker uses pool.request(), never `new mssql.Request(tx)` directly,
    // but we expose a no-op shim so the import surface is complete.
    Request: class MockRequest {},
  };
  return { ...sql, default: sql };
});

// Imports must come AFTER vi.mock so the waker's `import * as mssql` binds
// to the mocked module.
import {
  MssqlCdcWaker,
  MssqlCdcUnsupportedEngineError,
  MssqlCdcReadOnlyReplicaError,
  MssqlCdcWatermarkMissingError,
  MssqlCdcCaptureDisabledError,
  MssqlCdcRetentionOverrunError,
} from "../src/waker.js";

// --------------------------------------------------------------------------
// Test helpers
// --------------------------------------------------------------------------

/**
 * Builds the recordsets array for one successful `pollOnce` batch.
 *
 * The waker's poll SQL returns TWO recordsets:
 *   [0] meta row: { owned, retention_overrun, capture_disabled,
 *                   from_hex, min_hex, max_hex, effective_from }
 *   [1] zero or more change rows: { start_lsn: Buffer | null }
 *
 * Field defaults represent the "happy idle" state: capture is on, no
 * retention overrun, watermark equals max.
 */
function pollResult(opts: {
  owned?: number;
  retentionOverrun?: number;
  captureDisabled?: number;
  fromHex?: string | null;
  minHex?: string | null;
  maxHex?: string | null;
  effectiveFrom?: Buffer | null;
  rows?: Array<{ start_lsn: Buffer | null }>;
}): FakeQueryResult {
  return {
    recordsets: [
      [
        {
          owned: opts.owned ?? 1,
          retention_overrun: opts.retentionOverrun ?? 0,
          capture_disabled: opts.captureDisabled ?? 0,
          from_hex: opts.fromHex ?? "0x0000000000000000000A",
          min_hex: opts.minHex ?? "0x00000000000000000005",
          max_hex: opts.maxHex ?? "0x0000000000000000000A",
          effective_from: opts.effectiveFrom ?? Buffer.from([
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0x0a,
          ]),
        },
      ],
      opts.rows ?? [],
    ],
  };
}

/** "Engine supports CDC" probe response (EngineEdition=3 = Enterprise). */
const ENGINE_OK: FakeQueryResult = {
  recordset: [{ edition: 3 }],
  recordsets: [[{ edition: 3 }]],
};

/** "Engine refuses CDC" (EngineEdition=4 = Express). */
const ENGINE_EXPRESS: FakeQueryResult = {
  recordset: [{ edition: 4 }],
  recordsets: [[{ edition: 4 }]],
};

/** Read-write database probe response. */
const RW_DB: FakeQueryResult = {
  recordset: [{ ro: "READ_WRITE" }],
  recordsets: [[{ ro: "READ_WRITE" }]],
};

/** Watermark row exists. */
const WATERMARK_EXISTS: FakeQueryResult = {
  recordset: [{ exists_flag: 1 }],
  recordsets: [[{ exists_flag: 1 }]],
};

/** Watermark row missing. */
const WATERMARK_MISSING: FakeQueryResult = {
  recordset: [{ exists_flag: 0 }],
  recordsets: [[{ exists_flag: 0 }]],
};

/** persistWatermarkIncrement: this process won the conditional UPDATE race. */
const PERSIST_WON: FakeQueryResult = {
  recordset: [{ affected: 1 }],
  recordsets: [[{ affected: 1 }]],
};

function makeWaker(
  pool: FakePool,
  overrides: Partial<{
    onWake: () => void;
    onError: (err: Error) => void;
    pollIntervalMs: number;
    batchSize: number;
    stickyWakeCycles: number;
    captureInstance: string;
    watermarkSchema: string;
    watermarkTable: string;
  }> = {},
): { waker: MssqlCdcWaker; onWake: ReturnType<typeof vi.fn>; onError: ReturnType<typeof vi.fn> } {
  const onWake = vi.fn();
  const onError = vi.fn(overrides.onError);
  const waker = new MssqlCdcWaker({
    pool: pool as unknown as never,
    pollIntervalMs: overrides.pollIntervalMs ?? 100,
    batchSize: overrides.batchSize,
    stickyWakeCycles: overrides.stickyWakeCycles ?? 0,
    captureInstance: overrides.captureInstance,
    watermarkSchema: overrides.watermarkSchema,
    watermarkTable: overrides.watermarkTable,
    onError: (e) => onError(e),
  });
  return { waker, onWake: vi.fn(overrides.onWake ?? onWake), onError };
}

/**
 * Yield to the microtask queue so any pending Promise chains resolve.
 * Calling this several times in a row lets the waker's `runCycle` settle
 * after the timer fires.
 */
async function flushMicrotasks(times = 5): Promise<void> {
  for (let i = 0; i < times; i++) {
    await Promise.resolve();
  }
}

// --------------------------------------------------------------------------
// Constructor
// --------------------------------------------------------------------------

describe("MssqlCdcWaker constructor", () => {
  it("applies defaults: pollIntervalMs=1000, schema='eventferry', table='cdc_watermark', captureInstance='dbo_outbox'", async () => {
    // Drive a healthCheck (which interpolates schema + table into SQL and
    // passes captureInstance as @ci) to observe the defaults reaching the
    // wire — the only externally visible probe of the bracketed identifiers.
    const pool = new FakePool();
    pool.programResponses([
      {
        recordset: [
          {
            capture_enabled: 1,
            min_lsn_hex: "0x01",
            max_lsn_hex: "0x02",
            watermark_hex: "0x01",
            read_only: 0,
          },
        ],
      },
    ]);
    const waker = new MssqlCdcWaker({ pool: pool as unknown as never });
    await waker.healthCheck();
    const req = pool.requests[0]!;
    // Default capture instance binding
    expect(req.inputs.get("ci")?.value).toBe("dbo_outbox");
    expect(req.inputs.get("ci")?.type).toEqual({
      kind: "NVarChar",
      length: 128,
    });
    // Default schema.table interpolated as [eventferry].[cdc_watermark]
    expect(req.lastSql).toContain("[eventferry].[cdc_watermark]");
  });

  it("respects custom watermarkSchema/watermarkTable/captureInstance", async () => {
    const pool = new FakePool();
    pool.programResponses([
      {
        recordset: [
          {
            capture_enabled: 1,
            min_lsn_hex: null,
            max_lsn_hex: null,
            watermark_hex: null,
            read_only: 0,
          },
        ],
      },
    ]);
    const waker = new MssqlCdcWaker({
      pool: pool as unknown as never,
      captureInstance: "schemaA_outbox",
      watermarkSchema: "ops",
      watermarkTable: "wm",
    });
    await waker.healthCheck();
    const req = pool.requests[0]!;
    expect(req.inputs.get("ci")?.value).toBe("schemaA_outbox");
    expect(req.lastSql).toContain("[ops].[wm]");
  });

  it("rejects a missing pool with TypeError", () => {
    expect(
      () => new MssqlCdcWaker({ pool: undefined as unknown as never }),
    ).toThrow(/opts\.pool is required/);
  });

  it("rejects shared pool === storePool (reference equality)", () => {
    const pool = new FakePool();
    expect(
      () =>
        new MssqlCdcWaker({
          pool: pool as unknown as never,
          storePool: pool as unknown as never,
        }),
    ).toThrow(/must NOT be the same instance as `storePool`/);
  });

  it("rejects an invalid capture instance via assertCaptureInstance", () => {
    const pool = new FakePool();
    expect(
      () =>
        new MssqlCdcWaker({
          pool: pool as unknown as never,
          captureInstance: "bad; DROP TABLE x;--",
        }),
    ).toThrow();
  });

  it("rejects an invalid watermarkSchema via assertIdent", () => {
    const pool = new FakePool();
    expect(
      () =>
        new MssqlCdcWaker({
          pool: pool as unknown as never,
          watermarkSchema: "ops.bad",
        }),
    ).toThrow(/invalid SQL identifier/);
  });

  it("rejects out-of-range pollIntervalMs (clampInt enforces 100..60_000)", () => {
    const pool = new FakePool();
    expect(
      () =>
        new MssqlCdcWaker({
          pool: pool as unknown as never,
          pollIntervalMs: 50, // below floor
        }),
    ).toThrow(/pollIntervalMs/);
    expect(
      () =>
        new MssqlCdcWaker({
          pool: pool as unknown as never,
          pollIntervalMs: 120_000, // above ceiling
        }),
    ).toThrow(/pollIntervalMs/);
  });
});

// --------------------------------------------------------------------------
// start(): engine + replica + watermark gating
// --------------------------------------------------------------------------

describe("MssqlCdcWaker.start engine + replica + watermark gates", () => {
  it("refuses with MssqlCdcUnsupportedEngineError on EngineEdition=4 (SQL Server Express, no SQL Server Agent)", async () => {
    const pool = new FakePool();
    // 1st request: engine probe returns Express (edition=4).
    pool.programResponses([ENGINE_EXPRESS]);
    const { waker } = makeWaker(pool);
    await expect(waker.start(() => {})).rejects.toBeInstanceOf(
      MssqlCdcUnsupportedEngineError,
    );
    // No further probes happened — start exited at the engine gate.
    expect(pool.requests).toHaveLength(1);
  });

  it("refuses with MssqlCdcReadOnlyReplicaError on a READ_ONLY database (Always On secondary / log-shipping standby)", async () => {
    const pool = new FakePool();
    pool.programResponses([
      ENGINE_OK,
      { recordset: [{ ro: "READ_ONLY" }] }, // read-only probe
    ]);
    const { waker } = makeWaker(pool);
    await expect(waker.start(() => {})).rejects.toBeInstanceOf(
      MssqlCdcReadOnlyReplicaError,
    );
    expect(pool.requests).toHaveLength(2);
  });

  it("throws MssqlCdcWatermarkMissingError when no watermark row exists for the capture instance (NOT auto-seeded — would skip backlog)", async () => {
    const pool = new FakePool();
    pool.programResponses([ENGINE_OK, RW_DB, WATERMARK_MISSING]);
    const { waker } = makeWaker(pool);
    await expect(waker.start(() => {})).rejects.toBeInstanceOf(
      MssqlCdcWatermarkMissingError,
    );
    expect(pool.requests).toHaveLength(3);
    // The watermark check binds the capture instance.
    expect(pool.requests[2]?.inputs.get("ci")?.value).toBe("dbo_outbox");
  });

  it("proceeds past all three gates when engine supports CDC, DB is RW, and watermark row exists", async () => {
    const pool = new FakePool();
    // The 4th call is the first poll cycle — return a steady-state "no new
    // rows" snapshot so the test never observes a wake.
    pool.programResponses([
      ENGINE_OK,
      RW_DB,
      WATERMARK_EXISTS,
      pollResult({ rows: [] }),
    ]);
    const { waker, onError } = makeWaker(pool, { pollIntervalMs: 10_000 });
    await waker.start(() => {});
    // start() returned without throwing — the three gate requests fired.
    expect(pool.requests.length).toBeGreaterThanOrEqual(3);
    expect(onError).not.toHaveBeenCalled();
    await waker.stop();
  });
});

// --------------------------------------------------------------------------
// Poll loop: fires onWake exactly once per non-empty batch
// --------------------------------------------------------------------------

describe("MssqlCdcWaker poll loop — wake firing", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("fires onWake() exactly once when pollOnce sees new CDC rows (watermark advances)", async () => {
    const pool = new FakePool();
    // Gates pass, first cycle observes one row, persistWatermark wins.
    pool.programResponses([
      ENGINE_OK,
      RW_DB,
      WATERMARK_EXISTS,
      pollResult({
        rows: [
          { start_lsn: Buffer.from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10]) },
        ],
      }),
      PERSIST_WON,
      // Second cycle: empty so we can assert "exactly once" cleanly.
      pollResult({ rows: [] }),
    ]);
    const onWake = vi.fn();
    const onError = vi.fn();
    const waker = new MssqlCdcWaker({
      pool: pool as unknown as never,
      pollIntervalMs: 100,
      stickyWakeCycles: 0, // disable sticky so wake count is exact
      onError,
    });
    await waker.start(onWake);
    // Initial cycle was scheduled with delay 0 — advance the loop.
    await vi.advanceTimersByTimeAsync(0);
    await flushMicrotasks();
    // One advance → one wake.
    expect(onWake).toHaveBeenCalledTimes(1);
    // Advance again to drive the empty cycle.
    await vi.advanceTimersByTimeAsync(100);
    await flushMicrotasks();
    expect(onWake).toHaveBeenCalledTimes(1);
    expect(onError).not.toHaveBeenCalled();
    await waker.stop();
  });

  it("does NOT fire onWake() when pollOnce returns zero rows (max_lsn == watermark steady state)", async () => {
    const pool = new FakePool();
    pool.programResponses([
      ENGINE_OK,
      RW_DB,
      WATERMARK_EXISTS,
      pollResult({ rows: [] }), // empty cycle
      pollResult({ rows: [] }), // empty cycle
    ]);
    const onWake = vi.fn();
    const onError = vi.fn();
    const waker = new MssqlCdcWaker({
      pool: pool as unknown as never,
      pollIntervalMs: 100,
      stickyWakeCycles: 0,
      onError,
    });
    await waker.start(onWake);
    await vi.advanceTimersByTimeAsync(0);
    await flushMicrotasks();
    await vi.advanceTimersByTimeAsync(100);
    await flushMicrotasks();
    expect(onWake).not.toHaveBeenCalled();
    expect(onError).not.toHaveBeenCalled();
    await waker.stop();
  });

  it("does NOT fire onWake() when persistWatermarkIncrement loses the race (rowcount=0 — peer process advanced first)", async () => {
    const pool = new FakePool();
    const PERSIST_LOST: FakeQueryResult = {
      recordset: [{ affected: 0 }],
      recordsets: [[{ affected: 0 }]],
    };
    pool.programResponses([
      ENGINE_OK,
      RW_DB,
      WATERMARK_EXISTS,
      pollResult({
        rows: [
          { start_lsn: Buffer.from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10]) },
        ],
      }),
      PERSIST_LOST, // loser branch — must NOT fire wake
    ]);
    const onWake = vi.fn();
    const waker = new MssqlCdcWaker({
      pool: pool as unknown as never,
      pollIntervalMs: 100,
      stickyWakeCycles: 0,
    });
    await waker.start(onWake);
    await vi.advanceTimersByTimeAsync(0);
    await flushMicrotasks();
    expect(onWake).not.toHaveBeenCalled();
    await waker.stop();
  });
});

// --------------------------------------------------------------------------
// Poll loop: structural errors surface via onError (NEVER thrown into caller)
// --------------------------------------------------------------------------

describe("MssqlCdcWaker poll loop — error surface", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("surfaces MssqlCdcRetentionOverrunError via onError when watermark fell below min_lsn (NOT thrown into start() caller — loop continues to salvage [min, max])", async () => {
    const pool = new FakePool();
    // Retention overrun: from_hex < min_hex. effective_from clamps to min.
    // Empty rows in this cycle is fine — we're testing the error surface.
    pool.programResponses([
      ENGINE_OK,
      RW_DB,
      WATERMARK_EXISTS,
      pollResult({
        retentionOverrun: 1,
        fromHex: "0x00000000000000000003", // below
        minHex: "0x00000000000000000007", // floor
        maxHex: "0x0000000000000000000F",
        rows: [],
      }),
      // Next cycle: clean steady state.
      pollResult({ rows: [] }),
    ]);
    const onWake = vi.fn();
    const onError = vi.fn();
    const waker = new MssqlCdcWaker({
      pool: pool as unknown as never,
      pollIntervalMs: 100,
      stickyWakeCycles: 0,
      onError,
    });
    // start() must NOT reject — the error is async, surfaced via onError.
    await expect(waker.start(onWake)).resolves.toBeUndefined();
    await vi.advanceTimersByTimeAsync(0);
    await flushMicrotasks();

    expect(onError).toHaveBeenCalledTimes(1);
    const err = onError.mock.calls[0]![0] as Error;
    expect(err).toBeInstanceOf(MssqlCdcRetentionOverrunError);
    expect((err as MssqlCdcRetentionOverrunError).captureInstance).toBe(
      "dbo_outbox",
    );

    // Loop continued: a second cycle ran.
    await vi.advanceTimersByTimeAsync(100);
    await flushMicrotasks();
    // No further error from the empty steady-state cycle.
    expect(onError).toHaveBeenCalledTimes(1);
    await waker.stop();
  });

  it("surfaces MssqlCdcCaptureDisabledError via onError when capture is disabled (sp_cdc_disable_table/_db raced — covers the 'no active capture' detection)", async () => {
    const pool = new FakePool();
    pool.programResponses([
      ENGINE_OK,
      RW_DB,
      WATERMARK_EXISTS,
      pollResult({
        captureDisabled: 1,
        minHex: null,
        maxHex: null,
        effectiveFrom: null,
        rows: [],
      }),
      // Subsequent cycles also see capture_disabled; onError fires ONCE
      // (idempotent — inIdleMode latches until capture returns).
      pollResult({
        captureDisabled: 1,
        minHex: null,
        maxHex: null,
        effectiveFrom: null,
        rows: [],
      }),
    ]);
    const onWake = vi.fn();
    const onError = vi.fn();
    const waker = new MssqlCdcWaker({
      pool: pool as unknown as never,
      pollIntervalMs: 100,
      stickyWakeCycles: 0,
      onError,
    });
    await waker.start(onWake);
    await vi.advanceTimersByTimeAsync(0);
    await flushMicrotasks();

    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]![0]).toBeInstanceOf(
      MssqlCdcCaptureDisabledError,
    );

    // Second cycle still in idle mode — must NOT re-fire (latched).
    await vi.advanceTimersByTimeAsync(100);
    await flushMicrotasks();
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onWake).not.toHaveBeenCalled();
    await waker.stop();
  });

  it("a synchronous throw inside the loop is caught and routed through the error/backoff path — onError is NEVER called with a sync throw from the loop body itself", async () => {
    // We model a driver-level sync throw on the poll batch.  The waker's
    // runCycle wraps the entire pollOnce in try/catch; the surfaced
    // mechanism is the consecutiveFailures counter + scheduled backoff,
    // NOT a synchronous re-throw of the caller.
    const pool = new FakePool();
    pool.programResponses([ENGINE_OK, RW_DB, WATERMARK_EXISTS]);
    // Override pool.request to make .batch() throw on the first poll cycle.
    const realRequest = pool.request.bind(pool);
    let cycleNum = 0;
    pool.request = function (): FakeRequest {
      const req = realRequest();
      cycleNum++;
      if (cycleNum === 4) {
        // Replace .batch with a synchronous throw to simulate a driver bug.
        req.batch = (() => {
          throw new Error("synthetic sync driver throw");
        }) as unknown as FakeRequest["batch"];
      }
      return req;
    };

    const onWake = vi.fn();
    const onError = vi.fn();
    const waker = new MssqlCdcWaker({
      pool: pool as unknown as never,
      pollIntervalMs: 100,
      stickyWakeCycles: 0,
      onError,
    });
    // start() must NOT see the sync throw.
    await expect(waker.start(onWake)).resolves.toBeUndefined();
    await vi.advanceTimersByTimeAsync(0);
    await flushMicrotasks();

    // onError is NOT called for a one-off sync throw — it's only called
    // when consecutiveFailures crosses the stuck threshold (5).  The
    // first failure should be silent on onError, surfaced only via the
    // scheduled backoff.
    expect(onError).not.toHaveBeenCalled();
    expect(onWake).not.toHaveBeenCalled();
    await waker.stop();
  });
});

// --------------------------------------------------------------------------
// stop(): clean shutdown
// --------------------------------------------------------------------------

describe("MssqlCdcWaker.stop", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("sets the stop flag, awaits the in-flight cycle latch, and cancels the inflight request — does NOT close the pool (lifecycle owned by caller)", async () => {
    const pool = new FakePool();
    pool.programResponses([
      ENGINE_OK,
      RW_DB,
      WATERMARK_EXISTS,
      pollResult({ rows: [] }),
    ]);
    const { waker, onWake } = makeWaker(pool, { pollIntervalMs: 100 });
    await waker.start(onWake);
    await vi.advanceTimersByTimeAsync(0);
    await flushMicrotasks();

    // stop() returns even when no cycle is in flight.
    vi.useRealTimers();
    await waker.stop();

    // CRITICAL: stop() must NOT call pool.close() — the pool was supplied
    // by the caller (who may share it with healthCheck or another relay).
    expect(pool.closeCount).toBe(0);

    // After stop(), calling stop() again is idempotent (no throw, no close).
    await waker.stop();
    expect(pool.closeCount).toBe(0);
  });

  it("stop() drains promptly when no cycle is in flight (no shutdownTimeoutMs wait)", async () => {
    const pool = new FakePool();
    pool.programResponses([
      ENGINE_OK,
      RW_DB,
      WATERMARK_EXISTS,
      pollResult({ rows: [] }),
    ]);
    const { waker, onWake } = makeWaker(pool, { pollIntervalMs: 10_000 });
    await waker.start(onWake);
    // Don't advance the timer — the first cycle hasn't even fired yet.
    vi.useRealTimers();
    const t0 = Date.now();
    await waker.stop();
    const elapsed = Date.now() - t0;
    // Should be near-instant; certainly under the 5_000ms shutdown budget.
    expect(elapsed).toBeLessThan(500);
  });
});
