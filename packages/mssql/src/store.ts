import type {
  Logger,
  OutboxMessageInput,
  OutboxRecord,
  OutboxStore,
  Tracing,
} from "@eventferry/core";
import { OUTBOX_STATUS_CODE } from "@eventferry/core";
import * as mssql from "mssql";
import { assertIdent } from "./ident.js";
import { rowToRecord, type OutboxRow } from "./row.js";

/**
 * Construction-time options for {@link MssqlStore}.
 *
 * The store NEVER manages the pool lifecycle — the caller is responsible
 * for `await pool.connect()` BEFORE constructing the store, and for
 * attaching `pool.on('error', ...)` BEFORE handing the pool to the store
 * (an unhandled `'error'` event on a `mssql.ConnectionPool` will crash
 * the Node process). See the package README.
 */
export interface MssqlStoreOptions {
  /**
   * Already-connected `mssql.ConnectionPool`. Caller MUST `await pool.connect()`
   * before passing. The store does NOT call `pool.connect()` / `pool.close()`.
   *
   * NEVER pass the module-level `mssql` singleton (`sql.pool`, `sql.connect()`):
   * the global is process-wide and breaks per-store isolation in tests.
   */
  pool: mssql.ConnectionPool;
  /**
   * Schema name (default `"dbo"`). Validated by `assertIdent`. Stored with
   * brackets pre-applied internally so query composition stays simple.
   *
   * Schema qualification is REQUIRED for non-`dbo` deployments: Azure AD
   * logins, contained DB users, and AG per-app schemas all default to a
   * non-`dbo` schema. Without `[schema].[table]` in every statement, the
   * idempotency probes would inspect `dbo.outbox` while CREATE landed on
   * `<userschema>.outbox` — yielding either duplicate tables or error 2714
   * on the second migration run.
   */
  schema?: string;
  /**
   * Outbox table name (default `"outbox"`). Validated by `assertIdent`.
   * Stored with brackets pre-applied internally.
   */
  table?: string;
  /**
   * Visibility timeout (ms) after which a row stuck in `processing` is
   * reclaimable by any relay. Guards against permanently-orphaned rows
   * when a relay crashes between claim and ack. MUST be comfortably larger
   * than the worst-case publish latency, otherwise a slow-but-alive relay's
   * in-flight rows get reclaimed and re-published (a duplicate). Default
   * 60_000 ms.
   *
   * Clamped to `<= 86_400_000` (24 h) in the constructor. Values above the
   * clamp are almost always misconfiguration AND push toward the
   * `sql.Int` 2^31-1 ms ceiling (~24.85 d) where `DATEADD(MILLISECOND,
   * -@claimTimeoutMs, ...)` overflows. The clamp warns through the
   * supplied {@link Logger} (or `console`) and proceeds with `86_400_000`.
   */
  claimTimeoutMs?: number;
  /**
   * When true, `claimBatch` never claims `pending`(0) rows — only
   * `failed`(3) (retry due) and timed-out `processing`(1). Reserved for
   * a future CDC streaming relay that owns the pending path itself.
   * Default `false`.
   */
  claimFailedOnly?: boolean;
  /**
   * Optional trace-context propagator. When set, `enqueue` captures the
   * active W3C trace context into the row's `headers`, so it rides along
   * to the published message and the consumer can continue the trace.
   * The store never mutates the caller's `msg.headers` object — it works
   * against a defensive copy. See {@link Tracing}.
   */
  tracing?: Tracing;
  /**
   * Opt-in to SQL Server 2025+ native `json` column type in the migration
   * DDL emitted by {@link createMigrationSql}. This flag gates ONLY the
   * DDL; the wire format is identical at the store layer:
   * `JSON.stringify` on write, `JSON.parse` on read, either way (tedious
   * never auto-parses, even for the native `json` type). Default `false`.
   */
  useNativeJson?: boolean;
  /**
   * Optional structured logger. Used to emit the `claimTimeoutMs` clamp
   * warning and any future store-level diagnostics. Falls back to
   * `console` when omitted.
   */
  logger?: Logger;
}

/**
 * Options for {@link MssqlStore.purgeDone}.
 */
export interface PurgeDoneOptions {
  /** Delete `done` rows whose `processed_at` is older than this (milliseconds). */
  olderThanMs: number;
  /** Rows deleted per batch. Default 1000. */
  batchSize?: number;
  /**
   * Soft cap on total rows deleted in this call. The loop terminates AFTER
   * an iteration that crosses `maxRows`, so actual deletion may exceed
   * `maxRows` by up to `batchSize - 1` (parity with the Postgres and
   * MySQL adapters).
   */
  maxRows?: number;
}

const DEFAULT_CLAIM_TIMEOUT_MS = 60_000;
const MAX_CLAIM_TIMEOUT_MS = 86_400_000;
const MAX_BATCH_SIZE = 10_000;
const DEFAULT_PURGE_BATCH_SIZE = 1000;
// status code 1 (processing) is set inline in the claim SQL; no JS constant needed
const DONE = OUTBOX_STATUS_CODE.done;
const FAILED = OUTBOX_STATUS_CODE.failed;

/**
 * SQL Server outbox store. Owns every SQL string and parameter binding for
 * the adapter; every other module in the package (migrations, row mapping,
 * identifier validation) feeds into the methods below.
 *
 * Engine floor: SQL Server 2016 SP1+ (compatibility level 130 or higher).
 * Requires `OPENJSON` (used by {@link markDone}) and filtered indexes with
 * a `WHERE` clause (used by the migration DDL).
 *
 * Driver wiring (all enforced for every parameter):
 *   - explicit `Request.input(name, sql.<Type>, value)` — inferred types
 *     silently promote `String` to `NVarChar(4000)` (truncation) and
 *     `Date` to legacy `DATETIME` (3.33 ms precision, disabling
 *     `DATETIME2(3)` index seeks via `CONVERT_IMPLICIT`).
 *   - `Request.batch()` for multi-statement SQL (claim, enqueue: both
 *     `DECLARE` an `OUTPUT INTO @t` table variable). `query()` routes
 *     through `sp_executesql` which cannot carry session-scoped state
 *     cleanly across the multi-statement block.
 *   - `Request.query()` for single-statement SQL
 *     ({@link markDone}, {@link markFailed}, {@link requeue}, each
 *     iteration of {@link purgeDone}) so it benefits from
 *     `sp_executesql` plan caching.
 *
 * Lock and isolation contract (claim path):
 *   - The claim CTE acquires `READCOMMITTEDLOCK, READPAST, UPDLOCK,
 *     ROWLOCK` on the outer table reference. The combination is mandatory:
 *       * `READCOMMITTEDLOCK` forces locking semantics inside the CTE
 *         even on RCSI databases (where `READPAST` is otherwise silently
 *         ignored under default RC).
 *       * `READPAST` skips rows another claimer holds, giving concurrent
 *         relays SKIP-LOCKED semantics.
 *       * `ROWLOCK` is mandatory with `READPAST` — without it, SQL
 *         Server can escalate to page locks, `READPAST` degrades, and
 *         concurrent claimers serialize instead of skipping each other.
 *       * `UPDLOCK` upgrades the held lock so the subsequent `UPDATE`
 *         does not need to re-acquire.
 *   - The inner `NOT EXISTS` head-of-aggregate probe carries ONLY
 *     `READCOMMITTEDLOCK` — adding `READPAST` to the inner reference
 *     would cause concurrent relays to SKIP each other's locked earlier
 *     rows, making the `NOT EXISTS` clause return TRUE incorrectly and
 *     breaking head-of-aggregate ordering. We deliberately BLOCK
 *     briefly on a competitor's `UPDLOCK` here.
 *   - The `UPDATE` outer reference REPEATS the lock hint set
 *     (`READCOMMITTEDLOCK, ROWLOCK, UPDLOCK`). Without this, on RCSI
 *     the `UPDATE`'s X-lock acquisition is decoupled from the CTE's
 *     `UPDLOCK`, and a concurrent claimer can sneak in between the
 *     CTE-fetch and UPDATE-acquire phases — duplicate claim.
 *   - `OPTION (MAXDOP 1)` — intra-query parallelism with
 *     `READPAST+UPDLOCK+ROWLOCK` has documented deadlock patterns; the
 *     claim scan is tiny (TOP @batchSize) so single-threaded execution
 *     is both correct and faster in practice.
 *
 * Transaction cleanup (claim path):
 *   - The claim SQL is wrapped in `SET XACT_ABORT ON; BEGIN TRY; BEGIN
 *     TRAN; ...; COMMIT; END TRY; BEGIN CATCH; IF @@TRANCOUNT > 0
 *     ROLLBACK; THROW; END CATCH;`. The leading `IF @@TRANCOUNT > 0
 *     ROLLBACK` belt-and-braces guarantees TX cleanup before the
 *     connection returns to the pool — `XACT_ABORT ON` alone does NOT
 *     cover client-side aborts / TDS attention signals, which would
 *     otherwise leave an open transaction on a pool-returned connection.
 *
 * Trigger compatibility:
 *   - Every `OUTPUT inserted.*` / `OUTPUT deleted.*` lands into a
 *     `DECLARE @t TABLE (...)` (see `enqueue_sql`, `claim_sql`,
 *     `purgedone_sql`). Bare `OUTPUT inserted.*` fails with SQL Server
 *     error 334 if ANY trigger is enabled on the table.
 *
 * BIGINT and money: `id` and parameter `@id` are bound via
 * `sql.BigInt` and the `OutboxRecord.id` contract is `string`. tedious
 * returns `BIGINT` as a JS string (`value.toString()`); we pass it
 * through end-to-end. NEVER `Number(row.id)` — outbox ids can exceed
 * `2^53` after enough throughput, and a silent precision loss there
 * would corrupt `markDone` / `markFailed` lookups.
 */
export class MssqlStore implements OutboxStore {
  private readonly pool: mssql.ConnectionPool;
  /** Bracketed identifier `[dbo]` etc. — composed once in the constructor. */
  private readonly schemaBracketed: string;
  /** Bracketed identifier `[outbox]` etc. — composed once in the constructor. */
  private readonly tableBracketed: string;
  private readonly claimTimeoutMs: number;
  private readonly claimFailedOnly: boolean;
  private readonly tracing: Tracing | null;
  private readonly logger: Logger | null;

  constructor(opts: MssqlStoreOptions) {
    if (opts.pool === undefined || opts.pool === null) {
      throw new TypeError(
        "MssqlStore: opts.pool is required (pass a connected mssql.ConnectionPool)",
      );
    }
    this.pool = opts.pool;

    // `assertIdent` runs BEFORE any string interpolation — this is the
    // SOLE injection defence. The constructor calls it for both schema
    // and table; the standalone migration exports run it independently
    // (each top-level entrypoint must validate its own inputs).
    const schema = assertIdent(opts.schema ?? "dbo", "schema");
    const table = assertIdent(opts.table ?? "outbox", "table");
    this.schemaBracketed = `[${schema}]`;
    this.tableBracketed = `[${table}]`;

    const requested = opts.claimTimeoutMs ?? DEFAULT_CLAIM_TIMEOUT_MS;
    if (!Number.isFinite(requested) || requested <= 0) {
      throw new TypeError(
        `MssqlStore: claimTimeoutMs must be a positive finite number, got ${String(requested)}`,
      );
    }
    this.logger = opts.logger ?? null;
    if (requested > MAX_CLAIM_TIMEOUT_MS) {
      const message = `MssqlStore: claimTimeoutMs ${requested}ms exceeds 24h ceiling (${MAX_CLAIM_TIMEOUT_MS}ms); clamping. Values higher than that approach the sql.Int 2^31-1 ms (~24.85d) overflow.`;
      if (this.logger !== null) {
        this.logger.warn(message, {
          requested,
          clamped: MAX_CLAIM_TIMEOUT_MS,
        });
      } else {
        // eslint-disable-next-line no-console
        console.warn(message);
      }
      this.claimTimeoutMs = MAX_CLAIM_TIMEOUT_MS;
    } else {
      this.claimTimeoutMs = requested;
    }

    this.claimFailedOnly = opts.claimFailedOnly ?? false;
    this.tracing = opts.tracing ?? null;
    // `useNativeJson` is intentionally NOT stored — it gates ONLY the
    // migration DDL (read by createMigrationSql), not the runtime wire
    // path. The store is opaque to whether payload/headers are NVARCHAR(MAX)
    // or native `json` server-side.
  }

  /** Fully-qualified `[schema].[table]` for use inside SQL strings. */
  private get qualifiedTable(): string {
    return `${this.schemaBracketed}.${this.tableBracketed}`;
  }

  /**
   * Insert a message into the outbox using the caller's transaction so
   * the row commits atomically with business state. `tx` is an
   * `mssql.Transaction` that has ALREADY been begun via
   * `await tx.begin()`; the store does NOT begin or commit.
   *
   * Atomicity contract: the caller MUST use the SAME `Transaction`
   * throughout the business-state write path (EF Core, TypeORM,
   * Drizzle, raw mssql) — wrapping `enqueue` in a different
   * `Transaction` breaks the outbox pattern's atomicity guarantee.
   *
   * Runtime guard: a fresh `Transaction` whose `.begin()` was NOT
   * called silently runs against the pool with NO transactional
   * atomicity (because `new sql.Request(tx)` falls back to the pool
   * when `tx` is not bound to a connection). We detect this by probing
   * the documented internal state and throw `TypeError` instead of
   * silently violating atomicity. Catching `EALREADYBEGUN` after the
   * fact would mask a different caller bug.
   *
   * Headers are deep-copied before tracing injection — the store NEVER
   * mutates the caller's `msg.headers` object.
   *
   * SQL pitfalls defended against:
   *   - `OUTPUT inserted.message_id INTO @inserted` (not bare `OUTPUT`)
   *     to stay trigger-safe (SQL Server error 334).
   *   - The message_id is read from `result.recordsets.at(-1)[0]`
   *     (NOT `result.recordset`) because the multi-statement batch
   *     produces multiple recordsets and the recordset position of
   *     the trailing `SELECT` is implementation-dependent across
   *     mssql versions.
   *   - `COALESCE(@messageId, LOWER(CONVERT(NVARCHAR(36), NEWID())))`
   *     server-mints when the caller omits `messageId`, so callers
   *     without access to `crypto` still work.
   *
   * @returns the message id (caller-supplied or server-generated).
   */
  async enqueue(
    tx: mssql.Transaction,
    msg: OutboxMessageInput & { traceId?: string },
  ): Promise<string> {
    if (tx === undefined || tx === null) {
      throw new TypeError(
        "MssqlStore.enqueue: tx (mssql.Transaction) is required and must have been begun via await tx.begin()",
      );
    }
    assertTransactionBegun(tx);

    // Defensive copy — never mutate the caller's headers object.
    const headers = { ...(msg.headers ?? {}) };
    this.tracing?.inject(headers);

    const sql = `
DECLARE @inserted TABLE (message_id NVARCHAR(64) NOT NULL);

INSERT INTO ${this.qualifiedTable}
    (message_id, aggregate_type, aggregate_id, topic, [key],
     payload, headers, trace_id, status)
OUTPUT inserted.message_id INTO @inserted (message_id)
VALUES
    (COALESCE(@messageId, CONVERT(NVARCHAR(64), LOWER(CONVERT(NVARCHAR(36), NEWID())))),
     @aggregateType, @aggregateId, @topic, @key,
     @payload, @headers, @traceId, 0);

SELECT message_id FROM @inserted;
`;

    const request = new mssql.Request(tx);
    // Explicit input types — see class-level JSDoc on why inferred
    // types are unsafe. Naming the parameter (never @p1) avoids
    // collision with the names tedious uses internally for sp_executesql.
    request.input("messageId", mssql.NVarChar(64), msg.messageId ?? null);
    request.input("aggregateType", mssql.NVarChar(128), msg.aggregateType);
    request.input("aggregateId", mssql.NVarChar(128), msg.aggregateId);
    request.input("topic", mssql.NVarChar(256), msg.topic);
    request.input("key", mssql.NVarChar(256), msg.key ?? null);
    request.input(
      "payload",
      mssql.NVarChar(mssql.MAX),
      JSON.stringify(msg.payload),
    );
    request.input(
      "headers",
      mssql.NVarChar(mssql.MAX),
      JSON.stringify(headers),
    );
    request.input("traceId", mssql.NVarChar(64), msg.traceId ?? null);

    // Multi-statement: DECLARE @inserted ... INSERT ... SELECT. Must go
    // through batch(); query() would route to sp_executesql which cannot
    // carry the table variable cleanly across statements.
    const result = await request.batch(sql);

    // The OUTPUT clause lands in result.recordsets, NOT result.output
    // (output is for declared OUTPUT *parameters*, a different feature).
    // The trailing SELECT * FROM @inserted is the last recordset; the
    // INSERT ... OUTPUT INTO also emits a recordset on some versions and
    // its position is implementation-dependent, so we always read the
    // LAST one rather than result.recordset.
    const recordsets = result.recordsets as Array<Array<{ message_id: string }>>;
    const last = recordsets.at(-1);
    const first = last?.[0];
    if (!first) {
      throw new Error(
        "MssqlStore.enqueue: INSERT ... OUTPUT did not return a message_id (driver state corruption?)",
      );
    }
    return first.message_id;
  }

  /**
   * Atomically claim up to `batchSize` due rows. At any wall-clock moment
   * AT MOST one row per `aggregate_id` is in `processing` status (the
   * head-of-aggregate invariant — see the class-level JSDoc for the
   * lock-hint protocol that enforces it).
   *
   * Due rows include:
   *   - `pending`(0) (gated by `next_retry_at` if set) — OMITTED when
   *     `claimFailedOnly = true` (streaming-relay mode).
   *   - `failed`(3) whose `next_retry_at` has elapsed (or is NULL).
   *   - `processing`(1) whose `claimed_at` is older than the visibility
   *     timeout (the reaper branch). The reaper INCREMENTS `attempts`
   *     so crash-looped rows can reach `dead` via the relay-side
   *     maxAttempts policy — this DIVERGES from Postgres/MySQL which
   *     currently leave `attempts` untouched on reap (an open
   *     cross-adapter design item).
   *
   * The store owns its own short-lived `BEGIN TRAN`/`COMMIT` via
   * `Request.batch()` (NOT a `sql.Transaction`) — the multi-statement
   * batch contains `DECLARE @claimed TABLE`, `SET XACT_ABORT ON`,
   * `BEGIN TRY` / `BEGIN CATCH`, so it cannot share state with
   * `sp_executesql`.
   *
   * The `${pendingPredicate}` token in the SQL template is a literal
   * string-replace marker — it expands either to the pending-row clause
   * or to the empty string. It is NEVER a SQL parameter, so it is
   * substituted with one of two fixed string constants (no
   * interpolation of user input).
   *
   * Throws if `batchSize <= 0` or `> 10_000`.
   *
   * @returns claimed rows in id-ascending order (the trailing
   *          `SELECT ... ORDER BY id` is the SOLE source of intra-batch
   *          ordering; the `OUTPUT inserted.* INTO @claimed` is
   *          unordered).
   */
  async claimBatch(batchSize: number): Promise<OutboxRecord[]> {
    if (!Number.isInteger(batchSize) || batchSize <= 0) {
      throw new TypeError(
        `MssqlStore.claimBatch: batchSize must be a positive integer, got ${String(batchSize)}`,
      );
    }
    if (batchSize > MAX_BATCH_SIZE) {
      throw new TypeError(
        `MssqlStore.claimBatch: batchSize ${batchSize} exceeds ${MAX_BATCH_SIZE} (the claim is a TOP scan — chunk into smaller calls)`,
      );
    }

    // The template literal is built ONCE per call; the only varying piece
    // is the pendingPredicate which is one of two fixed string constants
    // (NOT a user value). Identifiers are pre-validated in the constructor.
    const pendingPredicate = this.claimFailedOnly
      ? ""
      : "(o.status = 0 AND (o.next_retry_at IS NULL OR o.next_retry_at <= SYSUTCDATETIME())) OR";

    const sql = `
SET XACT_ABORT ON;
IF @@TRANCOUNT > 0 ROLLBACK;
BEGIN TRY
  BEGIN TRAN;

  DECLARE @claimed TABLE (
    id              BIGINT        NOT NULL PRIMARY KEY,
    message_id      NVARCHAR(64)  NOT NULL,
    aggregate_type  NVARCHAR(128) NOT NULL,
    aggregate_id    NVARCHAR(128) NOT NULL,
    topic           NVARCHAR(256) NOT NULL,
    [key]           NVARCHAR(256)     NULL,
    payload         NVARCHAR(MAX) NOT NULL,
    headers         NVARCHAR(MAX) NOT NULL,
    trace_id        NVARCHAR(64)      NULL,
    status          TINYINT       NOT NULL,
    attempts        INT           NOT NULL,
    next_retry_at   DATETIME2(3)      NULL,
    created_at      DATETIME2(3)  NOT NULL,
    processed_at    DATETIME2(3)      NULL
  );

  ;WITH claimable AS (
      SELECT TOP (@batchSize) o.id
      FROM   ${this.qualifiedTable} AS o WITH (READCOMMITTEDLOCK, READPAST, UPDLOCK, ROWLOCK)
      WHERE  (
                ${pendingPredicate}
                (o.status = 3 AND (o.next_retry_at IS NULL OR o.next_retry_at <= SYSUTCDATETIME()))
                OR (o.status = 1 AND o.claimed_at IS NOT NULL
                      AND o.claimed_at < DATEADD(MILLISECOND, -@claimTimeoutMs, SYSUTCDATETIME()))
             )
        -- Inner reference INTENTIONALLY has NO READPAST: must briefly
        -- BLOCK on a sibling's UPDLOCK so the head-of-aggregate guard
        -- sees the in-flight row and refuses to overtake it.
        AND NOT EXISTS (
                SELECT 1
                FROM   ${this.qualifiedTable} AS e WITH (READCOMMITTEDLOCK)
                WHERE  e.aggregate_id = o.aggregate_id
                  AND  e.id < o.id
                  AND  e.status IN (0, 1, 3)
            )
      ORDER BY o.id
  )
  UPDATE outer_o
  SET    status     = 1,
         claimed_at = SYSUTCDATETIME(),
         attempts   = CASE WHEN outer_o.status = 1 THEN outer_o.attempts + 1 ELSE outer_o.attempts END
  OUTPUT inserted.id,             inserted.message_id,    inserted.aggregate_type,
         inserted.aggregate_id,   inserted.topic,         inserted.[key],
         inserted.payload,        inserted.headers,       inserted.trace_id,
         inserted.status,         inserted.attempts,      inserted.next_retry_at,
         inserted.created_at,     inserted.processed_at
  INTO   @claimed
  FROM   ${this.qualifiedTable} AS outer_o WITH (READCOMMITTEDLOCK, ROWLOCK, UPDLOCK)
  JOIN   claimable c ON outer_o.id = c.id
  OPTION (MAXDOP 1);

  SELECT id, message_id, aggregate_type, aggregate_id, topic, [key],
         payload, headers, trace_id, status, attempts, next_retry_at,
         created_at, processed_at
  FROM   @claimed
  ORDER BY id;

  COMMIT;
END TRY
BEGIN CATCH
  IF @@TRANCOUNT > 0 ROLLBACK;
  THROW;
END CATCH;
`;

    const request = this.pool.request();
    request.input("batchSize", mssql.Int, batchSize);
    request.input("claimTimeoutMs", mssql.Int, this.claimTimeoutMs);

    // batch() (NOT query()): multi-statement block with DECLARE @t,
    // SET XACT_ABORT, BEGIN TRY/TRAN. sp_executesql cannot carry the
    // table variable across statements cleanly.
    const result = await request.batch(sql);

    // The trailing SELECT * FROM @claimed ORDER BY id is the last
    // recordset. result.recordset would also work in current mssql but
    // recordsets.at(-1) is robust to any future driver change that
    // emits an extra rows-affected recordset between the UPDATE and
    // the trailing SELECT.
    const recordsets = result.recordsets as Array<Array<OutboxRow>>;
    const rows = recordsets.at(-1) ?? [];
    return rows.map(rowToRecord);
  }

  /**
   * Batched ack via `OPENJSON(@ids) WITH (id BIGINT '$')`. No-op on
   * empty input (short-circuits BEFORE the SQL is built — `OPENJSON('[]')`
   * is legal but a wasted round-trip).
   *
   * Defence-in-depth:
   *   - Each id is asserted to match `/^\d+$/` at the TS boundary BEFORE
   *     serialization, so bad input fails with a clear caller-line stack
   *     trace instead of T-SQL's generic conversion error.
   *   - `OPENJSON` default (lax) mode is used — SQL Server does not
   *     support the `STRICT` keyword inside `OPENJSON WITH (...)`
   *     (`STRICT` exists only on `JSON_VALUE` / `JSON_QUERY`). Our id
   *     validation upstream covers the same hazard: every id is `/^\d+$/`
   *     by the time it reaches the SQL, so lax mode cannot silently drop
   *     a bad element.
   *   - `@@ROWCOUNT === recordIds.length` is verified. A mismatch means
   *     the reaper raced us, the caller passed an unknown id, or another
   *     worker already markedDone the same id — every possibility
   *     deserves a loud error rather than a silent miss.
   *
   * Single-statement (after the DECLARE @ids parameter): runs through
   * `Request.query()` so it benefits from `sp_executesql` plan
   * caching.
   */
  async markDone(recordIds: string[]): Promise<void> {
    if (recordIds.length === 0) return;
    for (const id of recordIds) {
      if (typeof id !== "string" || !/^\d+$/.test(id)) {
        throw new TypeError(
          `MssqlStore.markDone: every id must be a numeric string matching /^\\d+$/, got ${JSON.stringify(id)}`,
        );
      }
    }

    const sql = `
UPDATE o
SET    status       = ${DONE},
       processed_at = SYSUTCDATETIME()
FROM   ${this.qualifiedTable} AS o
JOIN   OPENJSON(@ids) WITH (id BIGINT '$') AS j
       ON o.id = j.id;
`;

    const request = this.pool.request();
    // JSON.stringify yields e.g. '["1","2","3"]' — OPENJSON STRICT casts
    // each to BIGINT. Sending one JSON parameter is one round-trip
    // regardless of recordIds.length.
    request.input("ids", mssql.NVarChar(mssql.MAX), JSON.stringify(recordIds));
    const result = await request.query(sql);
    const rowsAffected = (result.rowsAffected ?? [0])[0] ?? 0;
    if (rowsAffected !== recordIds.length) {
      throw new Error(
        `MssqlStore.markDone: rowsAffected (${rowsAffected}) did not equal recordIds.length (${recordIds.length}); the reaper or another worker may have raced this ack`,
      );
    }
  }

  /**
   * Single-row failure transition; ALWAYS bumps `attempts`.
   *
   * Hard contract: when `status='failed'`, `nextRetryAt` MUST be
   * non-null. Throws `TypeError` on `(null, 'failed')` to prevent the
   * instant-redrive hot loop where the next claim cycle would
   * immediately pick the row back up. `status='dead'` MAY pass null
   * (terminal state, never reclaimed).
   *
   * Status code mapping is taken from {@link OUTBOX_STATUS_CODE} so
   * the constant lives in exactly one place. Bound as `sql.TinyInt`.
   *
   * Single statement: `Request.query()` (plan-cached).
   */
  async markFailed(
    recordId: string,
    nextRetryAt: Date | null,
    status: "failed" | "dead",
  ): Promise<void> {
    if (status === "failed" && nextRetryAt === null) {
      throw new TypeError(
        "markFailed: nextRetryAt cannot be null when status is 'failed' (would cause instant re-claim hot loop)",
      );
    }
    const code = OUTBOX_STATUS_CODE[status];

    const sql = `
UPDATE ${this.qualifiedTable}
SET    status        = @status,
       attempts      = attempts + 1,
       next_retry_at = @nextRetryAt
WHERE  id = @id;
`;
    const request = this.pool.request();
    // BIGINT bound from a JS string — tedious accepts the string form
    // and avoids any 2^53 precision risk that would come from
    // round-tripping through Number.
    request.input("id", mssql.BigInt, recordId);
    request.input("status", mssql.TinyInt, code);
    // DATETIME2(3) explicitly — inferred Date binds as legacy DATETIME
    // (3.33ms rounding) and a CONVERT_IMPLICIT disables index seeks.
    request.input("nextRetryAt", mssql.DateTime2(3), nextRetryAt);
    await request.query(sql);
  }

  /**
   * Backpressure re-queue: status=3, claimed_at=NULL, attempts
   * UNCHANGED. Clearing `claimed_at` keeps the reaper from racing the
   * row. Distinct from {@link markFailed} which always bumps attempts —
   * backpressure (client-side producer queue full) is a "slow down"
   * signal, not a per-record failure, and should not burn the retry
   * budget. Mirrors the Postgres / MySQL `requeue` contract.
   */
  async requeue(recordId: string, retryAt: Date): Promise<void> {
    const sql = `
UPDATE ${this.qualifiedTable}
SET    status        = ${FAILED},
       claimed_at    = NULL,
       next_retry_at = @retryAt
WHERE  id = @id;
`;
    const request = this.pool.request();
    request.input("id", mssql.BigInt, recordId);
    request.input("retryAt", mssql.DateTime2(3), retryAt);
    await request.query(sql);
  }

  /**
   * Retention sweep over `done`(2) rows; loops in batches, oldest-first.
   *
   * The cutoff is computed in TS (`new Date(Date.now() - olderThanMs)`)
   * and bound as `sql.DateTime2(3)`. This dodges the `sql.Int`
   * 32-bit-ms overflow: `DATEADD(MILLISECOND, -<ms>, ...)` overflows
   * past ~24.85 days (2^31-1 ms), and common retention windows
   * (30 / 60 / 90 days in ms) all blow past that. By computing the
   * cutoff JS-side we keep the SQL parameter as a plain DATETIME2.
   *
   * `DELETE TOP (n)` does NOT accept `ORDER BY` directly, so the
   * statement uses the inner-subquery pattern:
   *   `DELETE FROM t WHERE id IN (SELECT TOP (@batchSize) id FROM t
   *    WHERE ... ORDER BY processed_at, id);`
   * which restores the oldest-first contract that Postgres
   * `ORDER BY id LIMIT` and MySQL `ORDER BY id LIMIT` provide.
   *
   * `OUTPUT deleted.id INTO @deleted` is the trigger-safe pattern (see
   * `enqueue` for the error 334 context) and powers the `COUNT(*)`
   * that the loop uses to decide whether to continue.
   *
   * `maxRows` is a SOFT cap — the loop terminates AFTER an iteration
   * that crosses it, so actual deletion may exceed `maxRows` by up to
   * `batchSize - 1` (parity with the Postgres and MySQL adapters).
   *
   * Each iteration is its own `Request.query()` — do NOT share a
   * `Transaction` across iterations.
   *
   * @returns total rows deleted across all iterations.
   */
  async purgeDone(opts: PurgeDoneOptions): Promise<number> {
    if (!Number.isFinite(opts.olderThanMs) || opts.olderThanMs < 0) {
      throw new TypeError(
        `MssqlStore.purgeDone: olderThanMs must be a non-negative finite number, got ${String(opts.olderThanMs)}`,
      );
    }
    const batchSize = opts.batchSize ?? DEFAULT_PURGE_BATCH_SIZE;
    if (!Number.isInteger(batchSize) || batchSize <= 0) {
      throw new TypeError(
        `MssqlStore.purgeDone: batchSize must be a positive integer, got ${String(batchSize)}`,
      );
    }
    if (batchSize > MAX_BATCH_SIZE) {
      throw new TypeError(
        `MssqlStore.purgeDone: batchSize ${batchSize} exceeds ${MAX_BATCH_SIZE}`,
      );
    }

    const sql = `
DECLARE @deleted TABLE (id BIGINT NOT NULL);

DELETE FROM ${this.qualifiedTable}
OUTPUT deleted.id INTO @deleted (id)
WHERE  id IN (
    SELECT TOP (@batchSize) id
    FROM   ${this.qualifiedTable}
    WHERE  status = ${DONE}
      AND  processed_at IS NOT NULL
      AND  processed_at < @cutoff
    ORDER BY processed_at, id
);

SELECT COUNT(*) AS deleted_count FROM @deleted;
`;

    let total = 0;
    for (;;) {
      // Cutoff recomputed each iteration so a long purge does not let
      // newer "done" rows escape the sweep (the next iteration's
      // window has moved forward by a few ms).
      const cutoff = new Date(Date.now() - opts.olderThanMs);

      const request = this.pool.request();
      request.input("cutoff", mssql.DateTime2(3), cutoff);
      request.input("batchSize", mssql.Int, batchSize);

      // Multi-statement (DECLARE + DELETE + SELECT) → batch().
      const result = await request.batch(sql);
      const recordsets = result.recordsets as Array<
        Array<{ deleted_count: number }>
      >;
      const last = recordsets.at(-1);
      const deleted = last?.[0]?.deleted_count ?? 0;

      total += deleted;
      if (deleted < batchSize) break;
      if (opts.maxRows !== undefined && total >= opts.maxRows) break;
    }
    return total;
  }

  /**
   * No-op. The pool lifecycle belongs to the caller (the store does NOT
   * call `pool.connect()`).
   */
  async init(): Promise<void> {
    return;
  }

  /**
   * No-op. The pool lifecycle belongs to the caller (the store does NOT
   * call `pool.close()`).
   */
  async close(): Promise<void> {
    return;
  }
}

/**
 * Probe whether a caller-supplied `mssql.Transaction` has had `.begin()`
 * called on it. Throws `TypeError` if not.
 *
 * Why a pre-check when the driver already errors?
 *
 *   The `mssql` driver itself raises `ENOTBEGUN` from `Transaction.acquire()`
 *   when a `Request` is sent against a not-yet-begun `Transaction` — that
 *   error surfaces inside `enqueue` regardless of this probe. The probe
 *   exists to give a CLEARER, EARLIER `TypeError` whose message names the
 *   exact caller-side fix (`await tx.begin()` first), instead of a generic
 *   driver code the user has to look up.
 *
 * Implementation:
 *   - Reads the documented-but-internal `_acquiredConnection` field set by
 *     `Transaction#begin()` (mssql source: `lib/base/transaction.js`).
 *     Wrapped in try/catch so a future rename downgrades us to the driver's
 *     own `ENOTBEGUN` rather than crashing here.
 *   - Also checks `_aborted` — using a rolled-back Transaction is a separate
 *     caller bug worth surfacing distinctly.
 *   - Deliberately does NOT call `new mssql.Request(tx).query("SELECT 1")`
 *     as a probe — that would consume a tx state slot under driver versions
 *     where the request DOES route through the tx connection.
 */
function assertTransactionBegun(tx: mssql.Transaction): void {
  let internal: { _acquiredConnection?: unknown; _aborted?: boolean };
  try {
    internal = tx as unknown as typeof internal;
  } catch {
    // If a future mssql version makes these fields non-accessible (e.g.
    // class fields with a Proxy guard), let the driver's own ENOTBEGUN
    // surface in the subsequent .query() instead.
    return;
  }
  let begun: boolean;
  try {
    begun =
      internal._acquiredConnection !== undefined &&
      internal._acquiredConnection !== null;
  } catch {
    return;
  }
  if (!begun) {
    throw new TypeError(
      "MssqlStore.enqueue: tx.begin() has not been called (or has not yet resolved). " +
        "The store does NOT begin transactions — the caller MUST `await tx.begin()` before enqueue, " +
        "and use the SAME Transaction for the business-state writes so they commit atomically.",
    );
  }
  let aborted: boolean;
  try {
    aborted = internal._aborted === true;
  } catch {
    return;
  }
  if (aborted) {
    throw new TypeError(
      "MssqlStore.enqueue: tx has been aborted (rolled back). Begin a fresh Transaction before retrying.",
    );
  }
}
