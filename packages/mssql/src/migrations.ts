import { assertIdent } from "./ident.js";

/**
 * Options for `createMigrationSql`.
 */
export interface CreateMigrationSqlOptions {
  /**
   * Owning schema. Default `"dbo"`. Validated by `assertIdent` BEFORE composition.
   *
   * Schema qualification is mandatory: without it, a non-dbo default schema (Azure
   * AD logins, contained DB users, Availability Group per-app schemas) causes the
   * `IF OBJECT_ID` guard to look at `dbo.outbox` while `CREATE TABLE` lands
   * elsewhere — producing duplicate tables across schemas, or error 2714 on the
   * second migration run.
   */
  schema?: string;
  /**
   * Opt-in to SQL Server 2025+ native `json` column type for payload/headers.
   *
   * When `true`: `payload` and `headers` are emitted as `json` and the
   * `ISJSON(...) = 1` CHECK constraints are OMITTED — SQL Server 2025+ rejects
   * `CREATE TABLE` if an `ISJSON` CHECK is declared on a `json`-typed column
   * (native `json` enforces validity intrinsically).
   *
   * When `false` (default): `payload` and `headers` are `NVARCHAR(MAX)` with
   * `CHECK (ISJSON(col) = 1)`. This works on every SQL Server 2016 SP1+,
   * Azure SQL Database, and Managed Instance.
   *
   * Wire-level behaviour is identical either way — TDS still serialises
   * `json` as `NVARCHAR(MAX)`, so the store's `JSON.stringify` in /
   * `JSON.parse` out code path is unchanged.
   */
  useNativeJson?: boolean;
}

/**
 * Generate the DDL for the SQL Server outbox table, parameterized by schema and
 * table name. Kept as a string template (not a file read) so it works regardless
 * of how the package is bundled or where it's installed.
 *
 * Requirements:
 *  - **SQL Server 2016 SP1+** (or Azure SQL Database / Managed Instance) for
 *    `OPENJSON`, `ISJSON`, and filtered indexes (`WHERE` clause) — compatibility
 *    level 130 or higher.
 *  - When `useNativeJson` is `true`: **SQL Server 2025+** (or current Azure SQL
 *    Database) for the native `json` type.
 *
 * The emitted block is idempotent at the per-object level:
 *  - The `CREATE TABLE` lives inside a single `IF OBJECT_ID(...) IS NULL` guard.
 *  - Each of the three indexes is its own `IF NOT EXISTS (SELECT 1 FROM sys.indexes ...)`
 *    block, so a partial deployment (table exists, indexes missing — e.g.
 *    hand-rolled DBA script) can be repaired by re-running the migration.
 *
 * The returned string is multi-statement and MUST be executed via
 * `mssql.Request.batch()` (NOT `Request.query()` — the latter routes through
 * `sp_executesql`, which cannot carry session-scoped state cleanly across
 * `IF`/`BEGIN`/`END` boundaries).
 *
 * Both `schema` and `table` are passed through `assertIdent` BEFORE any string
 * composition happens — this is the only injection defence on this code path.
 *
 * @param table  Outbox table name. Default `"outbox"`.
 * @param opts   Schema + native-json toggle. Defaults: `{ schema: "dbo", useNativeJson: false }`.
 */
export function createMigrationSql(
  table = "outbox",
  opts: CreateMigrationSqlOptions = {},
): string {
  const { schema = "dbo", useNativeJson = false } = opts;
  // Validate BOTH identifiers BEFORE composing any SQL — fail-fast on invalid
  // input and prevent any chance of injection through the {schema}/{table}
  // interpolation below.
  const s = assertIdent(schema, "schema");
  const t = assertIdent(table, "table");

  // Native `json` columns (SQL 2025+) reject ISJSON CHECK constraints in
  // CREATE TABLE, so the column lines and the CHECK clauses diverge across the
  // two modes. Default headers value remains N'{}' in both modes.
  const payloadColumn = useNativeJson
    ? `        payload         json          NOT NULL,`
    : `        payload         NVARCHAR(MAX) NOT NULL CONSTRAINT [CK_${t}_payload_json] CHECK (ISJSON(payload) = 1),`;

  const headersColumn = useNativeJson
    ? `        headers         json          NOT NULL CONSTRAINT [DF_${t}_headers] DEFAULT N'{}',`
    : `        headers         NVARCHAR(MAX) NOT NULL CONSTRAINT [DF_${t}_headers] DEFAULT N'{}'
                                     CONSTRAINT [CK_${t}_headers_json] CHECK (ISJSON(headers) = 1),`;

  return `
IF OBJECT_ID(N'[${s}].[${t}]', N'U') IS NULL
BEGIN
    CREATE TABLE [${s}].[${t}] (
        id              BIGINT        IDENTITY(1,1) NOT NULL,
        message_id      NVARCHAR(64)  NOT NULL,
        aggregate_type  NVARCHAR(128) NOT NULL,
        aggregate_id    NVARCHAR(128) NOT NULL,
        topic           NVARCHAR(256) NOT NULL,
        [key]           NVARCHAR(256)     NULL,
${payloadColumn}
${headersColumn}
        trace_id        NVARCHAR(64)      NULL,
        status          TINYINT       NOT NULL CONSTRAINT [DF_${t}_status]     DEFAULT (0),
        attempts        INT           NOT NULL CONSTRAINT [DF_${t}_attempts]   DEFAULT (0),
        next_retry_at   DATETIME2(3)      NULL,
        claimed_at      DATETIME2(3)      NULL,
        created_at      DATETIME2(3)  NOT NULL CONSTRAINT [DF_${t}_created_at] DEFAULT (SYSUTCDATETIME()),
        processed_at    DATETIME2(3)      NULL,
        CONSTRAINT [PK_${t}]            PRIMARY KEY CLUSTERED (id),
        CONSTRAINT [UQ_${t}_message_id] UNIQUE NONCLUSTERED (message_id),
        -- Invariant: a processing(1) row ALWAYS has a non-null claimed_at, so the
        -- reaper's claimed_at-window check can never miss a stuck row.
        CONSTRAINT [CK_${t}_claimed_at_when_processing]
                   CHECK (status <> 1 OR claimed_at IS NOT NULL)
    );
END;

-- Drives the head-of-aggregate NOT EXISTS probe inside the claim CTE.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE  name = N'IX_${t}_agg_id_id'
      AND  object_id = OBJECT_ID(N'[${s}].[${t}]')
)
    CREATE NONCLUSTERED INDEX [IX_${t}_agg_id_id]
        ON [${s}].[${t}] (aggregate_id, id)
        INCLUDE (status);

-- Filtered to the "claimable" universe so the claim scan is tiny.
-- Matches the predicate: pending(0) / failed(3) due, processing(1) reaper.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE  name = N'IX_${t}_claim_ready'
      AND  object_id = OBJECT_ID(N'[${s}].[${t}]')
)
    CREATE NONCLUSTERED INDEX [IX_${t}_claim_ready]
        ON [${s}].[${t}] (status, next_retry_at, id)
        INCLUDE (aggregate_id, claimed_at)
        WHERE status IN (0, 1, 3);

-- Retention scan for purgeDone.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE  name = N'IX_${t}_done_processed_at'
      AND  object_id = OBJECT_ID(N'[${s}].[${t}]')
)
    CREATE NONCLUSTERED INDEX [IX_${t}_done_processed_at]
        ON [${s}].[${t}] (processed_at, id)
        WHERE status = 2;
`.trim();
}

/**
 * Emit ONLY the `IX_<table>_done_processed_at` filtered index DDL. Useful for
 * upgrade paths where an operator wants to add the retention index to an
 * existing outbox table (e.g. retention scans started to dominate I/O after
 * volume grew).
 *
 * The default `createMigrationSql` already includes this index — this helper is
 * for the case where it was previously skipped or dropped. It is idempotent:
 * re-running it on a table that already has the index is a no-op.
 *
 * Both `schema` and `table` are passed through `assertIdent` BEFORE composition
 * — defence in depth for the `createRetentionIndexSql` entry point even though
 * it shares the validation logic with `createMigrationSql`.
 *
 * @param table  Outbox table name. Default `"outbox"`.
 * @param schema Owning schema. Default `"dbo"`.
 */
export function createRetentionIndexSql(
  table = "outbox",
  schema = "dbo",
): string {
  // Defence in depth: every top-level public migration export validates its
  // own identifiers BEFORE composing SQL — never rely on a downstream guard.
  const s = assertIdent(schema, "schema");
  const t = assertIdent(table, "table");

  return `
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE  name = N'IX_${t}_done_processed_at'
      AND  object_id = OBJECT_ID(N'[${s}].[${t}]')
)
    CREATE NONCLUSTERED INDEX [IX_${t}_done_processed_at]
        ON [${s}].[${t}] (processed_at, id)
        WHERE status = 2;
`.trim();
}

/**
 * Options for `createServiceBrokerSetupSql`.
 *
 * Service Broker URN-style names (`serviceName`, `initiatorServiceName`,
 * `contractName`, `messageTypeName`) legitimately contain forward slashes
 * (`//eventferry/outbox/...`), so they bypass `assertIdent`. They are
 * embedded verbatim as `N'...'` literals inside `QUOTENAME(...)` system-catalog
 * lookups and `[bracketed]` create-statement names. The other identifiers
 * (`table`, `schema`, `queueName`, `initiatorQueueName`, `triggerName`,
 * `cleanupProcName`) are SQL Server T-SQL identifiers and MUST pass
 * `assertIdent` before composition.
 */
export interface CreateServiceBrokerSetupSqlOptions {
  /** Outbox table the AFTER INSERT trigger fires on. Default `"outbox"`. */
  table?: string;
  /** Owning schema for the outbox table + Service Broker queues. Default `"dbo"`. */
  schema?: string;
  /** Target queue name (waker RECEIVEs from this). Default `"OutboxWakerQueue"`. */
  queueName?: string;
  /**
   * Initiator queue name. Receives EndDialog / Error system messages from the
   * trigger's BEGIN DIALOG side; drained by the cleanup activation proc to
   * prevent `sys.conversation_endpoints` from accumulating. Default
   * `"OutboxWakerInitiatorQueue"`.
   */
  initiatorQueueName?: string;
  /** Target service URN. Default `"//eventferry/outbox/WakerTargetService"`. */
  serviceName?: string;
  /** Initiator service URN. Default `"//eventferry/outbox/WakerInitiatorService"`. */
  initiatorServiceName?: string;
  /** Contract URN. Default `"//eventferry/outbox/WakerContract"`. */
  contractName?: string;
  /** Wakeup message type URN. Default `"//eventferry/outbox/Wakeup"`. */
  messageTypeName?: string;
  /** AFTER INSERT trigger name. Default `"tr_<table>_outbox_waker"`. */
  triggerName?: string;
  /**
   * Procedure name for the initiator-side cleanup activation proc that drains
   * EndDialog/Error to prevent `conversation_endpoints` leak. Default
   * `"OutboxWaker_InitiatorCleanup"`.
   */
  cleanupProcName?: string;
}

/**
 * Generate an idempotent T-SQL block that provisions every Service Broker
 * object the `MssqlServiceBrokerWaker` listens on:
 *
 *   1. Azure SQL Database refusal (`SERVERPROPERTY('EngineEdition') = 5`) —
 *      Service Broker is not supported there; we `RAISERROR` + `RETURN`
 *      rather than silently producing an unusable setup.
 *   2. `ALTER DATABASE ... SET ENABLE_BROKER` guarded by (a) the MI skip —
 *      EngineEdition `8` already has broker on and `ALTER DATABASE ... SET
 *      ENABLE_BROKER` is disallowed on Managed Instance — and (b) the
 *      `is_broker_enabled = 1` short-circuit so re-runs are no-ops.
 *   3. `CREATE MESSAGE TYPE` (wakeup payload type) — `IF NOT EXISTS`.
 *   4. `CREATE CONTRACT` binding the message type as `SENT BY INITIATOR` —
 *      `IF NOT EXISTS`.
 *   5. Target queue (`POISON_MESSAGE_HANDLING OFF`, `RETENTION OFF`) —
 *      `IF NOT EXISTS`. Poison-message handling is OFF because the waker's
 *      RECEIVE always commits without re-raising, so the 5-rollback
 *      auto-disable would only be a footgun.
 *   6. Initiator queue — `IF NOT EXISTS`. Sized identically; the cleanup
 *      activation proc handles its drain.
 *   7. Target + initiator services — `IF NOT EXISTS`, both bound to the
 *      contract.
 *   8. `CREATE OR ALTER PROCEDURE` for the initiator-cleanup activation proc.
 *      Per the design's *Correctness lens*: ending the dialog on the
 *      INITIATOR side immediately (as the trigger does) is safe ONLY because
 *      this activation proc continually drains the resulting EndDialog +
 *      Error messages — otherwise `sys.conversation_endpoints` accumulates
 *      until queue starvation. `EXECUTE AS OWNER` + `MAX_QUEUE_READERS = 1`
 *      to guarantee single-reader semantics and avoid contention on the
 *      initiator queue.
 *   9. `ALTER QUEUE ... WITH ACTIVATION` to wire the cleanup proc onto the
 *      initiator queue.
 *  10. `CREATE OR ALTER TRIGGER` on `<schema>.<table>` AFTER INSERT.
 *      *Correctness lens*: the trigger uses an `IF EXISTS (SELECT 1 FROM
 *      inserted)` guard + one `BEGIN DIALOG ... SEND` per statement (NOT
 *      per row) — bulk inserts of N rows produce ONE wake message, not N,
 *      keeping the queue depth proportional to write traffic, not row count.
 *      `END CONVERSATION @handle` runs on the initiator side immediately
 *      after `SEND` (fire-and-forget wakeup). The design notes this is
 *      acceptable specifically because the initiator-cleanup activation
 *      proc (step 8) drains the resulting endpoint cleanup messages — see
 *      the file-level correctness review.
 *
 * Identifier validation:
 *   - `table`, `schema`, `queueName`, `initiatorQueueName`, `triggerName`,
 *     `cleanupProcName` are validated via `assertIdent` BEFORE composition
 *     (defence in depth — never trust the caller).
 *   - `serviceName`, `initiatorServiceName`, `contractName`,
 *     `messageTypeName` are Service Broker URNs that legitimately contain
 *     `/`, so they bypass `assertIdent`. They are embedded as `N'...'`
 *     literals inside system-catalog `name = N'...'` lookups and as
 *     `[bracketed]` names in `CREATE SERVICE` / `CREATE CONTRACT` /
 *     `CREATE MESSAGE TYPE` statements. Per the design: URNs are treated
 *     as opaque strings — no `'`, `;`, `[`, `]` characters should appear
 *     in them, and operators MUST treat them as configuration, not user
 *     input.
 *
 * The returned string is multi-statement and MUST be executed via
 * `mssql.Request.batch()` (NOT `Request.query()`).
 */
export function createServiceBrokerSetupSql(
  opts: CreateServiceBrokerSetupSqlOptions = {},
): string {
  const tableRaw = opts.table ?? "outbox";
  const schemaRaw = opts.schema ?? "dbo";
  const queueRaw = opts.queueName ?? "OutboxWakerQueue";
  const initiatorQueueRaw = opts.initiatorQueueName ?? "OutboxWakerInitiatorQueue";
  const triggerRaw = opts.triggerName ?? `tr_${tableRaw}_outbox_waker`;
  const cleanupProcRaw = opts.cleanupProcName ?? "OutboxWaker_InitiatorCleanup";

  // Defence in depth: every public migration entrypoint validates its own
  // T-SQL identifiers BEFORE composing SQL — never rely on a downstream guard.
  const t = assertIdent(tableRaw, "table");
  const s = assertIdent(schemaRaw, "schema");
  const q = assertIdent(queueRaw, "queueName");
  const iq = assertIdent(initiatorQueueRaw, "initiatorQueueName");
  const trg = assertIdent(triggerRaw, "triggerName");
  const cleanupProc = assertIdent(cleanupProcRaw, "cleanupProcName");

  // URN-style names — legitimately contain '/'. Embedded as N'...' literals
  // for system-catalog lookups and as [bracketed] names in CREATE statements.
  // Treat these as opaque configuration; no quoting characters are expected.
  const svc = opts.serviceName ?? "//eventferry/outbox/WakerTargetService";
  const initSvc = opts.initiatorServiceName ?? "//eventferry/outbox/WakerInitiatorService";
  const contract = opts.contractName ?? "//eventferry/outbox/WakerContract";
  const msgType = opts.messageTypeName ?? "//eventferry/outbox/Wakeup";

  return `
-- 1. Azure SQL Database refusal: Service Broker is unsupported (EngineEdition=5).
--    RAISERROR + RETURN exits this batch cleanly without leaving partial state.
IF CAST(SERVERPROPERTY('EngineEdition') AS int) = 5
BEGIN
    RAISERROR(N'Service Broker is unsupported on Azure SQL Database (EngineEdition=5). Use the polling-only relay (omit the waker), or migrate to Azure SQL Managed Instance.', 16, 1);
    RETURN;
END;

-- 2. Enable broker. Skip on Managed Instance (EngineEdition=8) — broker is
--    already on there and ALTER DATABASE ... SET ENABLE_BROKER is disallowed.
--    Skip when is_broker_enabled = 1 so re-runs are no-ops.
IF CAST(SERVERPROPERTY('EngineEdition') AS int) <> 8
BEGIN
    DECLARE @db sysname = DB_NAME();
    IF NOT EXISTS (
        SELECT 1 FROM sys.databases WHERE name = @db AND is_broker_enabled = 1
    )
    BEGIN
        DECLARE @enableBrokerSql nvarchar(max) =
            N'ALTER DATABASE ' + QUOTENAME(@db) + N' SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;';
        EXEC sp_executesql @enableBrokerSql;
    END
END;

-- 3. Wakeup message type. Idempotent.
IF NOT EXISTS (SELECT 1 FROM sys.service_message_types WHERE name = N'${msgType}')
    CREATE MESSAGE TYPE [${msgType}] VALIDATION = NONE;

-- 4. Contract binding the message type SENT BY INITIATOR. Idempotent.
IF NOT EXISTS (SELECT 1 FROM sys.service_contracts WHERE name = N'${contract}')
    CREATE CONTRACT [${contract}] ([${msgType}] SENT BY INITIATOR);

-- 5. Target queue (waker RECEIVEs here). POISON_MESSAGE_HANDLING OFF because
--    the waker commits without re-raising — the 5-rollback auto-disable would
--    only be a footgun. RETENTION OFF because we never replay wake messages.
IF NOT EXISTS (
    SELECT 1 FROM sys.service_queues sq
    JOIN sys.schemas ss ON ss.schema_id = sq.schema_id
    WHERE sq.name = N'${q}' AND ss.name = N'${s}'
)
    CREATE QUEUE [${s}].[${q}]
        WITH STATUS = ON,
             RETENTION = OFF,
             POISON_MESSAGE_HANDLING (STATUS = OFF);

-- 6. Initiator queue. Drained by the cleanup activation proc below.
IF NOT EXISTS (
    SELECT 1 FROM sys.service_queues sq
    JOIN sys.schemas ss ON ss.schema_id = sq.schema_id
    WHERE sq.name = N'${iq}' AND ss.name = N'${s}'
)
    CREATE QUEUE [${s}].[${iq}]
        WITH STATUS = ON,
             RETENTION = OFF,
             POISON_MESSAGE_HANDLING (STATUS = OFF);

-- 7. Target + initiator services bound to the contract. Idempotent.
IF NOT EXISTS (SELECT 1 FROM sys.services WHERE name = N'${svc}')
    CREATE SERVICE [${svc}]
        ON QUEUE [${s}].[${q}] ([${contract}]);

IF NOT EXISTS (SELECT 1 FROM sys.services WHERE name = N'${initSvc}')
    CREATE SERVICE [${initSvc}]
        ON QUEUE [${s}].[${iq}] ([${contract}]);

-- 8. Initiator-cleanup activation procedure.
--    Correctness lens: END CONVERSATION on the INITIATOR side immediately
--    after SEND (in the trigger, step 10) is safe ONLY because this proc
--    continually drains the resulting EndDialog + Error messages from the
--    initiator queue. Without it, sys.conversation_endpoints grows
--    unbounded until queue starvation.
EXEC(N'
CREATE OR ALTER PROCEDURE [${s}].[${cleanupProc}]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @h UNIQUEIDENTIFIER;
    WHILE 1 = 1
    BEGIN
        WAITFOR (
            RECEIVE TOP (1) @h = conversation_handle
            FROM [${s}].[${iq}]
        ), TIMEOUT 1000;
        IF @h IS NULL BREAK;
        BEGIN TRY END CONVERSATION @h; END TRY BEGIN CATCH END CATCH
        SET @h = NULL;
    END
END;
');

-- 9. Wire activation: cleanup proc drains the initiator queue.
--    MAX_QUEUE_READERS = 1 guarantees single-reader semantics on cleanup;
--    EXECUTE AS OWNER avoids per-user permission entanglement.
ALTER QUEUE [${s}].[${iq}]
    WITH ACTIVATION (
        STATUS = ON,
        PROCEDURE_NAME = [${s}].[${cleanupProc}],
        MAX_QUEUE_READERS = 1,
        EXECUTE AS OWNER
    );

-- 10. AFTER INSERT trigger on the outbox table.
--     Correctness lens: ONE BEGIN DIALOG + SEND per STATEMENT (guarded by
--     EXISTS on inserted), NOT per row — bulk inserts of N rows produce
--     ONE wake message, keeping queue depth proportional to write traffic,
--     not row count. END CONVERSATION fires immediately on the initiator
--     side (fire-and-forget wakeup); safety relies on the initiator-cleanup
--     activation proc (step 8) draining the resulting endpoint messages.
EXEC(N'
CREATE OR ALTER TRIGGER [${s}].[${trg}]
ON [${s}].[${t}]
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM inserted) RETURN;
    DECLARE @h UNIQUEIDENTIFIER;
    BEGIN TRY
        BEGIN DIALOG CONVERSATION @h
            FROM SERVICE [${initSvc}]
            TO   SERVICE N''${svc}''
            ON CONTRACT [${contract}]
            WITH ENCRYPTION = OFF, LIFETIME = 3600;
        SEND ON CONVERSATION @h
            MESSAGE TYPE [${msgType}]
            (CAST(N''wake'' AS VARBINARY(MAX)));
        END CONVERSATION @h;
    END TRY
    BEGIN CATCH
        -- Severity 10 = informational, does NOT fail the user INSERT.
        -- The polling relay backstops any lost wake.
        DECLARE @msg NVARCHAR(2048) = ERROR_MESSAGE();
        RAISERROR(N''eventferry waker SEND failed: %s'', 10, 1, @msg) WITH LOG;
    END CATCH
END;
');
`.trim();
}
