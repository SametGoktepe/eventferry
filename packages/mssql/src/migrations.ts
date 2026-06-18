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
