import { assertIdent } from "./ident.js";

/**
 * Options for {@link createCdcEnablementSql}.
 *
 * All identifier fields (`table`, `schema`, `captureInstance`, `watermarkTable`,
 * `watermarkSchema`) are validated via {@link assertIdent} — they are
 * interpolated literally into T-SQL and must therefore be safe identifiers.
 *
 * `roleName` is validated by SQL Server itself (passed as an NVARCHAR parameter
 * to `sys.sp_cdc_enable_table`), so we accept any string or `null`. `null`
 * means "no gating role" — every login with SELECT on the source table can
 * read the change table.
 */
export interface CreateCdcEnablementSqlOptions {
  /** Source table name. Default `'outbox'`. */
  table?: string;
  /** Source schema name. Default `'dbo'`. */
  schema?: string;
  /**
   * SQL Server capture instance name. Defaults to `<schema>_<table>`
   * (the convention `sys.sp_cdc_enable_table` itself applies when
   * `@capture_instance` is NULL).
   */
  captureInstance?: string;
  /**
   * Optional gating role name passed to `sys.sp_cdc_enable_table`. `null`
   * (the default) emits `@role_name = NULL`, leaving access ungated.
   */
  roleName?: string | null;
  /**
   * `@supports_net_changes` flag. The relay only consumes inserts into the
   * outbox, so net changes (which require a primary key and add cost) buy
   * us nothing. Default `false`.
   */
  supportsNetChanges?: boolean;
  /**
   * Optional index name for net-change support. Defaults to `null` (engine
   * picks the PK). Only meaningful when `supportsNetChanges` is `true`.
   */
  indexName?: string | null;
  /** Watermark table name. Default `'outbox_cdc_watermark'`. */
  watermarkTable?: string;
  /** Watermark table schema. Default `'dbo'`. */
  watermarkSchema?: string;
}

/**
 * Build the idempotent T-SQL block that enables CDC on the outbox table and
 * provisions the watermark table the relay needs to track its progress.
 *
 * The returned script is safe to run repeatedly: every step is guarded by an
 * `IF NOT EXISTS` (or equivalent) check.
 *
 * --[ correctness lens findings from the design review ]----------------------
 *
 *   - `sys.sp_cdc_enable_db` and `sys.sp_cdc_enable_table` are reserved to
 *     `sysadmin` (server role) / `db_owner` (database role). The relay's
 *     runtime principal does NOT need either — it only needs `SELECT` on the
 *     generated `cdc.<captureInstance>_CT` change table and `SELECT`/`UPDATE`
 *     on the watermark table. This script is intended to be executed by a
 *     DBA at provisioning time, not by the application at boot.
 *
 *   - Default `captureInstance` is `<schema>_<table>`. That matches the name
 *     SQL Server itself generates when `@capture_instance` is `NULL`, so the
 *     `cdc.change_tables` lookup in this script and the runtime
 *     `cdc.fn_cdc_get_all_changes_<ci>` call from the relay both line up
 *     with what `sp_cdc_enable_table` would produce by default. Overriding
 *     it is supported (e.g. for blue/green schema migrations that need two
 *     concurrent capture instances on the same table).
 *
 *   - `supportsNetChanges` defaults to `false`. The relay is a strict tail-
 *     reader of inserts: every row in the outbox has a single insert, no
 *     updates, no deletes. Net-change support requires a unique index and
 *     emits an extra `cdc.fn_cdc_get_net_changes_<ci>` TVF that we never
 *     call. Turning it off keeps the change table cheaper and avoids the
 *     extra PK requirement on the source table.
 *
 *   - The Azure SQL Database guard is mandatory: `EngineEdition = 5` does
 *     NOT support `sys.sp_cdc_enable_db` (Azure SQL DB exposes a different
 *     CDC surface). Without the guard, this script would fail with an
 *     opaque "Could not find stored procedure" error mid-batch. We refuse
 *     up front with a clear message and `RETURN` so no partial state is
 *     left behind.
 */
export function createCdcEnablementSql(
  opts?: CreateCdcEnablementSqlOptions,
): string {
  const table = assertIdent(opts?.table ?? "outbox", "table");
  const schema = assertIdent(opts?.schema ?? "dbo", "schema");
  const captureInstance = assertIdent(
    opts?.captureInstance ?? `${schema}_${table}`,
    "captureInstance",
  );
  const watermarkTable = assertIdent(
    opts?.watermarkTable ?? "outbox_cdc_watermark",
    "watermarkTable",
  );
  const watermarkSchema = assertIdent(
    opts?.watermarkSchema ?? "dbo",
    "watermarkSchema",
  );
  const roleName = opts?.roleName ?? null;
  const supportsNetChanges = opts?.supportsNetChanges ?? false;
  const indexName = opts?.indexName ?? null;

  const roleNameSql =
    roleName === null ? "NULL" : `N'${roleName.replace(/'/g, "''")}'`;
  const supportsNetChangesSql = supportsNetChanges ? "1" : "0";
  const indexNameSql =
    indexName === null ? "NULL" : `N'${indexName.replace(/'/g, "''")}'`;

  return `-- eventferry CDC enablement for [${schema}].[${table}] (capture instance: ${captureInstance})
-- Idempotent. Must be run by a sysadmin / db_owner principal.

-- 1. Refuse on Azure SQL Database (EngineEdition = 5). sys.sp_cdc_enable_db
--    does not exist there; failing fast prevents partial provisioning.
IF CAST(SERVERPROPERTY('EngineEdition') AS int) = 5
BEGIN
    RAISERROR(N'eventferry CDC enablement is not supported on Azure SQL Database (EngineEdition = 5). Use the polling-only relay or migrate to SQL MI / on-prem SQL Server.', 16, 1);
    RETURN;
END;

-- 2. Enable CDC at the database level if it is not already on. sp_cdc_enable_db
--    is sysadmin/db_owner-only and is a no-op if CDC is already enabled, but
--    we gate it explicitly so re-runs are silent.
IF NOT EXISTS (
    SELECT 1 FROM sys.databases
    WHERE name = DB_NAME() AND is_cdc_enabled = 1
)
    EXEC sys.sp_cdc_enable_db;

-- 3. Enable CDC on [${schema}].[${table}] with capture instance '${captureInstance}'
--    if no matching capture instance already exists. supports_net_changes = ${supportsNetChangesSql}
--    because the relay only consumes inserts.
IF NOT EXISTS (
    SELECT 1
    FROM cdc.change_tables ct
    JOIN sys.tables  st ON st.object_id = ct.source_object_id
    JOIN sys.schemas ss ON ss.schema_id  = st.schema_id
    WHERE ss.name = N'${schema}'
      AND st.name = N'${table}'
      AND ct.capture_instance = N'${captureInstance}'
)
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'${schema}',
        @source_name          = N'${table}',
        @role_name            = ${roleNameSql},
        @supports_net_changes = ${supportsNetChangesSql},
        @capture_instance     = N'${captureInstance}',
        @index_name           = ${indexNameSql};

-- 4. Watermark table the relay uses to advance through change-table LSNs.
--    Single row per capture_instance; relay UPDATEs last_processed_lsn after
--    each successful batch.
IF OBJECT_ID(N'[${watermarkSchema}].[${watermarkTable}]', N'U') IS NULL
    CREATE TABLE [${watermarkSchema}].[${watermarkTable}] (
        capture_instance   NVARCHAR(128) NOT NULL PRIMARY KEY,
        last_processed_lsn BINARY(10)    NOT NULL,
        updated_at         DATETIME2(3)  NOT NULL
            CONSTRAINT [DF_${watermarkTable}_updated_at] DEFAULT (SYSUTCDATETIME())
    );
`;
}
